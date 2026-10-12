// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package eventlog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/simondxu/conductor/internal/controller/lease"
	"github.com/simondxu/conductor/proto"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// sqlEventLog is a database backed EventLog shared by the SQLite and
// PostgreSQL implementations.
type sqlEventLog struct {
	db *sql.DB
	// postgres fences appends on the lease generation, records it, and keeps
	// the runs projection.
	postgres bool
}

// Append serializes the event to JSON and inserts it into the database. It is
// idempotent on event.EventId when that is set.
func (l *sqlEventLog) Append(ctx context.Context, event *proto.StepEvent, gen int64) (step int64, err error) {
	ctx, endSpan := l.startSpan(ctx, "append", event.ConversationId)
	defer func() { endSpan(err) }()

	tx, err := l.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("eventlog: begin tx: %w", err)
	}
	defer tx.Rollback()

	if l.postgres {
		if err := lease.CheckFence(ctx, tx, event.ConversationId, gen); err != nil {
			return 0, err
		}
	}

	if err := tx.QueryRowContext(ctx, "SELECT COALESCE(MAX(step), 0) + 1 FROM conversation_log WHERE conversation_id = $1", event.ConversationId).Scan(&step); err != nil {
		return 0, fmt.Errorf("eventlog: compute step: %w", err)
	}

	payload, err := marshalOpts.Marshal(event)
	if err != nil {
		return 0, fmt.Errorf("eventlog: marshal event: %w", err)
	}

	// I10: a retried append with the same event_id inserts nothing and returns
	// the step the first attempt got.
	var eventID any
	if event.EventId != "" {
		eventID = event.EventId
	}
	inserted := true
	insert := `
		INSERT INTO conversation_log (conversation_id, step, payload, event_id) VALUES ($1, $2, $3, $4)
		ON CONFLICT (event_id) WHERE event_id IS NOT NULL DO NOTHING
		RETURNING step`
	args := []any{event.ConversationId, step, string(payload), eventID}
	if l.postgres {
		insert = `
		INSERT INTO conversation_log (conversation_id, step, payload, event_id, generation) VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (event_id) WHERE event_id IS NOT NULL DO NOTHING
		RETURNING step`
		args = append(args, gen)
	}
	err = tx.QueryRowContext(ctx, insert, args...).Scan(&step)
	if errors.Is(err, sql.ErrNoRows) {
		inserted = false
		err = tx.QueryRowContext(ctx, "SELECT step FROM conversation_log WHERE event_id = $1", eventID).Scan(&step)
	}
	if err != nil {
		return 0, fmt.Errorf("eventlog: insert conversation: %w", err)
	}
	if inserted && l.postgres {
		if err := projectRun(ctx, tx, event, step); err != nil {
			return 0, err
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("eventlog: commit: %w", err)
	}

	return step, nil
}

// Events retrieves all events from the database for a conversation, ordered by step.
func (l *sqlEventLog) Events(ctx context.Context, conversationID string) ([]*proto.StepEvent, error) {
	return l.EventsSince(ctx, conversationID, 0)
}

// EventsSince retrieves a conversation's events after afterStep, ordered by step.
func (l *sqlEventLog) EventsSince(ctx context.Context, conversationID string, afterStep int64) (events []*proto.StepEvent, err error) {
	ctx, endSpan := l.startSpan(ctx, "events", conversationID)
	defer func() { endSpan(err) }()

	rows, err := l.db.QueryContext(ctx, "SELECT payload FROM conversation_log WHERE conversation_id = $1 AND step > $2 ORDER BY step", conversationID, afterStep)
	if err != nil {
		return nil, fmt.Errorf("eventlog: query conversation: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var payload string
		if err := rows.Scan(&payload); err != nil {
			return nil, fmt.Errorf("eventlog: scan conversation: %w", err)
		}

		ev := &proto.StepEvent{}
		if err := unmarshalOpts.Unmarshal([]byte(payload), ev); err != nil {
			return nil, fmt.Errorf("eventlog: unmarshal event: %w", err)
		}
		events = append(events, ev)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("eventlog: iterate conversation: %w", err)
	}

	return events, nil
}

// Close releases the database connection.
func (l *sqlEventLog) Close() error {
	return l.db.Close()
}

func (l *sqlEventLog) startSpan(ctx context.Context, name string, conversationID string) (context.Context, func(err error)) {
	const tracerName = "eventlog.sql"
	tracer := otel.Tracer(tracerName)

	ctx, span := tracer.Start(ctx, "eventlog."+name, trace.WithAttributes(
		attribute.String("conversation_id", conversationID),
	))
	return ctx, func(err error) {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}
}
