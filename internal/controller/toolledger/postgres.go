// Copyright 2026 Simon Xu
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

package toolledger

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/simondxu/conductor/internal/controller/lease"
)

// Postgres is the Ledger on the tool_calls table (migration 0002).
type Postgres struct {
	db *sql.DB
}

var _ Ledger = (*Postgres)(nil)

// OpenPostgres connects to dsn. The schema is owned by `conductor migrate`.
func OpenPostgres(dsn string) (*Postgres, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("toolledger: open: %w", err)
	}
	return &Postgres{db: db}, nil
}

// Close closes the connection pool.
func (p *Postgres) Close() error { return p.db.Close() }

func (p *Postgres) Plan(ctx context.Context, gen int64, calls ...PlannedCall) error {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("toolledger: plan: %w", err)
	}
	defer tx.Rollback()
	fenced := map[string]bool{}
	var inserted []PlannedCall
	for _, c := range calls {
		if !fenced[c.ConversationID] {
			if err := lease.CheckFence(ctx, tx, c.ConversationID, gen); err != nil {
				return err
			}
			fenced[c.ConversationID] = true
		}
		res, err := tx.ExecContext(ctx, `
			INSERT INTO tool_calls (call_id, conversation_id, step, tool_name, args_hash, args,
			                        idempotency_key, class, state, generation)
			VALUES ($1, $2, $3, $4, $5, $6, $1, $7, 'PLANNED', $8)
			ON CONFLICT (call_id) DO NOTHING`,
			c.CallID, c.ConversationID, c.Step, c.ToolName, c.ArgsHash, string(c.Args), string(c.Class), gen)
		if err != nil {
			return fmt.Errorf("toolledger: plan %s: %w", c.CallID, err)
		}
		if n, _ := res.RowsAffected(); n == 1 {
			inserted = append(inserted, c)
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	for _, c := range inserted {
		toolCalls.WithLabelValues(c.ToolName, string(c.Class), string(Planned)).Inc()
	}
	return nil
}

func (p *Postgres) Start(ctx context.Context, gen int64, callID string) error {
	return p.transition(ctx, gen, callID, Started, []State{Planned, OutcomeUnknown}, `
		state = 'STARTED', attempts = t.attempts + 1, started_at = now(),
		reconcile_deadline = now() + make_interval(secs => $3), generation = $2`,
		callID, gen, ReconcileWindow.Seconds())
}

func (p *Postgres) Settle(ctx context.Context, gen int64, callID string, out Outcome) error {
	switch out.State {
	case Succeeded, Failed, Abandoned:
	default:
		return fmt.Errorf("%w: settle to %s", ErrTransition, out.State)
	}
	var result any
	if out.Result != nil {
		result = string(out.Result)
	}
	return p.transition(ctx, gen, callID, out.State, []State{Started, OutcomeUnknown}, `
		state = $3, result = $4, error = NULLIF($5, ''), settled_at = now(), generation = $2`,
		callID, gen, string(out.State), result, out.Error)
}

func (p *Postgres) MarkUnknown(ctx context.Context, gen int64, callID, reason string) error {
	return p.transition(ctx, gen, callID, OutcomeUnknown, []State{Started}, `
		state = 'OUTCOME_UNKNOWN', error = $3, generation = $2`,
		callID, gen, reason)
}

// transition moves callID from one of from to target with the assignments
// set ($1 is the call_id), fenced on gen (I4). The row is locked first, so
// the state it moved from is exact, for the metrics. A call already in the
// target state is a no-op; any other state is ErrTransition.
func (p *Postgres) transition(ctx context.Context, gen int64, callID string, target State, from []State, set string, args ...any) error {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("toolledger: %s %s: %w", target, callID, err)
	}
	defer tx.Rollback()
	var prev State
	var conv, tool string
	var class Class
	err = tx.QueryRowContext(ctx, `
		SELECT state, conversation_id, tool_name, class FROM tool_calls WHERE call_id = $1 FOR UPDATE`,
		callID).Scan(&prev, &conv, &tool, &class)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: %s %s: no such call", ErrTransition, target, callID)
	}
	if err != nil {
		return fmt.Errorf("toolledger: %s %s: %w", target, callID, err)
	}
	if err := lease.CheckFence(ctx, tx, conv, gen); err != nil {
		return err
	}
	if !slices.Contains(from, prev) {
		if prev == target && target != Started {
			return nil
		}
		return fmt.Errorf("%w: %s -> %s for %s", ErrTransition, prev, target, callID)
	}
	var secs float64
	if err := tx.QueryRowContext(ctx, `UPDATE tool_calls t SET `+set+` WHERE call_id = $1
		RETURNING COALESCE(EXTRACT(EPOCH FROM settled_at - started_at), 0)::float8`, args...).Scan(&secs); err != nil {
		return fmt.Errorf("toolledger: %s %s: %w", target, callID, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("toolledger: %s %s: %w", target, callID, err)
	}
	observeTransition(prev, target, tool, class, secs)
	return nil
}

// DeadlineExceeded is the error of a call abandoned at its reconcile_deadline.
const DeadlineExceeded = "reconcile_deadline_exceeded"

func (p *Postgres) ExpireDeadlines(ctx context.Context) (int, error) {
	rows, err := p.db.QueryContext(ctx, `
		UPDATE tool_calls SET state = 'ABANDONED', error = $1, settled_at = now()
		 WHERE state = 'OUTCOME_UNKNOWN' AND reconcile_deadline < now()
		RETURNING tool_name, class`, DeadlineExceeded)
	if err != nil {
		return 0, fmt.Errorf("toolledger: expire deadlines: %w", err)
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		var tool string
		var class Class
		if err := rows.Scan(&tool, &class); err != nil {
			return n, fmt.Errorf("toolledger: expire deadlines: %w", err)
		}
		toolCalls.WithLabelValues(tool, string(class), string(Abandoned)).Inc()
		outcomeUnknown.WithLabelValues(tool, resolutionDeadline).Inc()
		n++
	}
	return n, rows.Err()
}

func (p *Postgres) CountUnsettled(ctx context.Context) (map[State]int, error) {
	rows, err := p.db.QueryContext(ctx, `
		SELECT state, count(*) FROM tool_calls
		 WHERE state IN ('STARTED', 'OUTCOME_UNKNOWN') GROUP BY state`)
	if err != nil {
		return nil, fmt.Errorf("toolledger: count unsettled: %w", err)
	}
	defer rows.Close()
	out := map[State]int{}
	for rows.Next() {
		var s State
		var n int
		if err := rows.Scan(&s, &n); err != nil {
			return nil, fmt.Errorf("toolledger: count unsettled: %w", err)
		}
		out[s] = n
	}
	return out, rows.Err()
}

func (p *Postgres) Calls(ctx context.Context, conversationID string) ([]ToolCall, error) {
	rows, err := p.db.QueryContext(ctx, `
		SELECT call_id, conversation_id, step, tool_name, args_hash, args, class,
		       idempotency_key, state, attempts, result, COALESCE(error, ''), generation
		  FROM tool_calls WHERE conversation_id = $1 ORDER BY step, call_id`, conversationID)
	if err != nil {
		return nil, fmt.Errorf("toolledger: calls: %w", err)
	}
	defer rows.Close()
	var out []ToolCall
	for rows.Next() {
		var c ToolCall
		var args string
		var result sql.NullString
		if err := rows.Scan(&c.CallID, &c.ConversationID, &c.Step, &c.ToolName, &c.ArgsHash, &args, &c.Class,
			&c.IdempotencyKey, &c.State, &c.Attempts, &result, &c.Error, &c.Generation); err != nil {
			return nil, fmt.Errorf("toolledger: scan: %w", err)
		}
		c.Args = []byte(args)
		if result.Valid {
			c.Result = []byte(result.String)
		}
		out = append(out, c)
	}
	return out, rows.Err()
}
