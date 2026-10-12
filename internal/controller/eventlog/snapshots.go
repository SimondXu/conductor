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

package eventlog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	gproto "google.golang.org/protobuf/proto"

	"github.com/simondxu/conductor/proto"
)

// Snapshots stores replay snapshots (PLAN.md G6, migration 0007): a
// conversation's events 1..step folded into one event, so a replay reads
// the snapshot and only the events after it. A snapshot is a cache of the
// log, never authoritative, and the log is kept whole.
type Snapshots interface {
	// Snapshot returns the conversation's latest snapshot, or step 0 and a
	// nil event when it has none.
	Snapshot(ctx context.Context, conversationID string) (step int64, folded *proto.StepEvent, err error)
	// SaveSnapshot stores folded as the snapshot at step unless a later one
	// is stored. No fencing is needed: the log is append-only, so the fold
	// of its first step events is the same whoever computes it.
	SaveSnapshot(ctx context.Context, conversationID string, step int64, folded *proto.StepEvent) error
}

var _ Snapshots = (*Postgres)(nil)

// Snapshot implements Snapshots.
func (p *Postgres) Snapshot(ctx context.Context, conversationID string) (int64, *proto.StepEvent, error) {
	var step int64
	var data []byte
	err := p.db.QueryRowContext(ctx, "SELECT step, event FROM conversation_snapshots WHERE conversation_id = $1", conversationID).Scan(&step, &data)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil, nil
	}
	if err != nil {
		return 0, nil, fmt.Errorf("eventlog: read snapshot: %w", err)
	}
	ev := &proto.StepEvent{}
	if err := gproto.Unmarshal(data, ev); err != nil {
		return 0, nil, fmt.Errorf("eventlog: decode snapshot of %s at step %d: %w", conversationID, step, err)
	}
	return step, ev, nil
}

// SaveSnapshot implements Snapshots.
func (p *Postgres) SaveSnapshot(ctx context.Context, conversationID string, step int64, folded *proto.StepEvent) error {
	data, err := gproto.Marshal(folded)
	if err != nil {
		return fmt.Errorf("eventlog: encode snapshot: %w", err)
	}
	if _, err := p.db.ExecContext(ctx, `
		INSERT INTO conversation_snapshots (conversation_id, step, event) VALUES ($1, $2, $3)
		ON CONFLICT (conversation_id) DO UPDATE SET step = $2, event = $3, created_at = now()
		 WHERE conversation_snapshots.step < $2`, conversationID, step, data); err != nil {
		return fmt.Errorf("eventlog: save snapshot: %w", err)
	}
	return nil
}
