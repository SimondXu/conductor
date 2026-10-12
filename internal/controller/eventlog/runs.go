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
	"fmt"
	"strings"
	"time"

	"github.com/simondxu/conductor/proto"
)

// RunIndex is what the reconciler needs from the runs table
// (docs/conductor/ARCHITECTURE.md §2.3, §6.1).
type RunIndex interface {
	// RecoveryCandidates returns conversations of the given harnesses to
	// recover, from two sources: PENDING runs, and conversations with a tool
	// call in STARTED or OUTCOME_UNKNOWN, which catches a runs row whose
	// state is stale or wrong. Conversations whose backoff has not elapsed,
	// or whose lease has not expired, are left out.
	RecoveryCandidates(ctx context.Context, harnessIDs []string, limit int) ([]Candidate, error)
	// BumpRecoveryAttempt counts one recovery attempt and pushes the next
	// one back by backoff(n), in its own transaction, and returns n. It
	// returns 0 when the conversation has no runs row.
	BumpRecoveryAttempt(ctx context.Context, conversationID string, backoff func(n int) time.Duration) (int, error)
}

// Candidate is a conversation the reconciler may recover.
type Candidate struct {
	ConversationID string
	// AgentID is the run's harness.
	AgentID string
	// Idle is how long the run had gone without an append when it was found,
	// by the database clock.
	Idle time.Duration
	// KillKind is how its last owner let go: "graceful" (released),
	// "ungraceful" (the lease expired) or "none" (never leased).
	KillKind string
}

// projectRun applies one appended event to the runs projection: the last
// logged state, the first logged agent_id, the last step. The guard keeps a
// late write from moving the projection backwards.
func projectRun(ctx context.Context, tx *sql.Tx, ev *proto.StepEvent, step int64) error {
	var state, agentID any
	if ev.State != proto.State_STATE_UNSPECIFIED {
		state = strings.TrimPrefix(ev.State.String(), "STATE_")
	}
	if ev.AgentId != "" {
		agentID = ev.AgentId
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO runs AS r (conversation_id, state, agent_id, last_step, updated_at)
		VALUES ($1, COALESCE($2, 'PENDING'), $3, $4, now())
		ON CONFLICT (conversation_id) DO UPDATE
		   SET state = COALESCE($2, r.state), agent_id = COALESCE(r.agent_id, $3),
		       last_step = $4, updated_at = now()
		 WHERE r.last_step < $4`,
		ev.ConversationId, state, agentID, step); err != nil {
		return fmt.Errorf("eventlog: project run: %w", err)
	}
	return nil
}

func (p *Postgres) RecoveryCandidates(ctx context.Context, harnessIDs []string, limit int) ([]Candidate, error) {
	rows, err := p.db.QueryContext(ctx, `
		SELECT c.conversation_id, COALESCE(r.agent_id, ''),
		       EXTRACT(EPOCH FROM now() - r.updated_at)::float8,
		       CASE WHEN l.conversation_id IS NULL THEN 'none'
		            WHEN l.owner_id IS NULL THEN 'graceful'
		            ELSE 'ungraceful' END
		  FROM (SELECT conversation_id FROM runs WHERE state = 'PENDING'
		        UNION
		        SELECT conversation_id FROM tool_calls
		         WHERE state IN ('STARTED', 'OUTCOME_UNKNOWN')) c
		  -- runs is written with the log, so a call with no runs row has no
		  -- log to recover from.
		  JOIN runs r USING (conversation_id)
		  LEFT JOIN conversation_leases l USING (conversation_id)
		 WHERE r.agent_id = ANY($1)
		   AND (r.next_attempt_at IS NULL OR r.next_attempt_at <= now())
		   AND (l.expires_at IS NULL OR l.expires_at <= now())
		 ORDER BY r.next_attempt_at NULLS FIRST
		 LIMIT $2`, harnessIDs, limit)
	if err != nil {
		return nil, fmt.Errorf("eventlog: recovery candidates: %w", err)
	}
	defer rows.Close()
	var out []Candidate
	for rows.Next() {
		var c Candidate
		var idle float64
		if err := rows.Scan(&c.ConversationID, &c.AgentID, &idle, &c.KillKind); err != nil {
			return nil, fmt.Errorf("eventlog: scan candidate: %w", err)
		}
		c.Idle = time.Duration(idle * float64(time.Second))
		out = append(out, c)
	}
	return out, rows.Err()
}

func (p *Postgres) BumpRecoveryAttempt(ctx context.Context, conversationID string, backoff func(n int) time.Duration) (int, error) {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("eventlog: bump recovery attempt: %w", err)
	}
	defer tx.Rollback()
	var n int
	err = tx.QueryRowContext(ctx, `
		UPDATE runs SET recovery_attempts = recovery_attempts + 1
		 WHERE conversation_id = $1 RETURNING recovery_attempts`, conversationID).Scan(&n)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("eventlog: bump recovery attempt: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE runs SET next_attempt_at = now() + make_interval(secs => $2)
		 WHERE conversation_id = $1`, conversationID, backoff(n).Seconds()); err != nil {
		return 0, fmt.Errorf("eventlog: set backoff: %w", err)
	}
	return n, tx.Commit()
}
