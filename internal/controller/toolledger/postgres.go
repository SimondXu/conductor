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
	"fmt"

	_ "github.com/jackc/pgx/v5/stdlib"
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
	for _, c := range calls {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO tool_calls (call_id, conversation_id, step, tool_name, args_hash, args,
			                        idempotency_key, class, state, generation)
			VALUES ($1, $2, $3, $4, $5, $6, $1, $7, 'PLANNED', $8)
			ON CONFLICT (call_id) DO NOTHING`,
			c.CallID, c.ConversationID, c.Step, c.ToolName, c.ArgsHash, string(c.Args), string(c.Class), gen); err != nil {
			return fmt.Errorf("toolledger: plan %s: %w", c.CallID, err)
		}
	}
	return tx.Commit()
}

func (p *Postgres) Start(ctx context.Context, gen int64, callID string) error {
	return p.transition(ctx, callID, Started, `
		UPDATE tool_calls
		   SET state = 'STARTED', attempts = attempts + 1, started_at = now(),
		       reconcile_deadline = now() + make_interval(secs => $3), generation = $2
		 WHERE call_id = $1 AND state IN ('PLANNED', 'OUTCOME_UNKNOWN')`,
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
	return p.transition(ctx, callID, out.State, `
		UPDATE tool_calls
		   SET state = $3, result = $4, error = NULLIF($5, ''), settled_at = now(), generation = $2
		 WHERE call_id = $1 AND state IN ('STARTED', 'OUTCOME_UNKNOWN')`,
		callID, gen, string(out.State), result, out.Error)
}

func (p *Postgres) MarkUnknown(ctx context.Context, gen int64, callID, reason string) error {
	return p.transition(ctx, callID, OutcomeUnknown, `
		UPDATE tool_calls SET state = 'OUTCOME_UNKNOWN', error = $3, generation = $2
		 WHERE call_id = $1 AND state = 'STARTED'`,
		callID, gen, reason)
}

// transition runs a guarded UPDATE. When it matches no row, a call already in
// the target state is a no-op; anything else is ErrTransition.
func (p *Postgres) transition(ctx context.Context, callID string, target State, query string, args ...any) error {
	res, err := p.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("toolledger: %s %s: %w", target, callID, err)
	}
	if n, err := res.RowsAffected(); err != nil || n == 1 {
		return err
	}
	var current State
	if err := p.db.QueryRowContext(ctx, "SELECT state FROM tool_calls WHERE call_id = $1", callID).Scan(&current); err != nil {
		return fmt.Errorf("%w: %s %s: %v", ErrTransition, target, callID, err)
	}
	if current == target && target != Started {
		return nil
	}
	return fmt.Errorf("%w: %s -> %s for %s", ErrTransition, current, target, callID)
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
