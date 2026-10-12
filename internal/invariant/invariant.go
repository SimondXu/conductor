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

// Package invariant asserts the invariants of docs/conductor/correctness.md
// over the durable state of a set of conversations, one check each: never
// "did it recover". The scope is a conversation id prefix, so a bench or
// chaos run checks only its own conversations on a shared database.
//
// I4 is probed, not read: nothing in the database shows a rejected write
// after the fact. I8 is not here at all; its evidence is the
// conductor_unsafe_tool_turns_total counter and its test.
package invariant

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/simondxu/conductor/internal/controller"
	"github.com/simondxu/conductor/internal/controller/lease"
	"github.com/simondxu/conductor/internal/harness"
	"github.com/simondxu/conductor/proto"
	"google.golang.org/protobuf/encoding/protojson"
)

// Result is one invariant's verdict over the scope.
type Result struct {
	ID        string `json:"id"`
	Assertion string `json:"assertion"`
	// Check names what ran: the query in correctness.md, or the Go check.
	Check      string `json:"check"`
	Checks     int    `json:"checks"`
	Violations int    `json:"violations"`
	// Examples are up to maxExamples offending conversation or call ids.
	Examples []string `json:"examples,omitempty"`
}

const maxExamples = 5

// sqlCheck is an invariant checked by two queries over the scope ($1, a LIKE
// pattern): one counting what was checked, one listing the violations by id.
type sqlCheck struct {
	id, assertion, check string
	checks, violations   string
}

// The queries are correctness.md §1's, restricted to the scope.
var sqlChecks = []sqlCheck{{
	id: "I1", assertion: "every STARTED call reached a settled state", check: "unsettled call past reconcile_deadline",
	checks: `SELECT count(*) FROM tool_calls WHERE conversation_id LIKE $1 AND attempts > 0`,
	violations: `SELECT call_id FROM tool_calls
		WHERE conversation_id LIKE $1 AND state IN ('STARTED', 'OUTCOME_UNKNOWN') AND reconcile_deadline < now()`,
}, {
	id: "I1b", assertion: "no run COMPLETED with an unsettled call", check: "COMPLETED run joined to an unsettled call",
	checks: `SELECT count(*) FROM runs WHERE conversation_id LIKE $1 AND state = 'COMPLETED'`,
	violations: `SELECT t.call_id FROM runs r JOIN tool_calls t USING (conversation_id)
		WHERE r.conversation_id LIKE $1 AND r.state = 'COMPLETED' AND t.state IN ('STARTED', 'OUTCOME_UNKNOWN')`,
}, {
	id: "I2", assertion: "no extra effect per idempotency_key (IDEMPOTENT)", check: "fake_effects rows per key > 1",
	checks: `SELECT count(DISTINCT e.idempotency_key) FROM fake_effects e JOIN tool_calls t USING (call_id)
		WHERE t.conversation_id LIKE $1 AND t.class = 'IDEMPOTENT'`,
	violations: `SELECT e.idempotency_key FROM fake_effects e JOIN tool_calls t USING (call_id)
		WHERE t.conversation_id LIKE $1 AND t.class = 'IDEMPOTENT'
		GROUP BY e.idempotency_key HAVING count(*) > 1`,
}, {
	id: "I3'", assertion: "generation non-decreasing by step, per conversation", check: "lag(generation) over step",
	checks: `SELECT count(DISTINCT conversation_id) FROM conversation_log
		WHERE conversation_id LIKE $1 AND generation IS NOT NULL`,
	violations: `SELECT conversation_id || ' step ' || step FROM (
		SELECT conversation_id, step, generation,
		       lag(generation) OVER (PARTITION BY conversation_id ORDER BY step) AS prev
		  FROM conversation_log WHERE conversation_id LIKE $1 AND generation IS NOT NULL) s
		WHERE prev IS NOT NULL AND generation < prev`,
}, {
	id: "I5", assertion: "conversation_log.step contiguous from 1", check: "count(*) <> max(step)",
	checks: `SELECT count(DISTINCT conversation_id) FROM conversation_log WHERE conversation_id LIKE $1`,
	violations: `SELECT conversation_id FROM conversation_log WHERE conversation_id LIKE $1
		GROUP BY conversation_id HAVING count(*) <> max(step) OR min(step) <> 1`,
}, {
	id: "I7a", assertion: "runs projection == rebuild from log", check: "runs row vs the 0005 backfill query",
	checks: `SELECT count(*) FROM runs WHERE conversation_id LIKE $1`,
	violations: `SELECT r.conversation_id FROM runs r
		WHERE r.conversation_id LIKE $1 AND (r.state, COALESCE(r.agent_id, ''), r.last_step) IS DISTINCT FROM (
		  COALESCE((SELECT replace(l.payload::jsonb->>'state', 'STATE_', '') FROM conversation_log l
		             WHERE l.conversation_id = r.conversation_id AND l.payload::jsonb ? 'state'
		             ORDER BY l.step DESC LIMIT 1), 'PENDING'),
		  COALESCE((SELECT l.payload::jsonb->>'agent_id' FROM conversation_log l
		             WHERE l.conversation_id = r.conversation_id AND l.payload::jsonb->>'agent_id' <> ''
		             ORDER BY l.step LIMIT 1), ''),
		  (SELECT max(step) FROM conversation_log l WHERE l.conversation_id = r.conversation_id)::bigint)`,
}, {
	id: "I7b", assertion: "tool_calls terminal states consistent with log", check: "SUCCEEDED/FAILED call with no result step",
	checks: `SELECT count(*) FROM tool_calls WHERE conversation_id LIKE $1 AND state IN ('SUCCEEDED', 'FAILED')`,
	violations: `SELECT t.call_id FROM tool_calls t
		WHERE t.conversation_id LIKE $1 AND t.state IN ('SUCCEEDED', 'FAILED')
		  AND NOT EXISTS (
		    SELECT 1 FROM conversation_log l
		     WHERE l.conversation_id = t.conversation_id
		       AND l.payload::jsonb @> jsonb_build_object('steps', jsonb_build_array(
		             jsonb_build_object('tool_result', jsonb_build_object('call_id', t.call_id)))))`,
}}

// Check runs every check over the conversations whose id starts with prefix,
// in correctness.md order.
func Check(ctx context.Context, db *sql.DB, prefix string) ([]Result, error) {
	scope := prefix + "%"
	var out []Result
	for _, c := range sqlChecks {
		r := Result{ID: c.id, Assertion: c.assertion, Check: c.check}
		if err := db.QueryRowContext(ctx, c.checks, scope).Scan(&r.Checks); err != nil {
			return nil, fmt.Errorf("%s checks: %w", c.id, err)
		}
		if err := violations(ctx, db, c.violations, scope, &r); err != nil {
			return nil, fmt.Errorf("%s violations: %w", c.id, err)
		}
		out = append(out, r)
		if c.id == "I3'" {
			i4, err := probeFence(ctx, db, scope)
			if err != nil {
				return nil, fmt.Errorf("I4: %w", err)
			}
			out = append(out, i4)
		}
		if c.id == "I5" {
			i6, err := checkRebuild(ctx, db, scope)
			if err != nil {
				return nil, fmt.Errorf("I6: %w", err)
			}
			out = append(out, i6)
		}
	}
	return out, nil
}

func violations(ctx context.Context, db *sql.DB, query, scope string, r *Result) error {
	rows, err := db.QueryContext(ctx, query, scope)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return err
		}
		r.Violations++
		if len(r.Examples) < maxExamples {
			r.Examples = append(r.Examples, id)
		}
	}
	return rows.Err()
}

// probeFence is I4: every conversation in scope whose ownership changed
// (generation 2 or more) must reject a write at its previous generation. Each
// probe runs the fence every write runs, lease.CheckFence, in a transaction
// that is rolled back, so it writes nothing.
func probeFence(ctx context.Context, db *sql.DB, scope string) (Result, error) {
	r := Result{ID: "I4", Assertion: "stale-generation write rejected", Check: "lease.CheckFence at generation-1, rolled back"}
	rows, err := db.QueryContext(ctx, `SELECT conversation_id, generation FROM conversation_leases
		WHERE conversation_id LIKE $1 AND generation >= 2`, scope)
	if err != nil {
		return r, err
	}
	type owned struct {
		conv string
		gen  int64
	}
	var leases []owned
	for rows.Next() {
		var l owned
		if err := rows.Scan(&l.conv, &l.gen); err != nil {
			rows.Close()
			return r, err
		}
		leases = append(leases, l)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return r, err
	}
	for _, l := range leases {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return r, err
		}
		err = lease.CheckFence(ctx, tx, l.conv, l.gen-1)
		_ = tx.Rollback()
		r.Checks++
		if !errors.Is(err, lease.ErrFenced) {
			if err != nil {
				return r, err
			}
			r.Violations++
			if len(r.Examples) < maxExamples {
				r.Examples = append(r.Examples, l.conv)
			}
		}
	}
	return r, nil
}

// checkRebuild is I6 as far as durable state can show it: what the
// controller decided in memory must be what rebuild(log) gives. A run it
// ended COMPLETED or FAILED must rebuild to a history with every tool call
// answered, and a COMPLETED one to a history that needs no further turn.
// It does not test that rebuild is pure; that is a unit-test matter.
func checkRebuild(ctx context.Context, db *sql.DB, scope string) (Result, error) {
	r := Result{ID: "I6", Assertion: "rebuild(log) == post-recovery state", Check: "terminal run rebuilds to a closed history"}
	rows, err := db.QueryContext(ctx, `SELECT conversation_id, payload FROM conversation_log
		WHERE conversation_id LIKE $1 ORDER BY conversation_id, step`, scope)
	if err != nil {
		return r, err
	}
	defer rows.Close()
	var conv string
	var events []*proto.StepEvent
	flush := func() error {
		if conv == "" {
			return nil
		}
		state, history, err := controller.Rebuild(events)
		if err != nil {
			return fmt.Errorf("rebuild %s: %w", conv, err)
		}
		if state != proto.State_STATE_COMPLETED && state != proto.State_STATE_FAILED {
			return nil
		}
		r.Checks++
		if len(harness.UnansweredToolCalls(history)) > 0 || (state == proto.State_STATE_COMPLETED && harness.NeedsTurn(history)) {
			r.Violations++
			if len(r.Examples) < maxExamples {
				r.Examples = append(r.Examples, conv)
			}
		}
		return nil
	}
	for rows.Next() {
		var id, payload string
		if err := rows.Scan(&id, &payload); err != nil {
			return r, err
		}
		if id != conv {
			if err := flush(); err != nil {
				return r, err
			}
			conv, events = id, nil
		}
		ev := &proto.StepEvent{}
		if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal([]byte(payload), ev); err != nil {
			return r, fmt.Errorf("decode %s: %w", id, err)
		}
		events = append(events, ev)
	}
	if err := rows.Err(); err != nil {
		return r, err
	}
	return r, flush()
}
