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
	"os"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"

	schema "github.com/simondxu/conductor/db"
	"github.com/simondxu/conductor/proto"
)

func openTestPostgres(t *testing.T) *Postgres {
	t.Helper()
	dsn := os.Getenv("PG_TEST_DSN")
	if dsn == "" {
		t.Skip("PG_TEST_DSN not set; skipping Postgres runs tests")
	}
	if _, err := schema.Up(dsn); err != nil {
		t.Fatalf("migrate up: %v", err)
	}
	log, err := OpenPostgresEventLog(dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { log.Close() })
	return log.(*Postgres)
}

// derivedRun is the runs row derived from the log alone: the migration's
// backfill query for one conversation.
const derivedRun = `
SELECT COALESCE((SELECT replace(l.payload::jsonb->>'state', 'STATE_', '')
                   FROM conversation_log l
                  WHERE l.conversation_id = $1 AND l.payload::jsonb ? 'state'
                  ORDER BY l.step DESC LIMIT 1), 'PENDING'),
       COALESCE((SELECT l.payload::jsonb->>'agent_id'
                   FROM conversation_log l
                  WHERE l.conversation_id = $1 AND l.payload::jsonb->>'agent_id' <> ''
                  ORDER BY l.step LIMIT 1), ''),
       (SELECT max(step) FROM conversation_log WHERE conversation_id = $1)`

func appendEvent(t *testing.T, p *Postgres, ev *proto.StepEvent) {
	t.Helper()
	if ev.EventId == "" {
		ev.EventId = uuid.NewString()
	}
	if _, err := p.Append(context.Background(), ev); err != nil {
		t.Fatal(err)
	}
}

// I7a: the projection written with each append equals the one derived from
// the log, and a retried append does not move it.
func TestRunsProjectionMatchesLog(t *testing.T) {
	p := openTestPostgres(t)
	conv := t.Name() + "-" + uuid.NewString()
	appendEvent(t, p, &proto.StepEvent{ConversationId: conv, AgentId: "fake", State: proto.State_STATE_PENDING})
	appendEvent(t, p, &proto.StepEvent{ConversationId: conv, State: proto.State_STATE_PENDING})
	appendEvent(t, p, &proto.StepEvent{ConversationId: conv, AgentId: "other"})
	done := &proto.StepEvent{ConversationId: conv, State: proto.State_STATE_COMPLETED}
	appendEvent(t, p, done)
	appendEvent(t, p, done) // retry: same event_id

	var state, agent string
	var last int64
	if err := p.db.QueryRow("SELECT state, COALESCE(agent_id, ''), last_step FROM runs WHERE conversation_id = $1", conv).Scan(&state, &agent, &last); err != nil {
		t.Fatal(err)
	}
	if state != "COMPLETED" || agent != "fake" || last != 4 {
		t.Fatalf("runs = %s %q %d, want COMPLETED fake 4", state, agent, last)
	}
	var dState, dAgent string
	var dLast int64
	if err := p.db.QueryRow(derivedRun, conv).Scan(&dState, &dAgent, &dLast); err != nil {
		t.Fatal(err)
	}
	if dState != state || dAgent != agent || dLast != last {
		t.Fatalf("derived %s %q %d != projected %s %q %d", dState, dAgent, dLast, state, agent, last)
	}
}

func TestRecoveryCandidates(t *testing.T) {
	p := openTestPostgres(t)
	ctx := context.Background()
	id := func(name string) string { return t.Name() + "-" + name + "-" + uuid.NewString() }
	run := func(conv, agent string, state proto.State) {
		appendEvent(t, p, &proto.StepEvent{ConversationId: conv, AgentId: agent, State: state})
	}
	pending, other, done, leased, backedOff, unsettled :=
		id("pending"), id("other"), id("done"), id("leased"), id("backedoff"), id("unsettled")
	run(pending, "rf", proto.State_STATE_PENDING)
	run(other, "none", proto.State_STATE_PENDING)
	run(done, "rf", proto.State_STATE_COMPLETED)
	run(leased, "rf", proto.State_STATE_PENDING)
	run(backedOff, "rf", proto.State_STATE_PENDING)
	run(unsettled, "rf", proto.State_STATE_COMPLETED) // a wrong projection
	if _, err := p.db.Exec(`INSERT INTO conversation_leases VALUES ($1, 'someone', 1, now(), now() + interval '1 hour')`, leased); err != nil {
		t.Fatal(err)
	}
	if n, err := p.BumpRecoveryAttempt(ctx, backedOff, func(int) time.Duration { return time.Hour }); err != nil || n != 1 {
		t.Fatalf("bump = %d, %v; want 1", n, err)
	}
	if _, err := p.db.Exec(`INSERT INTO tool_calls (call_id, conversation_id, step, tool_name, args_hash, args, idempotency_key, class, state, generation)
		VALUES ($1, $2, 1, 't', 'h', '{}', $1, 'UNSAFE', 'STARTED', 1)`, uuid.NewString(), unsettled); err != nil {
		t.Fatal(err)
	}

	got, err := p.RecoveryCandidates(ctx, []string{"rf"}, 100000)
	if err != nil {
		t.Fatal(err)
	}
	for conv, want := range map[string]bool{pending: true, unsettled: true, other: false, done: false, leased: false, backedOff: false} {
		if slices.Contains(got, conv) != want {
			t.Errorf("candidate %s: got %t, want %t", conv, !want, want)
		}
	}
}

func TestBumpRecoveryAttempt(t *testing.T) {
	p := openTestPostgres(t)
	ctx := context.Background()
	conv := t.Name() + "-" + uuid.NewString()
	appendEvent(t, p, &proto.StepEvent{ConversationId: conv, State: proto.State_STATE_PENDING})
	var seen []int
	backoff := func(n int) time.Duration { seen = append(seen, n); return 0 }
	for want := 1; want <= 2; want++ {
		if n, err := p.BumpRecoveryAttempt(ctx, conv, backoff); err != nil || n != want {
			t.Fatalf("bump = %d, %v; want %d", n, err, want)
		}
	}
	if !slices.Equal(seen, []int{1, 2}) {
		t.Errorf("backoff called with %v, want [1 2]", seen)
	}
	if n, err := p.BumpRecoveryAttempt(ctx, "missing-"+uuid.NewString(), backoff); err != nil || n != 0 {
		t.Errorf("bump of a missing run = %d, %v; want 0", n, err)
	}
}
