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

package invariant

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"

	schema "github.com/simondxu/conductor/db"
	"github.com/simondxu/conductor/internal/controller"
	"github.com/simondxu/conductor/internal/controller/eventlog"
	"github.com/simondxu/conductor/internal/controller/lease"
	"github.com/simondxu/conductor/internal/controller/toolledger"
	"github.com/simondxu/conductor/internal/harness/fake"
	"github.com/simondxu/conductor/internal/harness/harnesstest"
	"github.com/simondxu/conductor/proto"
)

func openTestDB(t *testing.T) (*sql.DB, string) {
	t.Helper()
	dsn := os.Getenv("PG_TEST_DSN")
	if dsn == "" {
		t.Skip("PG_TEST_DSN not set; skipping invariant checks")
	}
	if _, err := schema.Up(dsn); err != nil {
		t.Fatalf("migrate up: %v", err)
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db, dsn
}

func byID(t *testing.T, db *sql.DB, prefix string) map[string]Result {
	t.Helper()
	results, err := Check(context.Background(), db, prefix)
	if err != nil {
		t.Fatal(err)
	}
	m := map[string]Result{}
	for _, r := range results {
		m[r.ID] = r
	}
	return m
}

// A real run on the whole stack (leases, ledger, fake effects in Postgres)
// checks every invariant and violates none.
func TestCheck_CleanRun(t *testing.T) {
	db, dsn := openTestDB(t)
	ctx := context.Background()
	effects, err := fake.OpenPostgresEffects(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { effects.Close() })
	log, err := eventlog.OpenPostgresEventLog(dsn)
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := toolledger.OpenPostgres(dsn)
	if err != nil {
		t.Fatal(err)
	}
	leases, err := lease.OpenPostgres(dsn)
	if err != nil {
		t.Fatal(err)
	}
	reg := controller.NewRegistry()
	if err := reg.Register("invariant-fake", fake.New(effects), controller.RecoveryFull); err != nil {
		t.Fatal(err)
	}
	reg.DeclareTools("invariant-fake", map[string]toolledger.Class{"charge_card": toolledger.Idempotent, "send_email": toolledger.Unsafe})
	c, err := controller.New(ctx, controller.Config{
		Registry:        reg,
		EventLogBuilder: func() (eventlog.EventLog, error) { return log, nil },
		Ledger:          ledger, Lease: leases, OwnerID: "invariant-test", LeaseTTL: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })

	prefix := "invariant-clean-" + uuid.NewString() + "-"
	// Two turns on a, so its lease is at generation 2 for the I4 probe.
	for _, conv := range []string{"a", "b", "a"} {
		if err := c.Exec(ctx, &proto.CreateInteractionEvent{
			ConversationId: prefix + conv, AgentId: "invariant-fake",
			Inputs: []*proto.Step{harnesstest.UserStep("go")},
		}, nil); err != nil {
			t.Fatal(err)
		}
	}
	for id, r := range byID(t, db, prefix) {
		if r.Checks == 0 || r.Violations != 0 {
			t.Errorf("%s: %d checks %d violations %v, want checks and no violation", id, r.Checks, r.Violations, r.Examples)
		}
	}
}

// Each check catches a violation planted for it.
func TestCheck_PlantedViolations(t *testing.T) {
	db, dsn := openTestDB(t)
	ctx := context.Background()
	effects, err := fake.OpenPostgresEffects(ctx, dsn) // creates fake_effects
	if err != nil {
		t.Fatal(err)
	}
	effects.Close()
	log, err := eventlog.OpenPostgresEventLog(dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { log.Close() })

	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := db.Exec(q, args...); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	call := func(conv, class, state string, deadline any) string {
		id := uuid.NewString()
		exec(`INSERT INTO tool_calls (call_id, conversation_id, step, tool_name, args_hash, args, idempotency_key, class, state, attempts, reconcile_deadline, generation)
			VALUES ($1, $2, 1, 't', 'h', '{}', $1, $3, $4, 1, $5, 1)`, id, conv, class, state, deadline)
		return id
	}
	logRow := func(conv string, step, gen int) {
		exec(`INSERT INTO conversation_log (conversation_id, step, payload, generation) VALUES ($1, $2, '{}', $3)`, conv, step, gen)
	}
	appendEv := func(ev *proto.StepEvent) {
		ev.EventId = uuid.NewString()
		if _, err := log.Append(ctx, ev, 0); err != nil {
			t.Fatal(err)
		}
	}
	toolCall := &proto.Step{Type: &proto.Step_ToolCall{ToolCall: &proto.ToolCallStep{
		Id: "c1", Type: &proto.ToolCallStep_FunctionCall{FunctionCall: &proto.FunctionCallStep{Name: "t"}},
	}}}

	plant := map[string]func(conv string){
		"I1": func(conv string) { call(conv, "IDEMPOTENT", "OUTCOME_UNKNOWN", time.Now().Add(-time.Hour)) },
		"I1b": func(conv string) {
			appendEv(&proto.StepEvent{ConversationId: conv, AgentId: "fake", State: proto.State_STATE_COMPLETED})
			call(conv, "UNSAFE", "STARTED", nil)
		},
		"I2": func(conv string) {
			id := call(conv, "IDEMPOTENT", "SUCCEEDED", nil)
			for range 2 {
				exec(`INSERT INTO fake_effects (call_id, idempotency_key, tool_name, payload) VALUES ($1, $1, 't', '{}')`, id)
			}
		},
		"I3'": func(conv string) { logRow(conv, 1, 2); logRow(conv, 2, 1) },
		"I5":  func(conv string) { logRow(conv, 1, 1); logRow(conv, 3, 1) },
		"I6": func(conv string) {
			appendEv(&proto.StepEvent{ConversationId: conv, AgentId: "fake", State: proto.State_STATE_PENDING,
				Steps: []*proto.Step{harnesstest.UserStep("go"), toolCall}})
			appendEv(&proto.StepEvent{ConversationId: conv, State: proto.State_STATE_COMPLETED})
		},
		"I7a": func(conv string) {
			appendEv(&proto.StepEvent{ConversationId: conv, AgentId: "fake", State: proto.State_STATE_PENDING})
			exec(`UPDATE runs SET state = 'COMPLETED' WHERE conversation_id = $1`, conv)
		},
		"I7b": func(conv string) { call(conv, "IDEMPOTENT", "SUCCEEDED", nil) },
	}
	for id, plant := range plant {
		prefix := "invariant-" + id + "-" + uuid.NewString() + "-"
		plant(prefix + "x")
		if r := byID(t, db, prefix)[id]; r.Violations == 0 || len(r.Examples) == 0 {
			t.Errorf("%s missed its planted violation: %+v", id, r)
		}
	}
}
