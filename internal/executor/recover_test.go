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

package executor_test

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
	"github.com/simondxu/conductor/internal/executor"
	"github.com/simondxu/conductor/internal/harness/harnesstest"
	"github.com/simondxu/conductor/proto"
)

// harness_kill through the local executor: the harness subprocess dies after
// the IDEMPOTENT call's effect and before its result. The run fails; with no
// client, the reconciler acquires a new subprocess at the next generation,
// runs the call again under the same key and completes: one effect, two
// attempts.
func TestLocal_HarnessKillIsRecovered(t *testing.T) {
	dsn := os.Getenv("PG_TEST_DSN")
	if dsn == "" {
		t.Skip("PG_TEST_DSN not set")
	}
	if _, err := schema.Up(dsn); err != nil {
		t.Fatal(err)
	}
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
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	local := &executor.Local{Command: []string{os.Args[0]}, Env: []string{"CONDUCTOR_TEST_HARNESS=1"}}
	t.Cleanup(func() { local.Close() })
	reg := controller.NewRegistry()
	if err := reg.Register("fake", &executor.Harness{Executor: local, AgentID: "fake"}, controller.RecoveryFull); err != nil {
		t.Fatal(err)
	}
	reg.DeclareTools("fake", map[string]toolledger.Class{"charge_card": toolledger.Idempotent})
	c, err := controller.New(context.Background(), controller.Config{
		Registry:        reg,
		EventLogBuilder: func() (eventlog.EventLog, error) { return log, nil },
		Ledger:          ledger, Lease: leases, OwnerID: "test-" + uuid.NewString(), LeaseTTL: 2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })

	conv := t.Name() + "-" + uuid.NewString()
	ctx := context.Background()
	err = c.Exec(ctx, &proto.CreateInteractionEvent{
		ConversationId: conv, AgentId: "fake",
		AgentConfig: []byte(`{"turns":[{"tool_calls":[{"name":"charge_card"}]},{"content":"done"}],"tools":{"charge_card":{"idempotent":true,"crash_after_effect":true}}}`),
		Inputs:      []*proto.Step{harnesstest.UserStep("go")},
	}, nil)
	if err == nil {
		t.Fatal("the run survived its harness dying")
	}

	loopCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go c.RunReconciler(loopCtx, controller.ReconcilerConfig{Interval: 100 * time.Millisecond})
	deadline := time.Now().Add(20 * time.Second)
	for {
		var state string
		if err := db.QueryRow("SELECT state FROM runs WHERE conversation_id = $1", conv).Scan(&state); err != nil {
			t.Fatal(err)
		}
		if state == "COMPLETED" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("run is %s, want COMPLETED", state)
		}
		time.Sleep(100 * time.Millisecond)
	}

	var call string
	var effects int
	if err := db.QueryRow(`SELECT t.state || ' ' || t.attempts, (SELECT count(*) FROM fake_effects e WHERE e.call_id = t.call_id)
		FROM tool_calls t WHERE t.conversation_id = $1`, conv).Scan(&call, &effects); err != nil {
		t.Fatal(err)
	}
	if call != "SUCCEEDED 2" || effects != 1 {
		t.Fatalf("call = %q with %d effects, want SUCCEEDED 2 with 1", call, effects)
	}
}
