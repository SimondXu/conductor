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

package controller

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	schema "github.com/simondxu/conductor/db"
	"github.com/simondxu/conductor/internal/controller/eventlog"
	"github.com/simondxu/conductor/internal/controller/lease"
	"github.com/simondxu/conductor/internal/controller/toolledger"
	"github.com/simondxu/conductor/internal/harness"
	"github.com/simondxu/conductor/internal/harness/fake"
	"github.com/simondxu/conductor/internal/harness/harnesstest"
	"github.com/simondxu/conductor/proto"
)

// recRig is a controller on Postgres with every recovery part: event log
// with the runs projection, tool ledger, leases.
type recRig struct {
	c       *Controller
	log     eventlog.EventLog
	runs    eventlog.RunIndex
	ledger  *toolledger.Postgres
	leases  *lease.Postgres
	db      *sql.DB
	effects *fake.MemoryEffects
	conv    string
}

func newRecRig(t *testing.T, register func(*Registry, *fake.MemoryEffects)) *recRig {
	t.Helper()
	dsn := os.Getenv("PG_TEST_DSN")
	if dsn == "" {
		t.Skip("PG_TEST_DSN not set; skipping reconciler integration tests")
	}
	if _, err := schema.Up(dsn); err != nil {
		t.Fatalf("migrate up: %v", err)
	}
	r := &recRig{effects: &fake.MemoryEffects{}, conv: t.Name() + "-" + uuid.NewString()}
	var err error
	if r.log, err = eventlog.OpenPostgresEventLog(dsn); err != nil {
		t.Fatal(err)
	}
	r.runs = r.log.(eventlog.RunIndex)
	if r.ledger, err = toolledger.OpenPostgres(dsn); err != nil {
		t.Fatal(err)
	}
	if r.leases, err = lease.OpenPostgres(dsn); err != nil {
		t.Fatal(err)
	}
	if r.db, err = sql.Open("pgx", dsn); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.db.Close() })
	reg := NewRegistry()
	register(reg, r.effects)
	r.c, err = New(context.Background(), Config{
		Registry:        reg,
		EventLogBuilder: func() (eventlog.EventLog, error) { return r.log, nil },
		Ledger:          r.ledger,
		Lease:           r.leases,
		OwnerID:         "test-" + uuid.NewString(),
		LeaseTTL:        2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.c.Close() })
	return r
}

// The DoD of M4: an owner dies inside a tool turn, still holding its lease.
// No client comes back. Once the lease expires the reconciler loop takes the
// run over at a higher generation and completes it, re-running the
// IDEMPOTENT call under the same key: two attempts, one effect.
func TestReconciler_FinishesKilledOwnersRun(t *testing.T) {
	var recovered atomic.Bool
	r := newRecRig(t, func(reg *Registry, effects *fake.MemoryEffects) {
		if err := reg.Register("fake", crashingHarness{fake.New(effects), &recovered, false}, RecoveryFull); err != nil {
			t.Fatal(err)
		}
		reg.DeclareTools("fake", map[string]toolledger.Class{"charge_card": toolledger.Idempotent})
	})
	ctx := context.Background()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = r.c.Exec(ctx, &proto.CreateInteractionEvent{
			ConversationId: r.conv, AgentId: "fake",
			AgentConfig: []byte(`{"turns":[{"tool_calls":[{"name":"charge_card"}]},{"content":"done"}],"tools":{"charge_card":{"idempotent":true}}}`),
			Inputs:      []*proto.Step{harnesstest.UserStep("go")},
		}, nil)
	}()
	<-done
	// The goroutine's deferred release ran; a killed process's would not.
	// Hold the lease as that dead owner, never renewing it.
	dead, err := r.leases.Acquire(ctx, r.conv, "dead-owner", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	recovered.Store(true)

	if err := r.c.reconcileOnce(ctx, r.runs, ReconcilerConfig{Concurrency: 4, MaxAttempts: 5}); err != nil {
		t.Fatal(err)
	}
	if st := rebuilt(t, r.log, r.conv).State; st != proto.State_STATE_PENDING {
		t.Fatalf("recovered under a live lease: state %v", st)
	}

	loopCtx, cancel := context.WithCancel(ctx)
	loopDone := make(chan error)
	go func() { loopDone <- r.c.RunReconciler(loopCtx, ReconcilerConfig{Interval: 100 * time.Millisecond}) }()
	deadline := time.Now().Add(15 * time.Second)
	for rebuilt(t, r.log, r.conv).State != proto.State_STATE_COMPLETED {
		if time.Now().After(deadline) {
			t.Fatal("the reconciler did not complete the run")
		}
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	if err := <-loopDone; err != nil {
		t.Fatal(err)
	}

	calls, err := r.ledger.Calls(ctx, r.conv)
	if err != nil {
		t.Fatal(err)
	}
	c := calls[0]
	if c.State != toolledger.Succeeded || c.Attempts != 2 || len(r.effects.Effects) != 1 {
		t.Fatalf("call = %s attempts %d effects %d, want SUCCEEDED after 2 with 1 effect", c.State, c.Attempts, len(r.effects.Effects))
	}
	if c.Generation <= dead.Generation() {
		t.Errorf("call settled at generation %d, want above the dead owner's %d", c.Generation, dead.Generation())
	}
}

// A run whose recovery keeps failing is failed after MaxAttempts, not
// retried forever.
func TestReconciler_PoisonsRunThatKeepsFailing(t *testing.T) {
	r := newRecRig(t, func(reg *Registry, _ *fake.MemoryEffects) {
		broken := &testHarness{startFunc: func(context.Context, string) (harness.Execution, error) {
			return nil, errors.New("harness unavailable")
		}}
		if err := reg.Register("broken", broken, RecoveryFull); err != nil {
			t.Fatal(err)
		}
	})
	ctx := context.Background()
	if _, err := r.log.Append(ctx, &proto.StepEvent{
		ConversationId: r.conv, EventId: uuid.NewString(), AgentId: "broken", State: proto.State_STATE_PENDING,
		Steps: []*proto.Step{harnesstest.UserStep("go")},
	}); err != nil {
		t.Fatal(err)
	}
	cfg := ReconcilerConfig{Concurrency: 4, MaxAttempts: 2}
	for pass := 1; pass <= 3; pass++ {
		if err := r.c.reconcileOnce(ctx, r.runs, cfg); err != nil {
			t.Fatal(err)
		}
		if _, err := r.db.Exec("UPDATE runs SET next_attempt_at = NULL WHERE conversation_id = $1", r.conv); err != nil {
			t.Fatal(err)
		}
		want := proto.State_STATE_PENDING
		if pass == 3 {
			want = proto.State_STATE_FAILED
		}
		if st := rebuilt(t, r.log, r.conv).State; st != want {
			t.Fatalf("after pass %d state = %v, want %v", pass, st, want)
		}
	}
}

// A client cannot drive a conversation another owner holds.
func TestExec_RefusedWhileLeaseHeld(t *testing.T) {
	r := newRecRig(t, func(reg *Registry, effects *fake.MemoryEffects) {
		if err := reg.Register("fake", fake.New(effects), RecoveryFull); err != nil {
			t.Fatal(err)
		}
	})
	ctx := context.Background()
	held, err := r.leases.Acquire(ctx, r.conv, "other", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Release(ctx)
	err = r.c.Exec(ctx, &proto.CreateInteractionEvent{
		ConversationId: r.conv, AgentId: "fake", Inputs: []*proto.Step{harnesstest.UserStep("go")},
	}, nil)
	if !errors.Is(err, lease.ErrHeld) {
		t.Fatalf("err = %v, want lease.ErrHeld", err)
	}
	if len(r.effects.Effects) != 0 {
		t.Fatal("ran without the lease")
	}
}
