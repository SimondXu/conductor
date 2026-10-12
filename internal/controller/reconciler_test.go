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
// with the runs projection, tool ledger, leases. More controllers on the same
// database, sharing the effect sink, come from controller.
type recRig struct {
	c        *Controller
	log      eventlog.EventLog
	runs     eventlog.RunIndex
	ledger   *toolledger.Postgres
	leases   *lease.Postgres
	db       *sql.DB
	effects  *fake.MemoryEffects
	conv     string
	dsn      string
	register func(*Registry, *fake.MemoryEffects)
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
	r := &recRig{effects: &fake.MemoryEffects{}, conv: t.Name() + "-" + uuid.NewString(), dsn: dsn, register: register}
	var err error
	if r.log, err = eventlog.OpenPostgresEventLog(dsn); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.log.Close() })
	r.runs = r.log.(eventlog.RunIndex)
	if r.ledger, err = toolledger.OpenPostgres(dsn); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.ledger.Close() })
	if r.leases, err = lease.OpenPostgres(dsn); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.leases.Close() })
	if r.db, err = sql.Open("pgx", dsn); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.db.Close() })
	r.c = r.controller(t, "test-"+uuid.NewString())
	return r
}

// controller is a new controller replica with its own connections.
func (r *recRig) controller(t *testing.T, ownerID string) *Controller {
	t.Helper()
	log, err := eventlog.OpenPostgresEventLog(r.dsn)
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := toolledger.OpenPostgres(r.dsn)
	if err != nil {
		t.Fatal(err)
	}
	leases, err := lease.OpenPostgres(r.dsn)
	if err != nil {
		t.Fatal(err)
	}
	reg := NewRegistry()
	r.register(reg, r.effects)
	c, err := New(context.Background(), Config{
		Registry:        reg,
		EventLogBuilder: func() (eventlog.EventLog, error) { return log, nil },
		Ledger:          ledger,
		Lease:           leases,
		OwnerID:         ownerID,
		LeaseTTL:        2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// reconcile runs c's reconciler loop until the test ends.
func reconcile(t *testing.T, c *Controller) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { done <- c.RunReconciler(ctx, ReconcilerConfig{Interval: 100 * time.Millisecond}) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
}

// waitState polls the log until the run reaches want.
func (r *recRig) waitState(t *testing.T, want proto.State) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for rebuilt(t, r.log, r.conv).State != want {
		if time.Now().After(deadline) {
			t.Fatalf("run did not reach %v", want)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// checkOneTerminalState fails unless the log has exactly one terminal
// event, COMPLETED, and its generations never decrease (I3').
func (r *recRig) checkOneTerminalState(t *testing.T) {
	t.Helper()
	var terminal []string
	rows, err := r.db.Query(`SELECT payload::jsonb->>'state' FROM conversation_log
		WHERE conversation_id = $1 AND payload::jsonb->>'state' IN ('STATE_COMPLETED', 'STATE_FAILED', 'STATE_CANCELED')`, r.conv)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		terminal = append(terminal, s)
	}
	if len(terminal) != 1 || terminal[0] != "STATE_COMPLETED" {
		t.Errorf("terminal events %v, want exactly one STATE_COMPLETED", terminal)
	}
	var violations int
	if err := r.db.QueryRow(`SELECT count(*) FROM (
		SELECT generation, lag(generation) OVER (ORDER BY step) AS prev
		  FROM conversation_log WHERE conversation_id = $1 AND generation IS NOT NULL) s
		 WHERE prev IS NOT NULL AND generation < prev`, r.conv).Scan(&violations); err != nil {
		t.Fatal(err)
	}
	if violations != 0 {
		t.Errorf("I3' violated: generation decreased %d time(s)", violations)
	}
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
	}, 0); err != nil {
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

// I8: with less lease left than the UNSAFE budget, even after a renewal, the
// tool turn does not start: the call stays PLANNED with no attempt and no
// effect, and the run stays PENDING. With the default budget it then runs.
func TestUnsafeToolNotStartedNearLeaseExpiry(t *testing.T) {
	r := newRecRig(t, func(reg *Registry, effects *fake.MemoryEffects) {
		if err := reg.Register("fake", fake.New(effects), RecoveryFull); err != nil {
			t.Fatal(err)
		}
	})
	ctx := context.Background()
	budget := r.c.unsafeBudget
	r.c.unsafeBudget = 5 * time.Second // above the rig's 2s ttl
	err := r.c.Exec(ctx, &proto.CreateInteractionEvent{
		ConversationId: r.conv, AgentId: "fake",
		AgentConfig: []byte(`{"turns":[{"tool_calls":[{"name":"send_email"}]},{"content":"done"}]}`),
		Inputs:      []*proto.Step{harnesstest.UserStep("go")},
	}, nil)
	if !errors.Is(err, errLeaseMargin) {
		t.Fatalf("err = %v, want errLeaseMargin", err)
	}
	calls, err := r.ledger.Calls(ctx, r.conv)
	if err != nil {
		t.Fatal(err)
	}
	if len(calls) != 1 || calls[0].Class != toolledger.Unsafe || calls[0].State != toolledger.Planned || calls[0].Attempts != 0 {
		t.Fatalf("calls = %+v, want one UNSAFE call PLANNED with no attempt", calls)
	}
	if len(r.effects.Effects) != 0 {
		t.Fatal("the UNSAFE call ran inside the lease margin")
	}
	r.waitState(t, proto.State_STATE_PENDING)

	r.c.unsafeBudget = budget
	if err := r.c.Exec(ctx, &proto.CreateInteractionEvent{ConversationId: r.conv}, nil); err != nil {
		t.Fatal(err)
	}
	r.waitState(t, proto.State_STATE_COMPLETED)
	if len(r.effects.Effects) != 1 {
		t.Fatalf("effects = %d, want 1", len(r.effects.Effects))
	}
}

// M7 DoD: three replicas. The holder dies inside a tool turn with its lease
// held. B and C both run the reconciler; exactly one takes the run over at a
// higher generation and completes it once. A's writes at its old generation
// are rejected by the database, in the log and in the ledger.
func TestReplicas_KillHolderTransfersOwnership(t *testing.T) {
	var recovered atomic.Bool
	r := newRecRig(t, func(reg *Registry, effects *fake.MemoryEffects) {
		if err := reg.Register("fake", crashingHarness{fake.New(effects), &recovered, false}, RecoveryFull); err != nil {
			t.Fatal(err)
		}
		reg.DeclareTools("fake", map[string]toolledger.Class{"charge_card": toolledger.Idempotent})
	})
	ctx := context.Background()
	b, c := r.controller(t, "replica-b"), r.controller(t, "replica-c")
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
	// A was killed holding its lease: take it as A, never renewing.
	dead, err := r.leases.Acquire(ctx, r.conv, r.c.ownerID, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	recovered.Store(true)
	reconcile(t, b)
	reconcile(t, c)
	r.waitState(t, proto.State_STATE_COMPLETED)
	r.checkOneTerminalState(t)

	calls, err := r.ledger.Calls(ctx, r.conv)
	if err != nil {
		t.Fatal(err)
	}
	call := calls[0]
	if call.State != toolledger.Succeeded || call.Attempts != 2 || len(r.effects.Effects) != 1 || call.Generation <= dead.Generation() {
		t.Fatalf("call = %s attempts %d generation %d effects %d; want SUCCEEDED, 2 attempts above generation %d, 1 effect",
			call.State, call.Attempts, call.Generation, len(r.effects.Effects), dead.Generation())
	}

	stale := newLogger(r.log, r.conv, "")
	stale.gen = dead.Generation()
	if _, err := stale.LogOutputs(ctx, nil, proto.State_STATE_FAILED); !errors.Is(err, lease.ErrFenced) {
		t.Fatalf("stale append: err = %v, want ErrFenced", err)
	}
	if err := r.ledger.MarkUnknown(ctx, dead.Generation(), call.CallID, "stale"); !errors.Is(err, lease.ErrFenced) {
		t.Fatalf("stale ledger write: err = %v, want ErrFenced", err)
	}
	r.checkOneTerminalState(t)
}

// M7 DoD: a rolling update. A is mid-tool-call when it is told to stop: its
// run is canceled, as srv.Stop does after the drain timeout, and it releases
// its lease on the way out. B takes the run over without waiting for the
// lease ttl and completes it; the log has one terminal state.
func TestReplicas_RollingUpdateHandsOverOnce(t *testing.T) {
	r := newRecRig(t, func(reg *Registry, effects *fake.MemoryEffects) {
		if err := reg.Register("fake", fake.New(effects), RecoveryFull); err != nil {
			t.Fatal(err)
		}
		reg.DeclareTools("fake", map[string]toolledger.Class{"charge_card": toolledger.Idempotent})
	})
	b := r.controller(t, "replica-b")
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan error)
	go func() {
		done <- r.c.Exec(ctx, &proto.CreateInteractionEvent{
			ConversationId: r.conv, AgentId: "fake",
			AgentConfig: []byte(`{"turns":[{"tool_calls":[{"name":"charge_card"}]},{"content":"done"}],"tools":{"charge_card":{"idempotent":true,"delay_ms":1000}}}`),
			Inputs:      []*proto.Step{harnesstest.UserStep("go")},
		}, nil)
	}()
	deadline := time.Now().Add(10 * time.Second)
	for {
		calls, err := r.ledger.Calls(context.Background(), r.conv)
		if err != nil {
			t.Fatal(err)
		}
		if len(calls) == 1 && calls[0].State == toolledger.Started {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the call never started")
		}
		time.Sleep(20 * time.Millisecond)
	}
	stop()
	if err := <-done; err == nil {
		t.Fatal("canceled run returned no error")
	}
	handover := time.Now()
	reconcile(t, b)
	r.waitState(t, proto.State_STATE_COMPLETED)
	if took := time.Since(handover); took > 2*time.Second+time.Second {
		t.Errorf("handover took %v; a released lease should not wait out the 2s ttl", took)
	}
	r.checkOneTerminalState(t)
	calls, _ := r.ledger.Calls(context.Background(), r.conv)
	if calls[0].State != toolledger.Succeeded || calls[0].Attempts != 2 || len(r.effects.Effects) != 1 {
		t.Fatalf("call = %s attempts %d effects %d, want SUCCEEDED after 2 with 1 effect", calls[0].State, calls[0].Attempts, len(r.effects.Effects))
	}
}

// A run released unfinished is taken over at once, not at the next pass:
// B's reconciler is already idle with a one-minute interval when A's caller
// goes away mid-tool, and B still completes the run within seconds.
func TestReconciler_TakesOverHandoverAtOnce(t *testing.T) {
	r := newRecRig(t, func(reg *Registry, effects *fake.MemoryEffects) {
		if err := reg.Register("fake", fake.New(effects), RecoveryFull); err != nil {
			t.Fatal(err)
		}
		reg.DeclareTools("fake", map[string]toolledger.Class{"charge_card": toolledger.Idempotent})
	})
	b := r.controller(t, "replica-b")
	loopCtx, stopLoop := context.WithCancel(context.Background())
	loopDone := make(chan error)
	go func() { loopDone <- b.RunReconciler(loopCtx, ReconcilerConfig{Interval: time.Minute}) }()
	t.Cleanup(func() {
		stopLoop()
		if err := <-loopDone; err != nil {
			t.Error(err)
		}
	})

	ctx, stop := context.WithCancel(context.Background())
	done := make(chan error)
	go func() {
		done <- r.c.Exec(ctx, &proto.CreateInteractionEvent{
			ConversationId: r.conv, AgentId: "fake",
			AgentConfig: []byte(`{"turns":[{"tool_calls":[{"name":"charge_card"}]},{"content":"done"}],"tools":{"charge_card":{"idempotent":true,"delay_ms":1000}}}`),
			Inputs:      []*proto.Step{harnesstest.UserStep("go")},
		}, nil)
	}()
	deadline := time.Now().Add(10 * time.Second)
	for {
		calls, err := r.ledger.Calls(context.Background(), r.conv)
		if err != nil {
			t.Fatal(err)
		}
		if len(calls) == 1 && calls[0].State == toolledger.Started {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the call never started")
		}
		time.Sleep(20 * time.Millisecond)
	}
	stop()
	if err := <-done; err == nil {
		t.Fatal("canceled run returned no error")
	}
	handover := time.Now()
	r.waitState(t, proto.State_STATE_COMPLETED)
	if took := time.Since(handover); took > 5*time.Second {
		t.Errorf("handover took %v; it should not wait for the reconciler's next pass", took)
	}
	r.checkOneTerminalState(t)
}

// A recovery that loses the lease to another owner never tried the run: its
// attempt is refunded and the backoff cleared, so a later takeover does not
// wait for it.
func TestRecoverOne_HeldAttemptIsRefunded(t *testing.T) {
	r := newRecRig(t, func(reg *Registry, effects *fake.MemoryEffects) {
		if err := reg.Register("fake", fake.New(effects), RecoveryFull); err != nil {
			t.Fatal(err)
		}
	})
	ctx := context.Background()
	if _, err := r.log.Append(ctx, &proto.StepEvent{
		ConversationId: r.conv, EventId: uuid.NewString(), AgentId: "fake", State: proto.State_STATE_PENDING,
		Steps: []*proto.Step{harnesstest.UserStep("go")},
	}, 0); err != nil {
		t.Fatal(err)
	}
	held, err := r.leases.Acquire(ctx, r.conv, "other", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Release(ctx)
	r.c.recoverOne(ctx, r.runs, eventlog.Candidate{ConversationID: r.conv, AgentID: "fake"}, 5)
	var attempts int
	var next sql.NullTime
	if err := r.db.QueryRow("SELECT recovery_attempts, next_attempt_at FROM runs WHERE conversation_id = $1", r.conv).Scan(&attempts, &next); err != nil {
		t.Fatal(err)
	}
	if attempts != 0 || next.Valid {
		t.Fatalf("after a held recovery attempts = %d next_attempt_at valid = %v, want 0 and none", attempts, next.Valid)
	}
}
