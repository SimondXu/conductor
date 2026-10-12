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
	"errors"
	"os"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/protobuf/types/known/structpb"

	schema "github.com/simondxu/conductor/db"
	"github.com/simondxu/conductor/internal/controller/lease"
)

// openTestLedger returns a ledger on PG_TEST_DSN, skipping when it is unset.
func openTestLedger(t *testing.T) *Postgres {
	t.Helper()
	dsn := os.Getenv("PG_TEST_DSN")
	if dsn == "" {
		t.Skip("PG_TEST_DSN not set; skipping Postgres tool ledger tests")
	}
	if _, err := schema.Up(dsn); err != nil {
		t.Fatalf("migrate up: %v", err)
	}
	l, err := OpenPostgres(dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	for _, table := range []string{"tool_calls", "conversation_leases"} {
		if _, err := l.db.Exec("DELETE FROM "+table+" WHERE conversation_id = $1", t.Name()); err != nil {
			t.Fatal(err)
		}
	}
	return l
}

func plan(t *testing.T, l Ledger, step int64) string {
	t.Helper()
	args, _ := structpb.NewStruct(map[string]any{"amount": 4200})
	hash, canonical, err := ArgsHash(args)
	if err != nil {
		t.Fatal(err)
	}
	id := CallID(t.Name(), step, "charge_card", hash)
	if err := l.Plan(context.Background(), 0, PlannedCall{
		CallID: id, ConversationID: t.Name(), Step: step, ToolName: "charge_card",
		ArgsHash: hash, Args: canonical, Class: Idempotent,
	}); err != nil {
		t.Fatal(err)
	}
	return id
}

func only(t *testing.T, l Ledger) ToolCall {
	t.Helper()
	calls, err := l.Calls(context.Background(), t.Name())
	if err != nil || len(calls) != 1 {
		t.Fatalf("calls = %v, err = %v; want exactly one", calls, err)
	}
	return calls[0]
}

func TestHappyPath(t *testing.T) {
	l := openTestLedger(t)
	ctx := context.Background()
	id := plan(t, l, 3)
	if err := l.Plan(ctx, 0, PlannedCall{CallID: id, ConversationID: t.Name(), Step: 3, ToolName: "x", Args: []byte("{}"), Class: Unsafe}); err != nil {
		t.Fatalf("re-plan: %v", err)
	}
	if c := only(t, l); c.State != Planned || c.Attempts != 0 || c.IdempotencyKey != id || c.Class != Idempotent {
		t.Fatalf("after plan: %+v", c)
	}
	if err := l.Start(ctx, 0, id); err != nil {
		t.Fatal(err)
	}
	out := Outcome{State: Succeeded, Result: []byte(`{"status":"ok"}`)}
	if err := l.Settle(ctx, 0, id, out); err != nil {
		t.Fatal(err)
	}
	if err := l.Settle(ctx, 0, id, out); err != nil {
		t.Fatalf("duplicate settle: %v", err)
	}
	c := only(t, l)
	if c.State != Succeeded || c.Attempts != 1 || string(c.Result) != `{"status": "ok"}` {
		t.Fatalf("after settle: %+v (result %s)", c, c.Result)
	}
}

func TestOutcomeUnknownThenRetry(t *testing.T) {
	l := openTestLedger(t)
	ctx := context.Background()
	metric := func(c *prometheus.CounterVec, labels ...string) float64 {
		return testutil.ToFloat64(c.WithLabelValues(labels...))
	}
	retried0 := metric(outcomeUnknown, "charge_card", resolutionRetried)
	attempts0 := metric(toolAttempts, "charge_card", string(Idempotent))
	succeeded0 := metric(toolCalls, "charge_card", string(Idempotent), string(Succeeded))
	id := plan(t, l, 1)
	if err := l.Start(ctx, 0, id); err != nil {
		t.Fatal(err)
	}
	if err := l.MarkUnknown(ctx, 0, id, "turn failed"); err != nil {
		t.Fatal(err)
	}
	if c := only(t, l); c.State != OutcomeUnknown || c.Error != "turn failed" || c.Attempts != 1 {
		t.Fatalf("after unknown: %+v", c)
	}
	// IDEMPOTENT retry: OUTCOME_UNKNOWN -> STARTED, attempts++.
	if err := l.Start(ctx, 0, id); err != nil {
		t.Fatal(err)
	}
	if err := l.Settle(ctx, 0, id, Outcome{State: Succeeded, Result: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if c := only(t, l); c.State != Succeeded || c.Attempts != 2 {
		t.Fatalf("after retry: %+v", c)
	}
	if d := metric(outcomeUnknown, "charge_card", resolutionRetried) - retried0; d != 1 {
		t.Errorf("outcome_unknown{retried} += %v, want 1", d)
	}
	if d := metric(toolAttempts, "charge_card", string(Idempotent)) - attempts0; d != 2 {
		t.Errorf("attempts += %v, want 2", d)
	}
	if d := metric(toolCalls, "charge_card", string(Idempotent), string(Succeeded)) - succeeded0; d != 1 {
		t.Errorf("tool_calls{SUCCEEDED} += %v, want 1", d)
	}
}

// I1's bound: an OUTCOME_UNKNOWN call past its deadline is abandoned; one
// still STARTED, or within its deadline, is not.
func TestExpireDeadlines(t *testing.T) {
	l := openTestLedger(t)
	ctx := context.Background()
	expired, started := plan(t, l, 1), plan(t, l, 2)
	for _, id := range []string{expired, started} {
		if err := l.Start(ctx, 0, id); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.MarkUnknown(ctx, 0, expired, "turn failed"); err != nil {
		t.Fatal(err)
	}
	if _, err := l.db.Exec("UPDATE tool_calls SET reconcile_deadline = now() - interval '1 second' WHERE conversation_id = $1", t.Name()); err != nil {
		t.Fatal(err)
	}
	if n, err := l.CountUnsettled(ctx); err != nil || n[Started] < 1 || n[OutcomeUnknown] < 1 {
		t.Fatalf("CountUnsettled = %v, %v; want at least one STARTED and one OUTCOME_UNKNOWN", n, err)
	}
	if n, err := l.ExpireDeadlines(ctx); err != nil || n < 1 {
		t.Fatalf("ExpireDeadlines = %d, %v; want at least 1", n, err)
	}
	calls, err := l.Calls(ctx, t.Name())
	if err != nil {
		t.Fatal(err)
	}
	if calls[0].State != Abandoned || calls[0].Error != DeadlineExceeded || calls[1].State != Started {
		t.Fatalf("calls = %s %q, %s; want ABANDONED %q, STARTED", calls[0].State, calls[0].Error, calls[1].State, DeadlineExceeded)
	}
}

// I4 for the ledger: once another owner holds the conversation, the old
// owner's transitions are rejected and the row is untouched.
func TestFencedTransitionsRejectStaleGeneration(t *testing.T) {
	l := openTestLedger(t)
	ctx := context.Background()
	id := plan(t, l, 1) // generation 0: never leased
	leases, err := lease.OpenPostgres(os.Getenv("PG_TEST_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { leases.Close() })
	a, err := leases.Acquire(ctx, t.Name(), "a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Start(ctx, 0, id); !errors.Is(err, lease.ErrFenced) {
		t.Fatalf("start at generation 0 on a leased conversation: err = %v, want ErrFenced", err)
	}
	if err := l.Start(ctx, a.Generation(), id); err != nil {
		t.Fatal(err)
	}
	if _, err := l.db.Exec("UPDATE conversation_leases SET expires_at = now() WHERE conversation_id = $1", t.Name()); err != nil {
		t.Fatal(err)
	}
	b, err := leases.Acquire(ctx, t.Name(), "b", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Settle(ctx, a.Generation(), id, Outcome{State: Succeeded, Result: []byte(`{}`)}); !errors.Is(err, lease.ErrFenced) {
		t.Fatalf("stale settle: err = %v, want ErrFenced", err)
	}
	if err := l.Plan(ctx, a.Generation(), PlannedCall{CallID: id + "x", ConversationID: t.Name(), Step: 2, ToolName: "t", Args: []byte("{}"), Class: Unsafe}); !errors.Is(err, lease.ErrFenced) {
		t.Fatalf("stale plan: err = %v, want ErrFenced", err)
	}
	if c := only(t, l); c.State != Started || c.Generation != a.Generation() {
		t.Fatalf("after stale writes: %s at generation %d, want STARTED at %d", c.State, c.Generation, a.Generation())
	}
	if err := l.MarkUnknown(ctx, b.Generation(), id, "owner lost"); err != nil {
		t.Fatalf("new owner: %v", err)
	}
}

func TestIllegalTransitions(t *testing.T) {
	l := openTestLedger(t)
	ctx := context.Background()
	id := plan(t, l, 1)

	for name, err := range map[string]error{
		"settle PLANNED":      l.Settle(ctx, 0, id, Outcome{State: Succeeded}),
		"unknown PLANNED":     l.MarkUnknown(ctx, 0, id, "x"),
		"start missing call":  l.Start(ctx, 0, "no-such-call"),
		"settle to non-final": l.Settle(ctx, 0, id, Outcome{State: Started}),
	} {
		if !errors.Is(err, ErrTransition) {
			t.Errorf("%s: err = %v, want ErrTransition", name, err)
		}
	}

	_ = l.Start(ctx, 0, id)
	_ = l.Settle(ctx, 0, id, Outcome{State: Failed})
	for name, err := range map[string]error{
		"start settled":       l.Start(ctx, 0, id),
		"resettle other":      l.Settle(ctx, 0, id, Outcome{State: Succeeded}),
		"unknown after final": l.MarkUnknown(ctx, 0, id, "x"),
	} {
		if !errors.Is(err, ErrTransition) {
			t.Errorf("%s: err = %v, want ErrTransition", name, err)
		}
	}
}

func TestCallIDIsStableAndSensitive(t *testing.T) {
	a, _ := structpb.NewStruct(map[string]any{"b": 1, "a": 2})
	b, _ := structpb.NewStruct(map[string]any{"a": 2, "b": 1})
	ha, _, _ := ArgsHash(a)
	hb, _, _ := ArgsHash(b)
	if ha != hb {
		t.Fatal("args hash depends on key order")
	}
	base := CallID("c", 1, "t", ha)
	if len(base) != 32 || base != CallID("c", 1, "t", hb) {
		t.Fatalf("call id %q not stable", base)
	}
	for _, other := range []string{CallID("c2", 1, "t", ha), CallID("c", 2, "t", ha), CallID("c", 1, "u", ha), CallID("c", 1, "t", "x")} {
		if other == base {
			t.Errorf("call id did not change with its inputs")
		}
	}
}

// An operator may record what an ABANDONED call really did, and nothing
// else: not a call still in flight, and not back to ABANDONED.
func TestOverrideOnlyFromAbandoned(t *testing.T) {
	l := openTestLedger(t)
	ctx := context.Background()
	id := plan(t, l, 1)
	if err := l.Start(ctx, 0, id); err != nil {
		t.Fatal(err)
	}
	if err := l.Override(ctx, 0, id, Succeeded, "checked"); !errors.Is(err, ErrTransition) {
		t.Fatalf("override STARTED: err = %v, want ErrTransition", err)
	}
	if err := l.Settle(ctx, 0, id, Outcome{State: Abandoned, Error: "unknown"}); err != nil {
		t.Fatal(err)
	}
	if err := l.Override(ctx, 0, id, Abandoned, "x"); !errors.Is(err, ErrTransition) {
		t.Fatalf("override to ABANDONED: err = %v, want ErrTransition", err)
	}
	if err := l.Override(ctx, 0, id, Succeeded, "the email is in the provider's sent log"); err != nil {
		t.Fatal(err)
	}
	if c := only(t, l); c.State != Succeeded || c.Error != "operator: the email is in the provider's sent log" {
		t.Fatalf("after override: %s %q", c.State, c.Error)
	}
	// Not a silent no-op on a call that is no longer ABANDONED.
	if err := l.Override(ctx, 0, id, Succeeded, "again"); !errors.Is(err, ErrTransition) {
		t.Fatalf("override SUCCEEDED: err = %v, want ErrTransition", err)
	}
}
