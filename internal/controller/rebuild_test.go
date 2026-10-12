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
	"bytes"
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	gproto "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/simondxu/conductor/internal/controller/eventlog"
	"github.com/simondxu/conductor/internal/controller/eventlog/eventlogtest"
	"github.com/simondxu/conductor/internal/controller/toolledger"
	"github.com/simondxu/conductor/internal/harness"
	"github.com/simondxu/conductor/internal/harness/fake"
	"github.com/simondxu/conductor/internal/harness/harnesstest"
	"github.com/simondxu/conductor/proto"
)

// I6: rebuild is a pure function of the events. The same events give the
// same state every time, and rebuilding leaves the events as they were.
func TestRebuildIsPure(t *testing.T) {
	cfg, _ := structpb.NewStruct(map[string]any{"turns": []any{"x"}})
	call := &proto.Step{Type: &proto.Step_ToolCall{ToolCall: &proto.ToolCallStep{
		Id: "c1", Type: &proto.ToolCallStep_FunctionCall{FunctionCall: &proto.FunctionCallStep{Name: "charge_card"}},
	}}}
	result := &proto.Step{Type: &proto.Step_ToolResult{ToolResult: &proto.ToolResultStep{
		CallId: "c1", Type: &proto.ToolResultStep_FunctionResult{FunctionResult: &proto.FunctionResultStep{Name: "charge_card"}},
	}}}
	events := []*proto.StepEvent{
		{ConversationId: "c", AgentId: "fake", AgentConfig: cfg, State: proto.State_STATE_PENDING,
			Steps: []*proto.Step{harnesstest.UserStep("go"), call}},
		{ConversationId: "c", State: proto.State_STATE_PENDING, Steps: []*proto.Step{result}},
		{ConversationId: "c", AgentId: "other", State: proto.State_STATE_COMPLETED},
	}
	before := make([]*proto.StepEvent, len(events))
	for i, ev := range events {
		before[i] = gproto.Clone(ev).(*proto.StepEvent)
	}

	first, err := rebuild(events)
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		again, err := rebuild(events)
		if err != nil {
			t.Fatal(err)
		}
		if again.State != first.State || again.HarnessID != first.HarnessID || !bytes.Equal(again.AgentConfig, first.AgentConfig) ||
			!slices.EqualFunc(again.History, first.History, func(a, b *proto.Step) bool { return gproto.Equal(a, b) }) {
			t.Fatalf("rebuild gave %+v, then %+v", first, again)
		}
	}
	if first.State != proto.State_STATE_COMPLETED || first.HarnessID != "fake" || len(first.History) != 3 {
		t.Fatalf("rebuilt %v %q with %d steps, want COMPLETED fake with 3", first.State, first.HarnessID, len(first.History))
	}
	for i := range events {
		if !gproto.Equal(events[i], before[i]) {
			t.Fatalf("rebuild changed event %d", i)
		}
	}
}

// I6: what the controller drives the harness with after a recovery is the
// state rebuilt from the log alone. Every turn, before and after the crash,
// queues exactly the rebuilt history of the log at that moment.
func TestPostRecoveryStateEqualsRebuilt(t *testing.T) {
	var recovered atomic.Bool
	var log *eventlogtest.MemoryEventLog
	rec := &rebuiltChecker{log: func() eventlog.EventLog { return log }}
	r := newLedgerRig(t, func(h harness.Harness) harness.Harness {
		rec.Harness = crashingHarness{h, &recovered, false}
		return rec
	})
	log, rec.conv = r.log, r.conv
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = r.exec(`{"turns":[{"tool_calls":[{"name":"charge_card"}]},{"content":"done"}],"tools":{"charge_card":{"idempotent":true}}}`)
	}()
	<-done
	recovered.Store(true)
	before := rec.turns()
	if err := r.resume(); err != nil {
		t.Fatalf("resume: %v", err)
	}
	r.wantState(t, proto.State_STATE_COMPLETED)
	if rec.turns() <= before {
		t.Fatal("no turn ran after the recovery")
	}
	if len(rec.mismatches) > 0 {
		t.Fatal(rec.mismatches)
	}
}

// rebuiltChecker compares the history queued for each turn with the
// conversation's log rebuilt at the moment the turn runs.
type rebuiltChecker struct {
	harness.Harness
	log  func() eventlog.EventLog
	conv string

	mu         sync.Mutex
	runs       int
	mismatches []string
}

func (c *rebuiltChecker) turns() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.runs
}

func (c *rebuiltChecker) Start(ctx context.Context, conv string, cfg []byte) (harness.Execution, error) {
	e, err := c.Harness.Start(ctx, conv, cfg)
	return &checkedExecution{Execution: e, c: c}, err
}

type checkedExecution struct {
	harness.Execution
	c      *rebuiltChecker
	queued []*proto.Step
}

func (e *checkedExecution) Queue(ctx context.Context, steps ...*proto.Step) error {
	e.queued = append(e.queued, steps...)
	return e.Execution.Queue(ctx, steps...)
}

func (e *checkedExecution) Run(ctx context.Context, h harness.Handler) error {
	events, err := e.c.log().Events(ctx, e.c.conv)
	if err != nil {
		return err
	}
	st, err := rebuild(events)
	if err != nil {
		return err
	}
	e.c.mu.Lock()
	e.c.runs++
	if !slices.EqualFunc(e.queued, st.History, sameStep) {
		e.c.mismatches = append(e.c.mismatches, fmt.Sprintf("turn %d queued %d steps, the log rebuilds to %d", e.c.runs, len(e.queued), len(st.History)))
	}
	e.c.mu.Unlock()
	e.queued = nil
	return e.Execution.Run(ctx, h)
}

// The replay snapshot: for every cut point k, the fold of the first k events,
// stored and read back as binary protobuf, followed by the rest, rebuilds to
// the state the whole log rebuilds to. The log is a real three-turn run.
func TestFoldRebuildsTheSameState(t *testing.T) {
	r := newLedgerRig(t, nil)
	for i := range 3 { // a different agent_config each turn: the last one wins
		if err := r.exec(fmt.Sprintf(`{"turns":[{"tool_calls":[{"name":"charge_card"}]},{"content":"done %d"}],"tools":{"charge_card":{"idempotent":true}}}`, i)); err != nil {
			t.Fatal(err)
		}
	}
	events, err := r.log.Events(context.Background(), r.conv)
	if err != nil {
		t.Fatal(err)
	}
	want, err := rebuild(events)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(want.AgentConfig, []byte("done 2")) {
		t.Fatalf("the log rebuilds to agent_config %s, want the last turn's", want.AgentConfig)
	}
	for k := 0; k <= len(events); k++ {
		data, err := gproto.Marshal(fold(r.conv, events[:k]))
		if err != nil {
			t.Fatal(err)
		}
		snap := &proto.StepEvent{}
		if err := gproto.Unmarshal(data, snap); err != nil {
			t.Fatal(err)
		}
		got, err := rebuild(append([]*proto.StepEvent{snap}, events[k:]...))
		if err != nil {
			t.Fatal(err)
		}
		if got.State != want.State || got.HarnessID != want.HarnessID || !bytes.Equal(got.AgentConfig, want.AgentConfig) ||
			!slices.EqualFunc(got.History, want.History, func(a, b *proto.Step) bool { return gproto.Equal(a, b) }) {
			t.Fatalf("snapshot after %d of %d events rebuilds to %v %q %d steps, the log to %v %q %d steps",
				k, len(events), got.State, got.HarnessID, len(got.History), want.State, want.HarnessID, len(want.History))
		}
	}
}

// Replay reads only the tail after the latest snapshot, however long the
// conversation grows, and every turn still runs on exactly the history the
// whole log rebuilds to.
func TestReplay_ReadsOnlyTheTailAfterASnapshot(t *testing.T) {
	var r *recRig
	rec := &rebuiltChecker{log: func() eventlog.EventLog { return r.log }}
	r = newRecRig(t, func(reg *Registry, effects *fake.MemoryEffects) {
		rec.Harness = fake.New(effects)
		if err := reg.Register("fake", rec, RecoveryFull); err != nil {
			t.Fatal(err)
		}
		reg.DeclareTools("fake", map[string]toolledger.Class{"charge_card": toolledger.Idempotent})
	})
	rec.conv = r.conv
	r.c.snapshotEvery = 5
	ctx := context.Background()
	maxRead := 0.0
	for i := range 15 {
		before := testutil.ToFloat64(replayEvents)
		if err := r.c.Exec(ctx, &proto.CreateInteractionEvent{
			ConversationId: r.conv, AgentId: "fake",
			AgentConfig: []byte(`{"turns":[{"tool_calls":[{"name":"charge_card","args":{"n":` + fmt.Sprint(i) + `}}]},{"content":"done"}],"tools":{"charge_card":{"idempotent":true}}}`),
			Inputs:      []*proto.Step{harnesstest.UserStep("go")},
		}, nil); err != nil {
			t.Fatal(err)
		}
		maxRead = max(maxRead, testutil.ToFloat64(replayEvents)-before)
	}
	events, err := r.log.Events(ctx, r.conv)
	if err != nil {
		t.Fatal(err)
	}
	step, snap, err := r.log.(eventlog.Snapshots).Snapshot(ctx, r.conv)
	if err != nil {
		t.Fatal(err)
	}
	// The snapshot is the fold of exactly the first step events.
	fromSnap, _ := rebuild([]*proto.StepEvent{snap})
	fromLog, _ := rebuild(events[:min(int(step), len(events))])
	if fromSnap.State != fromLog.State || !slices.EqualFunc(fromSnap.History, fromLog.History, sameStep) {
		t.Fatalf("snapshot labelled step %d rebuilds to %v with %d steps, the log's first %d events to %v with %d",
			step, fromSnap.State, len(fromSnap.History), step, fromLog.State, len(fromLog.History))
	}
	if maxRead > 10 || len(events) < 70 || step < int64(len(events))-10 {
		t.Fatalf("an Exec read up to %v events of %d, latest snapshot at step %d; want at most 10 and a recent snapshot", maxRead, len(events), step)
	}
	if rec.turns() == 0 || len(rec.mismatches) > 0 {
		t.Fatalf("%d turns, mismatches: %v", rec.turns(), rec.mismatches)
	}
}

// sameStep compares two steps but for Index, which the live path sets on a
// step after logging it, so a log read back from PostgreSQL never has it.
func sameStep(a, b *proto.Step) bool {
	a, b = gproto.Clone(a).(*proto.Step), gproto.Clone(b).(*proto.Step)
	a.Index, b.Index = 0, 0
	return gproto.Equal(a, b)
}
