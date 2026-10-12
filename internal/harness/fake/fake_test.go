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

package fake

import (
	"context"
	"strings"
	"testing"

	gproto "google.golang.org/protobuf/proto"

	"github.com/simondxu/conductor/internal/harness"
	"github.com/simondxu/conductor/internal/harness/harnesstest"
	"github.com/simondxu/conductor/proto"
)

// turn runs one owned-loop turn over history and returns the emitted steps.
func turn(t *testing.T, h *Harness, cfg string, history []*proto.Step) ([]*proto.Step, error) {
	t.Helper()
	exec, err := h.Start(context.Background(), "conv", []byte(cfg))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := exec.Queue(context.Background(), history...); err != nil {
		t.Fatalf("Queue: %v", err)
	}
	mh := &harnesstest.MockHandler{}
	err = exec.Run(context.Background(), mh)
	if err == nil && !mh.IsDone() {
		t.Fatal("Run returned without OnComplete")
	}
	return mh.Collected(), err
}

// play drives turns until the history needs none, as the controller does.
func play(t *testing.T, h *Harness, cfg string, history []*proto.Step) []*proto.Step {
	t.Helper()
	for i := 0; harness.NeedsTurn(history); i++ {
		if i == 20 {
			t.Fatal("no completion after 20 turns")
		}
		out, err := turn(t, h, cfg, history)
		if err != nil {
			t.Fatalf("turn %d: %v", i, err)
		}
		history = append(history, out...)
	}
	return history
}

func kinds(steps []*proto.Step) string {
	var out []string
	for _, s := range steps {
		switch {
		case s.GetContent() != nil:
			out = append(out, s.GetContent().GetRole())
		case s.GetThought() != nil:
			out = append(out, "thought")
		case s.GetToolCall() != nil:
			out = append(out, "call:"+s.GetToolCall().GetFunctionCall().GetName())
		case s.GetToolResult() != nil:
			out = append(out, "result:"+s.GetToolResult().GetFunctionResult().GetName())
		}
	}
	return strings.Join(out, " ")
}

func TestDefaultScriptPlaysToCompletion(t *testing.T) {
	effects := &MemoryEffects{}
	got := play(t, New(effects), "", []*proto.Step{harnesstest.UserStep("go")})

	want := "user thought call:charge_card result:charge_card call:send_email result:send_email assistant"
	if k := kinds(got); k != want {
		t.Fatalf("steps = %q, want %q", k, want)
	}
	if len(effects.Effects) != 2 || effects.Effects[0].ToolName != "charge_card" || effects.Effects[1].ToolName != "send_email" {
		t.Fatalf("effects = %+v", effects.Effects)
	}
	if got[2].GetToolCall().GetId() != got[3].GetToolResult().GetCallId() {
		t.Error("result call_id does not match the call")
	}
}

func TestReplayedTurnIsIdentical(t *testing.T) {
	h := New(&MemoryEffects{})
	history := []*proto.Step{harnesstest.UserStep("go")}
	a, err := turn(t, h, "", history)
	if err != nil {
		t.Fatal(err)
	}
	b, err := turn(t, h, "", history)
	if err != nil {
		t.Fatal(err)
	}
	if len(a) != len(b) {
		t.Fatalf("len %d != %d", len(a), len(b))
	}
	for i := range a {
		if !gproto.Equal(a[i], b[i]) {
			t.Errorf("step %d differs: %v vs %v", i, a[i], b[i])
		}
	}
}

func TestCallIDsDifferAcrossConversations(t *testing.T) {
	h := New(&MemoryEffects{})
	ids := map[string]bool{}
	for _, conv := range []string{"a", "b"} {
		exec, err := h.Start(context.Background(), conv, nil)
		if err != nil {
			t.Fatal(err)
		}
		_ = exec.Queue(context.Background(), harnesstest.UserStep("go"))
		mh := &harnesstest.MockHandler{}
		if err := exec.Run(context.Background(), mh); err != nil {
			t.Fatal(err)
		}
		for _, s := range mh.Collected() {
			if c := s.GetToolCall(); c != nil {
				if ids[c.GetId()] {
					t.Fatalf("call id %q reused across conversations", c.GetId())
				}
				ids[c.GetId()] = true
			}
		}
	}
}

func TestScriptRestartsOnNewUserInput(t *testing.T) {
	cfg := `{"turns":[{"content":"one"}]}`
	h := New(&MemoryEffects{})
	history := play(t, h, cfg, []*proto.Step{harnesstest.UserStep("a")})
	history = play(t, h, cfg, append(history, harnesstest.UserStep("b")))
	if k := kinds(history); k != "user assistant user assistant" {
		t.Fatalf("steps = %q", k)
	}
}

const toolCfg = `{"turns":[{"tool_calls":[{"name":"t"}]},{"content":"done"}],"tools":{"t":%s}}`

func TestCrashAfterEffectExitsOnlyOnFirstEffect(t *testing.T) {
	effects := &MemoryEffects{}
	h := New(effects)
	exits := 0
	h.exit = func(code int) {
		if code != CrashExitCode {
			t.Errorf("exit code %d", code)
		}
		exits++
	}
	cfg := strings.Replace(toolCfg, "%s", `{"crash_after_effect":true}`, 1)
	history := []*proto.Step{harnesstest.UserStep("go")}
	calls, err := turn(t, h, cfg, history)
	if err != nil {
		t.Fatal(err)
	}
	history = append(history, calls...)

	// The stubbed exit returns, so each tool turn runs to the end; a real one
	// would have died after the first effect.
	for range 2 {
		if _, err := turn(t, h, cfg, history); err != nil {
			t.Fatal(err)
		}
	}
	if exits != 1 {
		t.Errorf("exits = %d, want 1", exits)
	}
	if len(effects.Effects) != 2 {
		t.Errorf("effects = %d, want 2 (one per attempt)", len(effects.Effects))
	}
}

func TestIdempotentToolPerformsOneEffectPerKey(t *testing.T) {
	effects := &MemoryEffects{}
	h := New(effects)
	cfg := strings.Replace(toolCfg, "%s", `{"idempotent":true}`, 1)
	history := []*proto.Step{harnesstest.UserStep("go")}
	calls, err := turn(t, h, cfg, history)
	if err != nil {
		t.Fatal(err)
	}
	history = append(history, calls...)
	for range 2 {
		if _, err := turn(t, h, cfg, history); err != nil {
			t.Fatal(err)
		}
	}
	if len(effects.Effects) != 1 {
		t.Errorf("effects = %d, want 1 for a repeated key", len(effects.Effects))
	}
}

func TestDuplicateResultReportsTwice(t *testing.T) {
	cfg := strings.Replace(toolCfg, "%s", `{"duplicate_result":true}`, 1)
	got := play(t, New(&MemoryEffects{}), cfg, []*proto.Step{harnesstest.UserStep("go")})
	if k := kinds(got); k != "user call:t result:t result:t assistant" {
		t.Fatalf("steps = %q", k)
	}
}

func TestToolFailOnceFailsBeforeTheEffect(t *testing.T) {
	effects := &MemoryEffects{}
	h := New(effects)
	cfg := strings.Replace(toolCfg, "%s", `{"fail_once":true}`, 1)
	history := []*proto.Step{harnesstest.UserStep("go")}
	calls, err := turn(t, h, cfg, history)
	if err != nil {
		t.Fatal(err)
	}
	history = append(history, calls...)

	if _, err := turn(t, h, cfg, history); err == nil {
		t.Fatal("first tool turn: want error")
	}
	if len(effects.Effects) != 0 {
		t.Fatalf("effect recorded before the failure: %+v", effects.Effects)
	}
	if _, err := turn(t, h, cfg, history); err != nil {
		t.Fatalf("second tool turn: %v", err)
	}
	if len(effects.Effects) != 1 {
		t.Errorf("effects = %d, want 1", len(effects.Effects))
	}
}

func TestModelFailOnce(t *testing.T) {
	h := New(&MemoryEffects{})
	cfg := `{"turns":[{"content":"x","fail_once":true}]}`
	history := []*proto.Step{harnesstest.UserStep("go")}
	if _, err := turn(t, h, cfg, history); err == nil {
		t.Fatal("first attempt: want error")
	}
	if _, err := turn(t, h, cfg, history); err != nil {
		t.Fatalf("second attempt: %v", err)
	}
}

func TestStartRejectsBadScript(t *testing.T) {
	if _, err := New(&MemoryEffects{}).Start(context.Background(), "c", []byte(`{"turnz":[]}`)); err == nil {
		t.Fatal("want error for unknown field")
	}
}

func TestScriptExhausted(t *testing.T) {
	h := New(&MemoryEffects{})
	cfg := `{"turns":[{"tool_calls":[{"name":"t"}]}]}`
	history := []*proto.Step{harnesstest.UserStep("go")}
	calls, _ := turn(t, h, cfg, history)
	history = append(history, calls...)
	results, _ := turn(t, h, cfg, history)
	history = append(history, results...)
	if _, err := turn(t, h, cfg, history); err == nil || !strings.Contains(err.Error(), "script has 1 turns") {
		t.Fatalf("err = %v", err)
	}
}
