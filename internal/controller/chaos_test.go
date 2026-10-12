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
	"fmt"
	"sync"
	"testing"

	"github.com/simondxu/conductor/internal/chaos"
	"github.com/simondxu/conductor/internal/controller/toolledger"
	"github.com/simondxu/conductor/internal/harness/fake"
	"github.com/simondxu/conductor/internal/harness/harnesstest"
	"github.com/simondxu/conductor/proto"
)

// Each crash point fires where its name says: what the ledger and the fake's
// effects show at the first hit. The exit is replaced, so the run goes on.
func TestChaosBoundaries(t *testing.T) {
	want := map[string]string{
		chaos.ModelStarted:   "no call, 0 effects",
		chaos.ModelCompleted: "no call, 0 effects",
		chaos.ToolPlanned:    "PLANNED, 0 effects",
		chaos.ToolStarted:    "STARTED, 0 effects",
		chaos.ToolEffectDone: "STARTED, 1 effects",
		chaos.ToolSettled:    "SUCCEEDED, 1 effects",
		chaos.EventAppended:  "no call, 0 effects", // the input event
		chaos.LeaseRenewed:   "",                   // timing-dependent; only that it fires
	}
	for _, boundary := range chaos.Boundaries {
		t.Run(boundary, func(t *testing.T) {
			r := newRecRig(t, func(reg *Registry, effects *fake.MemoryEffects) {
				if err := reg.Register("fake", fake.New(effects), RecoveryFull); err != nil {
					t.Fatal(err)
				}
				reg.DeclareTools("fake", map[string]toolledger.Class{"charge_card": toolledger.Idempotent})
			})
			ctx := context.Background()
			var once sync.Once
			got, code := "unfired", 0
			observe := func(exitCode int) {
				once.Do(func() {
					code = exitCode
					if want[boundary] == "" {
						got = ""
						return
					}
					calls, err := r.ledger.Calls(ctx, r.conv)
					if err != nil {
						got = err.Error()
						return
					}
					if len(calls) == 0 {
						got = "no call, 0 effects"
						return
					}
					n, _ := r.effects.Count(ctx, calls[0].CallID)
					got = fmt.Sprintf("%s, %d effects", calls[0].State, n)
				})
			}
			spec := boundary
			if boundary == chaos.ToolStarted {
				spec += ":charge_card"
			}
			if err := chaos.Arm(spec, observe); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = chaos.Arm("", nil) })
			// The tool outlasts a renewal of the rig's 2s lease.
			if err := r.c.Exec(ctx, &proto.CreateInteractionEvent{
				ConversationId: r.conv, AgentId: "fake",
				AgentConfig: []byte(`{"turns":[{"tool_calls":[{"name":"charge_card"}]},{"content":"done"}],"tools":{"charge_card":{"idempotent":true,"delay_ms":1000}}}`),
				Inputs:      []*proto.Step{harnesstest.UserStep("go")},
			}, nil); err != nil {
				t.Fatal(err)
			}
			_ = chaos.Arm("", nil) // the lease goroutine may still be renewing
			once.Do(func() {})
			if got != want[boundary] || code != chaos.ExitCode {
				t.Errorf("at %s: %q exit %d, want %q exit %d", boundary, got, code, want[boundary], chaos.ExitCode)
			}
		})
	}
}
