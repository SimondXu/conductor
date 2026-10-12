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

// Package chaos holds the named crash points of docs/conductor/ARCHITECTURE.md
// §8.1. A process started with CONDUCTOR_CHAOS_KILL_AT=<boundary>[:<tool>]
// exits at the first point that matches. It calls os.Exit rather than
// unwinding, so nothing deferred runs: no lease release, no final append, as
// with a SIGKILL.
package chaos

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strings"
	"sync/atomic"
)

// Env arms the hook.
const Env = "CONDUCTOR_CHAOS_KILL_AT"

// ExitCode is the status of a chaos kill, that of a SIGKILLed process.
const ExitCode = 137

// The boundaries, in the order a run passes them. Only the tool_* ones take
// a tool name.
const (
	ModelStarted   = "model_started"    // before a model turn runs
	ModelCompleted = "model_completed"  // a model turn returned, its calls logged but not planned
	ToolPlanned    = "tool_planned"     // a call is PLANNED in the ledger
	ToolStarted    = "tool_started"     // a call is STARTED, its tool turn not yet run
	ToolEffectDone = "tool_effect_done" // fake harness: first effect recorded, result not returned
	ToolSettled    = "tool_settled"     // a call is settled in the ledger
	EventAppended  = "event_appended"   // an event is in the conversation log
	LeaseRenewed   = "lease_renewed"    // a lease renewal succeeded
)

// Boundaries lists every boundary in run order.
var Boundaries = []string{ModelStarted, ModelCompleted, ToolPlanned, ToolStarted, ToolEffectDone, ToolSettled, EventAppended, LeaseRenewed}

// armed is read by every At, on any goroutine.
var armed atomic.Pointer[arming]

type arming struct {
	at, tool string
	exit     func(int)
}

func init() {
	if err := Arm(os.Getenv(Env), os.Exit); err != nil {
		panic(err)
	}
}

// Arm sets the hook to spec, <boundary>[:<tool>] or empty to disarm, and the
// function it exits with. The environment arms it at startup; tests call
// Arm directly.
func Arm(spec string, exitFunc func(int)) error {
	at, tool, _ := strings.Cut(spec, ":")
	if at != "" && !slices.Contains(Boundaries, at) {
		return fmt.Errorf("%s=%q: unknown boundary %q, want one of %s", Env, spec, at, strings.Join(Boundaries, ", "))
	}
	if tool != "" && !strings.HasPrefix(at, "tool_") {
		return fmt.Errorf("%s=%q: only the tool_* boundaries take a tool name", Env, spec)
	}
	armed.Store(&arming{at: at, tool: tool, exit: exitFunc})
	return nil
}

// At exits the process if the hook is armed for boundary and, when the
// arming names a tool, for tool.
func At(ctx context.Context, boundary, tool string) {
	a := armed.Load()
	if a.at != boundary || (a.tool != "" && a.tool != tool) {
		return
	}
	slog.ErrorContext(ctx, "chaos.kill", slog.String("boundary", boundary), slog.String("tool", tool))
	a.exit(ExitCode)
}
