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

package bench

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/simondxu/conductor/internal/chaos"
)

// The failure classes (docs/conductor/ARCHITECTURE.md §8.1).
const (
	ControllerKillGraceful = "controller_kill_graceful"
	ControllerKillSIGKILL  = "controller_kill_sigkill"
	HarnessKill            = "harness_kill"
	DBUnavailableOnAppend  = "db_unavailable_on_append"
	DuplicateConcurrentReq = "duplicate_concurrent_req"
	RollingRestart         = "rolling_restart"
)

// Classes lists them in report order.
var Classes = []string{ControllerKillGraceful, ControllerKillSIGKILL, HarnessKill, DBUnavailableOnAppend, DuplicateConcurrentReq, RollingRestart}

// DefaultKillAt is where controller_kill_sigkill kills when no boundary is
// given: the IDEMPOTENT call has started and not run.
const DefaultKillAt = chaos.ToolStarted + ":charge_card"

// Scripts for the fake harness. charge_card is IDEMPOTENT and send_email
// UNSAFE (fake.DefaultScriptTools). The charge takes 4s: longer than the
// replicas' 2s drain, so a SIGTERM hands the run over instead of letting it
// finish, and longer than a renewal period of the compose stack's 5s lease,
// so lease_renewed fires inside it.
const (
	slowScript = `{"turns":[{"tool_calls":[{"name":"charge_card","args":{"amount":4200}}]},` +
		`{"tool_calls":[{"name":"send_email","args":{"to":"user@example.com"}}]},{"content":"done"}],` +
		`"tools":{"charge_card":{"idempotent":true,"delay_ms":4000}}}`
	crashScript = `{"turns":[{"tool_calls":[{"name":"charge_card","args":{"amount":4200}}]},{"content":"done"}],` +
		`"tools":{"charge_card":{"idempotent":true,"crash_after_effect":true}}}`
)

// dbPause outlasts the compose stack's 5s lease, so the owner loses it while
// its result append is blocked.
const dbPause = 6 * time.Second

// recoverTimeout bounds how long a run may take to end after an injection.
const recoverTimeout = 90 * time.Second

// Injection is one failure injected into one run.
type Injection struct {
	Conversation string    `json:"conversation"`
	Injected     time.Time `json:"injected"`
	// Ended is when the run was seen to leave PENDING; zero if it did not.
	Ended time.Time `json:"ended"`
	// State is the run's final state in the runs projection.
	State string `json:"state"`
	// Error says why the run did not recover, or the injection did not
	// happen as intended.
	Error string `json:"error,omitempty"`
}

// Recovered says whether the run reached a terminal state after the
// injection. FAILED counts: an UNSAFE call whose outcome is unknown fails
// the run by design.
func (in Injection) Recovered() bool { return in.Error == "" && !in.Ended.IsZero() }

// RecoveryTime is from the injection to the end of the run.
func (in Injection) RecoveryTime() time.Duration { return in.Ended.Sub(in.Injected) }

// Chaos injects class repeat times. Repeat n runs its conversations under
// the scope prefix-r<n>- and then calls each with the scope and its
// injections. at is the kill boundary of controller_kill_sigkill. It returns
// an error only when the stack cannot be driven; a run that does not recover
// is an Injection with an Error. It leaves every replica running and
// disarmed.
func (s *Stack) Chaos(ctx context.Context, class, at string, repeat int, prefix string, each func(scope string, ins []Injection)) error {
	if !slices.Contains(Classes, class) {
		return fmt.Errorf("unknown class %q", class)
	}
	if at == "" {
		at = DefaultKillAt
	}
	if boundary, _, err := chaos.Parse(at); err != nil {
		return err
	} else if boundary == chaos.ToolEffectDone {
		return fmt.Errorf("%s is a fake harness boundary; inject it with %s", boundary, HarnessKill)
	}
	services := make([]string, len(s.replicas))
	for i, r := range s.replicas {
		services[i] = r.Service
	}
	if err := s.up(ctx, "", services...); err != nil {
		return err
	}
	target := s.replicas[0].Service
	if class == ControllerKillSIGKILL {
		if err := s.up(ctx, at, target); err != nil {
			return err
		}
		defer s.up(context.WithoutCancel(ctx), "", target)
	}
	for n := 1; n <= repeat; n++ {
		scope := fmt.Sprintf("%s-r%d-", prefix, n)
		conv := scope + "a"
		var ins []Injection
		var err error
		switch class {
		case ControllerKillSIGKILL:
			ins, err = s.killAtBoundary(ctx, conv, at)
		case ControllerKillGraceful:
			ins, err = s.killGraceful(ctx, conv)
		case HarnessKill:
			ins, err = s.killHarness(ctx, conv)
		case DBUnavailableOnAppend:
			ins, err = s.pauseDB(ctx, conv)
		case DuplicateConcurrentReq:
			ins, err = s.duplicate(ctx, conv)
		case RollingRestart:
			ins, err = s.rollingRestart(ctx, scope)
		}
		if err != nil {
			return fmt.Errorf("%s %s: %w", class, conv, err)
		}
		each(scope, ins)
	}
	return nil
}

// sendAsync sends a turn and reports its result on the returned channel.
func (s *Stack) sendAsync(ctx context.Context, i int, conv, cfg string) <-chan error {
	done := make(chan error, 1)
	go func() { done <- s.Send(ctx, i, conv, cfg, "chaos") }()
	return done
}

// finish waits for in's run to end and records how.
func (s *Stack) finish(ctx context.Context, in Injection) Injection {
	st, at, err := s.waitTerminal(ctx, in.Conversation, recoverTimeout)
	in.State = st
	if err != nil {
		in.Error = err.Error()
		return in
	}
	in.Ended = at
	return in
}

// killAtBoundary: the armed target exits at the boundary, holding its lease;
// another replica takes the run over once the lease expires. The target is
// then started again, still armed.
func (s *Stack) killAtBoundary(ctx context.Context, conv, at string) ([]Injection, error) {
	target := s.replicas[0].Service
	s.sendAsync(ctx, 0, conv, slowScript)
	in := Injection{Conversation: conv}
	err := waitFor(ctx, 30*time.Second, target+" to die at "+at, func(ctx context.Context) (bool, error) {
		if st, _ := s.runState(ctx, conv); st != "" && st != "PENDING" {
			return false, fmt.Errorf("the run ended %s without reaching %s", st, at)
		}
		st, err := s.state(ctx, target)
		return st == "exited", err
	})
	if err != nil {
		in.Error = err.Error()
		return []Injection{s.finish(ctx, in)}, s.up(ctx, at, target)
	}
	in.Injected = time.Now()
	in = s.finish(ctx, in)
	return []Injection{in}, s.up(ctx, at, target)
}

// killGraceful SIGTERMs the owner while the charge runs. It drains for 2s,
// less than the charge takes, so it cancels the run and releases the lease,
// and another replica takes the run over.
func (s *Stack) killGraceful(ctx context.Context, conv string) ([]Injection, error) {
	target := s.replicas[0].Service
	s.sendAsync(ctx, 0, conv, slowScript)
	if err := s.waitCall(ctx, conv, "charge_card", "STARTED"); err != nil {
		return nil, err
	}
	in := Injection{Conversation: conv, Injected: time.Now()}
	if err := s.compose(ctx, nil, "kill", "-s", "SIGTERM", target); err != nil {
		return nil, err
	}
	in = s.finish(ctx, in)
	if err := s.waitService(ctx, target, "exited"); err != nil {
		return nil, err
	}
	return []Injection{in}, s.up(ctx, "", target)
}

// killHarness: the fake harness exits after charge_card's effect and before
// its result, and compose restarts it. The injection is when the client
// sees the stream break.
func (s *Stack) killHarness(ctx context.Context, conv string) ([]Injection, error) {
	err := <-s.sendAsync(ctx, 0, conv, crashScript)
	in := Injection{Conversation: conv, Injected: time.Now()}
	if err == nil {
		in.Error = "the harness did not crash"
	}
	return []Injection{s.finish(ctx, in)}, nil
}

// pauseDB freezes PostgreSQL while the charge runs, so its result append
// and every lease renewal block for dbPause.
func (s *Stack) pauseDB(ctx context.Context, conv string) ([]Injection, error) {
	s.sendAsync(ctx, 0, conv, slowScript)
	if err := s.waitCall(ctx, conv, "charge_card", "STARTED"); err != nil {
		return nil, err
	}
	in := Injection{Conversation: conv, Injected: time.Now()}
	if err := s.compose(ctx, nil, "pause", "postgres"); err != nil {
		return nil, err
	}
	time.Sleep(dbPause)
	if err := s.compose(context.WithoutCancel(ctx), nil, "unpause", "postgres"); err != nil {
		return nil, err
	}
	return []Injection{s.finish(ctx, in)}, nil
}

// duplicate sends the same conversation to two replicas at once. Exactly
// one may run it; the other must be refused.
func (s *Stack) duplicate(ctx context.Context, conv string) ([]Injection, error) {
	in := Injection{Conversation: conv, Injected: time.Now()}
	a, b := s.sendAsync(ctx, 0, conv, slowScript), s.sendAsync(ctx, 1, conv, slowScript)
	errA, errB := <-a, <-b
	in = s.finish(ctx, in)
	if (errA == nil) == (errB == nil) {
		in.Error = fmt.Sprintf("want exactly one client to run it, got errors %v and %v", errA, errB)
	}
	return []Injection{in}, nil
}

// rollingRestart replaces the replicas one at a time, each with a run of its
// own in flight: start a run on it, wait for the charge to start, restart it
// (drain, stop, start) and wait until it is healthy, then the next. Each run
// is watched from its restart on, so its end is seen when it happens.
func (s *Stack) rollingRestart(ctx context.Context, scope string) ([]Injection, error) {
	ins := make([]Injection, len(s.replicas))
	var wg sync.WaitGroup
	for i, r := range s.replicas {
		conv := fmt.Sprintf("%s%d", scope, i+1)
		s.sendAsync(ctx, i, conv, slowScript)
		if err := s.waitCall(ctx, conv, "charge_card", "STARTED"); err != nil {
			return nil, err
		}
		ins[i] = Injection{Conversation: conv, Injected: time.Now()}
		wg.Go(func() { ins[i] = s.finish(ctx, ins[i]) })
		if err := s.compose(ctx, nil, "restart", r.Service); err != nil {
			return nil, err
		}
		if err := s.up(ctx, "", r.Service); err != nil {
			return nil, err
		}
	}
	wg.Wait()
	return ins, nil
}
