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
	"strings"
	"sync"
	"time"
)

// LoadConfig shapes the load: Concurrency sessions at a time, each a new
// conversation of Turns user turns, each turn one model turn with ToolCalls
// IDEMPOTENT calls, their tool turn and a final answer.
type LoadConfig struct {
	Concurrency, Turns, ToolCalls int
	// Duration is how long new sessions are started; the last ones finish.
	Duration time.Duration
	// Prefix scopes the conversations.
	Prefix string
}

// LoadResult is what the load observed from the clients.
type LoadResult struct {
	Started   int `json:"sessions_started"`
	Completed int `json:"sessions_completed"`
	Failed    int `json:"sessions_failed"`
	// MaxConcurrent is the most sessions in flight at once.
	MaxConcurrent int           `json:"max_concurrent_sessions"`
	Elapsed       time.Duration `json:"elapsed_ns"`
	// TurnLatencies and RunLatencies are per completed turn and session.
	TurnLatencies []time.Duration `json:"-"`
	RunLatencies  []time.Duration `json:"-"`
	// Errors are the first few session failures.
	Errors []string `json:"errors,omitempty"`
}

// loadScript is the fake's script for one user turn of the load.
func loadScript(toolCalls int) string {
	calls := make([]string, toolCalls)
	for i := range calls {
		calls[i] = fmt.Sprintf(`{"name":"charge_card","args":{"amount":%d}}`, i+1)
	}
	return `{"turns":[{"tool_calls":[` + strings.Join(calls, ",") + `]},{"content":"done"}],` +
		`"tools":{"charge_card":{"idempotent":true}}}`
}

// Load runs sessions over the replicas round-robin for cfg.Duration.
func (s *Stack) Load(ctx context.Context, cfg LoadConfig) LoadResult {
	script := loadScript(cfg.ToolCalls)
	var mu sync.Mutex
	var r LoadResult
	inFlight := 0
	start := time.Now()
	stop := start.Add(cfg.Duration)
	var wg sync.WaitGroup
	for w := range cfg.Concurrency {
		wg.Go(func() {
			for n := 0; time.Now().Before(stop) && ctx.Err() == nil; n++ {
				conv := fmt.Sprintf("%s-w%d-s%d", cfg.Prefix, w, n)
				replica := (w + n) % len(s.replicas)
				mu.Lock()
				r.Started++
				inFlight++
				r.MaxConcurrent = max(r.MaxConcurrent, inFlight)
				mu.Unlock()

				sessionStart := time.Now()
				var turns []time.Duration
				var err error
				for t := range cfg.Turns {
					turnStart := time.Now()
					if err = s.Send(ctx, replica, conv, script, fmt.Sprintf("turn %d", t+1)); err != nil {
						break
					}
					turns = append(turns, time.Since(turnStart))
				}

				mu.Lock()
				inFlight--
				r.TurnLatencies = append(r.TurnLatencies, turns...)
				if err != nil {
					r.Failed++
					if len(r.Errors) < 5 {
						r.Errors = append(r.Errors, conv+": "+err.Error())
					}
				} else {
					r.Completed++
					r.RunLatencies = append(r.RunLatencies, time.Since(sessionStart))
				}
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	r.Elapsed = time.Since(start)
	return r
}
