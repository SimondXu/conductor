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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Ledger metrics (docs/conductor/ARCHITECTURE.md §7.2), counted from the
// transitions the database actually made.
var (
	toolCalls = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "conductor_tool_calls_total",
		Help: "Tool call transitions, by the state entered.",
	}, []string{"tool", "class", "state"})
	toolAttempts = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "conductor_tool_attempts_total",
		Help: "Tool call starts, retries included.",
	}, []string{"tool", "class"})
	toolDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "conductor_tool_duration_seconds",
		Help:    "From STARTED to an observed result, by database clock.",
		Buckets: prometheus.ExponentialBuckets(0.005, 2, 14), // 5ms .. ~41s
	}, []string{"tool"})
	outcomeUnknown = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "conductor_tool_outcome_unknown_total",
		Help: "OUTCOME_UNKNOWN calls resolved, by resolution: retried, reconciled, abandoned or deadline_exceeded.",
	}, []string{"tool", "resolution"})
)

// How an OUTCOME_UNKNOWN call left that state.
const (
	resolutionRetried    = "retried"           // IDEMPOTENT, started again
	resolutionReconciled = "reconciled"        // settled from a result observed later
	resolutionAbandoned  = "abandoned"         // UNSAFE, or its run failed
	resolutionDeadline   = "deadline_exceeded" // reconcile_deadline passed
)

func observeTransition(prev, target State, tool string, class Class, secs float64) {
	toolCalls.WithLabelValues(tool, string(class), string(target)).Inc()
	switch {
	case target == Started:
		toolAttempts.WithLabelValues(tool, string(class)).Inc()
	case prev == Started && (target == Succeeded || target == Failed):
		toolDuration.WithLabelValues(tool).Observe(secs)
	}
	if prev != OutcomeUnknown {
		return
	}
	switch target {
	case Started:
		outcomeUnknown.WithLabelValues(tool, resolutionRetried).Inc()
	case Abandoned:
		outcomeUnknown.WithLabelValues(tool, resolutionAbandoned).Inc()
	case Succeeded, Failed:
		outcomeUnknown.WithLabelValues(tool, resolutionReconciled).Inc()
	}
}
