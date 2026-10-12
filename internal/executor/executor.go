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

// Package executor owns the lifecycle of the environment a harness runs in
// (docs/conductor/ARCHITECTURE.md §5.4): a subprocess or a Kubernetes pod per
// conversation, serving HarnessService. It does not speak the agent protocol;
// Harness dials what an Executor acquires with internal/harness/remote.
package executor

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/simondxu/conductor/internal/harness"
	"github.com/simondxu/conductor/internal/harness/remote"
)

// Spec is what an environment is acquired for.
type Spec struct {
	// Generation is the acquiring owner's lease generation. An environment
	// of a lower generation for the same conversation is removed first
	// (ARCHITECTURE.md §6.2).
	Generation int64
}

// Target is an acquired environment.
type Target struct {
	Addr       string // host:port serving HarnessService
	Kind       string // "local" | "pod"
	Generation int64
}

// Executor acquires and releases one environment per conversation.
type Executor interface {
	Acquire(ctx context.Context, conversationID string, spec Spec) (Target, error)
	// Release removes the conversation's environment of generation gen. A
	// newer generation's environment is left alone.
	Release(ctx context.Context, conversationID string, gen int64) error
}

var acquireDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "conductor_executor_acquire_duration_seconds",
	Help:    "Time to acquire an execution environment, by backend: the cold-start cost.",
	Buckets: prometheus.ExponentialBuckets(0.01, 2, 14), // 10ms .. ~82s
}, []string{"kind"})

type generationKey struct{}

// WithGeneration returns ctx carrying the caller's lease generation, which
// Harness passes to Acquire. harness.Harness.Start is frozen, so the
// generation travels in the context.
func WithGeneration(ctx context.Context, gen int64) context.Context {
	return context.WithValue(ctx, generationKey{}, gen)
}

func generation(ctx context.Context) int64 {
	gen, _ := ctx.Value(generationKey{}).(int64)
	return gen
}

// Harness is a harness.Harness whose every execution runs in its own
// environment: Start acquires one, and closing the execution releases it.
type Harness struct {
	Executor Executor
	// AgentID is sent to the environment's HarnessService.
	AgentID string
}

var _ harness.Harness = (*Harness)(nil)

// Start implements harness.Harness.
func (h *Harness) Start(ctx context.Context, conversationID string, config []byte) (harness.Execution, error) {
	gen := generation(ctx)
	start := time.Now()
	_, span := otel.Tracer("conductor.executor").Start(ctx, "executor.acquire",
		trace.WithAttributes(attribute.Int64("generation", gen)))
	t, err := h.Executor.Acquire(ctx, conversationID, Spec{Generation: gen})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		span.End()
		return nil, fmt.Errorf("executor: acquire %s: %w", conversationID, err)
	}
	span.SetAttributes(attribute.String("kind", t.Kind), attribute.String("addr", t.Addr))
	span.End()
	acquireDuration.WithLabelValues(t.Kind).Observe(time.Since(start).Seconds())
	release := func(ctx context.Context) error { return h.Executor.Release(ctx, conversationID, gen) }
	rh, err := remote.NewHarness(t.Addr, h.AgentID)
	if err != nil {
		_ = release(context.WithoutCancel(ctx))
		return nil, err
	}
	e, err := rh.Start(ctx, conversationID, config)
	if err != nil {
		rh.Close()
		_ = release(context.WithoutCancel(ctx))
		return nil, err
	}
	return &execution{Execution: e, conn: rh, release: release}, nil
}

type execution struct {
	harness.Execution
	conn    *remote.Harness
	release func(context.Context) error
}

// Close closes the execution and releases its environment, even when ctx is
// already canceled.
func (e *execution) Close(ctx context.Context) error {
	err := e.Execution.Close(ctx)
	e.conn.Close()
	if rerr := e.release(context.WithoutCancel(ctx)); err == nil {
		err = rerr
	}
	return err
}
