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

package executor_test

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/simondxu/conductor/internal/harness/harnesstest"
	"github.com/simondxu/conductor/internal/server"
	"github.com/simondxu/conductor/proto"
)

// M8 DoD: one trace spans client → server → controller → executor → model →
// tool → append. A client span calls a real gRPC server, whose controller
// runs a fake harness in a local executor subprocess against Postgres.
func TestTrace_OneTraceFromClientToAppend(t *testing.T) {
	rec := spanRecorder()

	c, _ := newPGController(t)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	lis.Close()
	srv := server.New(c)
	go srv.Serve(addr)
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	ctx, root := otel.Tracer("test").Start(context.Background(), "client")
	var stream grpc.ServerStreamingClient[proto.CreateInteractionResponse]
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		stream, err = proto.NewInteractionsServiceClient(conn).CreateInteraction(ctx, &proto.CreateInteractionEvent{
			ConversationId: t.Name() + "-" + uuid.NewString(), AgentId: testHarnessID,
			AgentConfig: []byte(`{"turns":[{"tool_calls":[{"name":"charge_card"}]},{"content":"done"}],"tools":{"charge_card":{"idempotent":true}}}`),
			Inputs:      []*proto.Step{harnesstest.UserStep("go")},
		})
		if err == nil || time.Now().After(deadline) {
			break
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := stream.Recv(); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatal(err)
		}
	}
	root.End()

	want := map[string]bool{"client": false, "exec": false, "lease.acquire": false, "replay": false,
		"executor.acquire": false, "harness.turn": false, "model.call": false, "tool.execute": false, "eventlog.append": false}
	server := false
	for _, s := range rec.Ended() {
		if s.SpanContext().TraceID() != root.SpanContext().TraceID() {
			continue
		}
		if _, ok := want[s.Name()]; ok {
			want[s.Name()] = true
		}
		if s.SpanKind() == trace.SpanKindServer {
			server = true
		}
	}
	for name, seen := range want {
		if !seen {
			t.Errorf("no %s span in the client's trace", name)
		}
	}
	if !server {
		t.Error("no gRPC server span in the client's trace")
	}
}

// spanRecorder installs one tracer provider for the whole test process.
// Package-level tracers (controller.tracer) bind to the first global
// provider and never follow a later SetTracerProvider, so a second run
// with a fresh provider would lose their spans. Runs share the recorder;
// the test tells its spans apart by trace id.
var spanRecorder = sync.OnceValue(func() *tracetest.SpanRecorder {
	rec := tracetest.NewSpanRecorder()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec)))
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return rec
})
