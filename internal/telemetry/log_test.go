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

package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"go.opentelemetry.io/otel/trace"
)

func TestLogHandlerAddsContextAttrsAndTraceID(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(LogHandler{slog.NewJSONHandler(&buf, nil)})
	tid, _ := trace.TraceIDFromHex("0102030405060708090a0b0c0d0e0f10")
	sid, _ := trace.SpanIDFromHex("0102030405060708")
	ctx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: sid}))
	ctx = WithLogAttrs(ctx, slog.String("conversation_id", "c1"))
	child := WithLogAttrs(ctx, slog.Int64("generation", 3))
	_ = WithLogAttrs(ctx, slog.String("sibling", "x")) // must not leak into child

	log.InfoContext(child, "hello", slog.Int64("step", 7))
	var got map[string]any
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"conversation_id": "c1", "generation": 3.0, "step": 7.0, "trace_id": tid.String()}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %v, want %v", k, got[k], v)
		}
	}
	if _, ok := got["sibling"]; ok {
		t.Error("a sibling context's attr leaked")
	}
}
