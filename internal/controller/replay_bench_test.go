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
	"math"
	"os"
	"testing"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/structpb"

	schema "github.com/simondxu/conductor/db"
	"github.com/simondxu/conductor/internal/controller/eventlog"
	"github.com/simondxu/conductor/proto"
)

// BenchmarkReplay replays a conversation of n events on PostgreSQL: the
// whole log, and the latest snapshot plus a 10-event tail.
//
//	PG_TEST_DSN=... go test -run '^$' -bench BenchmarkReplay ./internal/controller/
func BenchmarkReplay(b *testing.B) {
	dsn := os.Getenv("PG_TEST_DSN")
	if dsn == "" {
		b.Skip("PG_TEST_DSN not set")
	}
	if _, err := schema.Up(dsn); err != nil {
		b.Fatal(err)
	}
	log, err := eventlog.OpenPostgresEventLog(dsn)
	if err != nil {
		b.Fatal(err)
	}
	defer log.Close()
	ctx := context.Background()
	for _, n := range []int{200, 2000} {
		conv := fmt.Sprintf("bench-replay-%d-%s", n, uuid.NewString())
		for i := range n {
			args, _ := structpb.NewStruct(map[string]any{"amount": i, "note": "a realistic argument payload of some length"})
			step := &proto.Step{Type: &proto.Step_ToolCall{ToolCall: &proto.ToolCallStep{
				Id:   uuid.NewString(),
				Type: &proto.ToolCallStep_FunctionCall{FunctionCall: &proto.FunctionCallStep{Name: "charge_card", Arguments: args}},
			}}}
			if _, err := log.Append(ctx, &proto.StepEvent{ConversationId: conv, EventId: uuid.NewString(), AgentId: "fake",
				State: proto.State_STATE_PENDING, Steps: []*proto.Step{step}}, 0); err != nil {
				b.Fatal(err)
			}
		}
		c := &Controller{eventLog: log, snapshotEvery: math.MaxInt}
		events, err := log.Events(ctx, conv)
		if err != nil {
			b.Fatal(err)
		}
		// The whole log, read as the snapshot-less conversation it is.
		b.Run(fmt.Sprintf("events=%d/whole-log", n), func(b *testing.B) {
			for b.Loop() {
				if _, read, err := c.replay(ctx, conv); err != nil || read != n {
					b.Fatalf("read %d, %v", read, err)
				}
			}
		})
		if err := log.(eventlog.Snapshots).SaveSnapshot(ctx, conv, int64(n-10), fold(conv, events[:n-10])); err != nil {
			b.Fatal(err)
		}
		b.Run(fmt.Sprintf("events=%d/snapshot+10", n), func(b *testing.B) {
			for b.Loop() {
				if _, read, err := c.replay(ctx, conv); err != nil || read != 10 {
					b.Fatalf("read %d, %v", read, err)
				}
			}
		})
	}
}
