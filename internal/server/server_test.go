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

package server_test

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/simondxu/conductor/internal/controller"
	"github.com/simondxu/conductor/internal/controller/eventlog"
	"github.com/simondxu/conductor/internal/controller/eventlog/eventlogtest"
	"github.com/simondxu/conductor/internal/harness/fake"
	"github.com/simondxu/conductor/internal/harness/harnesstest"
	"github.com/simondxu/conductor/internal/server"
	"github.com/simondxu/conductor/proto"
)

// A client that goes away does not cancel its run (PRD.md §6.2): a slow
// UNSAFE call in flight when the client drops still finishes, and the run
// completes with its one effect.
func TestCreateInteraction_ClientDisconnectDoesNotCancelRun(t *testing.T) {
	log := &eventlogtest.MemoryEventLog{}
	effects := &fake.MemoryEffects{}
	reg := controller.NewRegistry()
	if err := reg.Register("fake", fake.New(effects), controller.RecoveryFull); err != nil {
		t.Fatal(err)
	}
	c, err := controller.New(context.Background(), controller.Config{
		Registry:        reg,
		EventLogBuilder: func() (eventlog.EventLog, error) { return log, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	lis.Close()
	srv := server.New(c)
	go srv.Serve(addr)
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })

	ctx, drop := context.WithCancel(context.Background())
	var stream grpc.ServerStreamingClient[proto.CreateInteractionResponse]
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		stream, err = proto.NewInteractionsServiceClient(conn).CreateInteraction(ctx, &proto.CreateInteractionEvent{
			ConversationId: "disconnect", AgentId: "fake",
			AgentConfig: []byte(`{"turns":[{"tool_calls":[{"name":"send_email"}]},{"content":"sent"}],` +
				`"tools":{"send_email":{"delay_ms":500}}}`),
			Inputs: []*proto.Step{harnesstest.UserStep("go")},
		})
		if err == nil || time.Now().After(deadline) {
			break
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	for called := false; !called; {
		resp, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}
		for _, s := range resp.Outputs {
			called = called || s.GetToolCall() != nil
		}
	}
	drop() // mid-call

	var state proto.State
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		events, _ := log.Events(context.Background(), "disconnect")
		if n := len(events); n > 0 && events[n-1].State != proto.State_STATE_PENDING {
			state = events[n-1].State
			break
		}
	}
	if state != proto.State_STATE_COMPLETED || len(effects.Effects) != 1 {
		t.Fatalf("after the client dropped: state %v, %d effect(s); want COMPLETED with 1", state, len(effects.Effects))
	}
}
