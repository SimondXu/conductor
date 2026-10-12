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
	"errors"
	"io"
	"net"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	schema "github.com/simondxu/conductor/db"
	"github.com/simondxu/conductor/internal/controller"
	"github.com/simondxu/conductor/internal/controller/eventlog"
	"github.com/simondxu/conductor/internal/controller/lease"
	"github.com/simondxu/conductor/internal/controller/toolledger"
	"github.com/simondxu/conductor/internal/harness/fake"
	"github.com/simondxu/conductor/internal/harness/harnesstest"
	"github.com/simondxu/conductor/internal/server"
	"github.com/simondxu/conductor/proto"
)

// attachHarness is this test's own harness id, so no other test's
// reconciler adopts its runs in the shared database.
const attachHarness = "attach-test"

// P1 DoD (PRD.md §6.1, client-stream catch-up replay): a client loses its
// stream mid-run, and the run carries on without it. Re-attaching with the
// last step it saw, it is sent every logged step after that exactly once,
// in log order, catch-up then live, and the stream ends with the run.
func TestAttach_ReconnectGetsEveryMissedStepOnce(t *testing.T) {
	dsn := os.Getenv("PG_TEST_DSN")
	if dsn == "" {
		t.Skip("PG_TEST_DSN not set; skipping the attach test")
	}
	if _, err := schema.Up(dsn); err != nil {
		t.Fatal(err)
	}
	log, err := eventlog.OpenPostgresEventLog(dsn)
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := toolledger.OpenPostgres(dsn)
	if err != nil {
		t.Fatal(err)
	}
	leases, err := lease.OpenPostgres(dsn)
	if err != nil {
		t.Fatal(err)
	}
	reg := controller.NewRegistry()
	if err := reg.Register(attachHarness, fake.New(&fake.MemoryEffects{}), controller.RecoveryFull); err != nil {
		t.Fatal(err)
	}
	reg.DeclareTools(attachHarness, map[string]toolledger.Class{"charge_card": toolledger.Idempotent})
	c, err := controller.New(context.Background(), controller.Config{
		Registry:        reg,
		EventLogBuilder: func() (eventlog.EventLog, error) { return log, nil },
		Ledger:          ledger, Lease: leases, OwnerID: "attach-" + uuid.NewString(), LeaseTTL: 5 * time.Second,
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

	// Client A runs three slow tool turns and drops after the first call.
	conv := t.Name() + "-" + uuid.NewString()
	ctxA, dropA := context.WithCancel(context.Background())
	var streamA grpc.ServerStreamingClient[proto.CreateInteractionResponse]
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		streamA, err = proto.NewInteractionsServiceClient(conn).CreateInteraction(ctxA, &proto.CreateInteractionEvent{
			ConversationId: conv, AgentId: attachHarness,
			AgentConfig: []byte(`{"turns":[{"tool_calls":[{"name":"charge_card","args":{"n":1}}]},` +
				`{"tool_calls":[{"name":"charge_card","args":{"n":2}}]},{"tool_calls":[{"name":"charge_card","args":{"n":3}}]},` +
				`{"content":"done"}],"tools":{"charge_card":{"idempotent":true,"delay_ms":300}}}`),
			Inputs: []*proto.Step{harnesstest.UserStep("go")},
		})
		if err == nil || time.Now().After(deadline) {
			break
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	var lastA int64
	for lastA == 0 {
		resp, err := streamA.Recv()
		if err != nil {
			t.Fatal(err)
		}
		for _, s := range resp.Outputs {
			if s.GetToolCall() != nil {
				lastA = s.Index
			}
		}
	}
	dropA()

	// Client B re-attaches from the last step A saw.
	var got []int64
	ctxB, cancelB := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelB()
	stream, err := proto.NewConductorServiceClient(conn).Attach(ctxB, &proto.AttachRequest{ConversationId: conv, AfterStep: lastA})
	if err != nil {
		t.Fatal(err)
	}
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		for _, s := range resp.Outputs {
			got = append(got, s.Index)
		}
	}

	events, err := log.Events(context.Background(), conv)
	if err != nil {
		t.Fatal(err)
	}
	var want []int64
	for i, ev := range events {
		for range ev.Steps {
			if step := int64(i + 1); step > lastA {
				want = append(want, step)
			}
		}
	}
	if last := events[len(events)-1].State; last != proto.State_STATE_COMPLETED {
		t.Fatalf("run ended %v, want COMPLETED", last)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("after step %d B got steps %v, the log has %v", lastA, got, want)
	}
	if len(want) < 4 {
		t.Fatalf("only %d steps after the drop; the test did not drop mid-run", len(want))
	}
}
