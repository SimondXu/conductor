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

package remote_test

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"

	"github.com/simondxu/conductor/internal/controller"
	"github.com/simondxu/conductor/internal/controller/eventlog"
	"github.com/simondxu/conductor/internal/controller/eventlog/eventlogtest"
	"github.com/simondxu/conductor/internal/harness/fake"
	"github.com/simondxu/conductor/internal/harness/harnesstest"
	"github.com/simondxu/conductor/internal/harness/remote"
	"github.com/simondxu/conductor/proto"
)

// TestOwnedLoopOverHarnessService runs the fake script through the controller
// with the harness behind a real gRPC HarnessService.
func TestOwnedLoopOverHarnessService(t *testing.T) {
	effects := &fake.MemoryEffects{}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	proto.RegisterHarnessServiceServer(srv, remote.NewServer(fake.New(effects)))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	h, err := remote.NewHarness(lis.Addr().String(), "fake")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.Close() })

	reg := controller.NewRegistry()
	if err := reg.Register("fake", h, controller.RecoveryFull); err != nil {
		t.Fatal(err)
	}
	log := &eventlogtest.MemoryEventLog{}
	c, err := controller.New(context.Background(), controller.Config{
		Registry:        reg,
		EventLogBuilder: func() (eventlog.EventLog, error) { return log, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	var delivered int
	if err := c.Exec(context.Background(), &proto.CreateInteractionEvent{
		ConversationId: "remote-owned",
		AgentId:        "fake",
		Inputs:         []*proto.Step{harnesstest.UserStep("go")},
	}, func(r *proto.CreateInteractionResponse) error {
		delivered += len(r.Outputs)
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	events, _ := log.Events(context.Background(), "remote-owned")
	if got := events[len(events)-1].State; got != proto.State_STATE_COMPLETED {
		t.Errorf("final state = %v, want COMPLETED", got)
	}
	if delivered != 6 || len(effects.Effects) != 2 {
		t.Errorf("delivered = %d, effects = %d; want 6 and 2", delivered, len(effects.Effects))
	}
}
