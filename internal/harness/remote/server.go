// Copyright 2026 Google LLC
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

package remote

import (
	"context"
	"fmt"
	"sync"

	"github.com/simondxu/conductor/internal/harness"
	"github.com/simondxu/conductor/proto"
)

// server adapts a Go harness.Harness onto the gRPC HarnessService.
type server struct {
	proto.UnimplementedHarnessServiceServer
	h harness.Harness
}

// NewServer returns a HarnessService that drives h: each Connect is one
// Start / Queue / Run / Close of an Execution.
func NewServer(h harness.Harness) proto.HarnessServiceServer {
	return &server{h: h}
}

// Connect drives one harness execution. Per the HarnessService contract, the
// client sends HarnessRequest{start} (and may send one HarnessRequest{cancel}
// mid-stream); the server streams zero or more HarnessResponse{outputs} frames
// terminated by exactly one HarnessResponse{end}.
func (s *server) Connect(stream proto.HarnessService_ConnectServer) error {
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	// The first frame must be a start.
	req, err := stream.Recv()
	if err != nil {
		return err
	}
	start := req.GetStart()
	if start == nil {
		return fmt.Errorf("first HarnessRequest must be a start frame")
	}
	convID := req.GetConversationId()

	exec, err := s.h.Start(ctx, convID, start.GetAgentConfig())
	if err != nil {
		return sendEnd(stream, convID, proto.State_STATE_FAILED, err)
	}
	defer func() { _ = exec.Close(context.WithoutCancel(ctx)) }()

	if len(start.GetSteps()) > 0 {
		if err := exec.Queue(ctx, start.GetSteps()...); err != nil {
			return sendEnd(stream, convID, proto.State_STATE_FAILED, err)
		}
	}

	// Watch for a mid-stream cancel frame; it cancels the run's context.
	//
	// Note: mid-run steering (injecting extra human input while a turn is running)
	// is intentionally NOT supported here. The execution can accept steering via
	// Queue, but the HarnessRequest oneof only defines {start, cancel} -- there is
	// no wire frame to carry additional input after start -- so a client cannot
	// deliver steering over Connect. Multi-turn conversation is instead expressed
	// as separate Connect executions that resume via the persisted cursor. Any
	// non-cancel frame received here is therefore ignored.
	go func() {
		for {
			r, err := stream.Recv()
			if err != nil {
				return // stream closed or errored; run ends by other means
			}
			if r.GetCancel() != nil {
				cancel()
				return
			}
		}
	}()

	// Run the turn, forwarding each agent message as an outputs frame.
	handler := &streamHandler{stream: stream, convID: convID}
	if err := exec.Run(ctx, handler); err != nil {
		if ctx.Err() != nil {
			return sendEnd(stream, convID, proto.State_STATE_CANCELED, ctx.Err())
		}
		return sendEnd(stream, convID, proto.State_STATE_FAILED, err)
	}
	if serr := handler.err(); serr != nil {
		return serr // failed while sending an outputs frame; stream is broken
	}
	return sendEnd(stream, convID, proto.State_STATE_COMPLETED, nil)
}

// streamHandler forwards each message the harness produces during a turn as a
// HarnessResponse{outputs} frame.
type streamHandler struct {
	stream proto.HarnessService_ConnectServer
	convID string

	mu      sync.Mutex
	sendErr error
}

var _ harness.Handler = (*streamHandler)(nil)

func (h *streamHandler) OnMessage(_ context.Context, _ string, step *proto.Step) error {
	err := h.stream.Send(&proto.HarnessResponse{
		ConversationId: h.convID,
		Type: &proto.HarnessResponse_Outputs{
			Outputs: &proto.HarnessOutputs{Steps: []*proto.Step{step}},
		},
	})
	if err != nil {
		h.mu.Lock()
		h.sendErr = err
		h.mu.Unlock()
	}
	return err
}

func (h *streamHandler) OnComplete(_ context.Context, _ string) error { return nil }

func (h *streamHandler) err() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.sendErr
}

// sendEnd sends the single terminal HarnessResponse{end} frame. If err is
// non-nil, it is attached as the terminal Error.
func sendEnd(stream proto.HarnessService_ConnectServer, convID string, state proto.State, err error) error {
	end := &proto.HarnessEnd{State: state}
	if err != nil {
		end.Error = &proto.Error{Description: err.Error()}
	}
	return stream.Send(&proto.HarnessResponse{
		ConversationId: convID,
		Type:           &proto.HarnessResponse_End{End: end},
	})
}
