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

// Package remote is the generic client for the gRPC HarnessService.
package remote

import (
	"context"
	"fmt"
	"io"

	"github.com/simondxu/conductor/internal/harness"
	"github.com/simondxu/conductor/proto"
)

// Turn drives one execution turn over HarnessService.Connect: it sends a single
// HarnessRequest{start} carrying config and inputs, half-closes the stream, and
// dispatches the responses to handler until the terminal HarnessEnd.
func Turn(ctx context.Context, client proto.HarnessServiceClient, conversationID, agentID string, config []byte, inputs []*proto.Step, execID string, handler harness.Handler) error {
	stream, err := client.Connect(ctx)
	if err != nil {
		return fmt.Errorf("failed to open harness service stream: %w", err)
	}

	start := &proto.HarnessRequest{
		ConversationId: conversationID,
		AgentId:        agentID,
		Type: &proto.HarnessRequest_Start{
			Start: &proto.HarnessStart{
				AgentConfig: config,
				Steps:       inputs,
			},
		},
	}
	// A server that fails before reading the start frame makes Send/CloseSend
	// report io.EOF; the real status is surfaced by drainStream's Recv below, so
	// only treat non-EOF errors as send failures.
	if err := stream.Send(start); err != nil && err != io.EOF {
		return fmt.Errorf("failed to send harness start: %w", err)
	}
	// Close send direction to trigger server processing.
	if err := stream.CloseSend(); err != nil && err != io.EOF {
		return fmt.Errorf("failed to close stream send direction: %w", err)
	}

	return drainStream(ctx, stream, execID, handler)
}

// drainStream reads from the harness gRPC stream until io.EOF, dispatching messages
// to the handler, and returns the final execution status.
func drainStream(ctx context.Context, stream proto.HarnessService_ConnectClient, execID string, handler harness.Handler) error {
	var endState proto.State
	var endErr error
	hasEnd := false

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("gRPC harness streaming failure: %w", err)
		}

		switch payload := resp.Type.(type) {
		case *proto.HarnessResponse_Outputs:
			for _, step := range payload.Outputs.Steps {
				if err := handler.OnMessage(ctx, execID, step); err != nil {
					return fmt.Errorf("failed to dispatch streamed output: %w", err)
				}
			}
		case *proto.HarnessResponse_End:
			hasEnd = true
			endState = payload.End.GetState()
			if endState == proto.State_STATE_FAILED {
				if errDetail := payload.End.GetError(); errDetail != nil {
					endErr = fmt.Errorf("harness failed: [%d] %s", errDetail.GetCode(), errDetail.GetDescription())
				} else {
					endErr = fmt.Errorf("harness failed with no error details")
				}
			}
		}
	}

	if !hasEnd {
		return fmt.Errorf("harness stream ended without HarnessEnd frame")
	}
	if endState == proto.State_STATE_FAILED {
		return endErr
	}
	return handler.OnComplete(ctx, execID)
}
