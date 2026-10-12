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

package remote

import (
	"context"
	"fmt"
	"sync"

	"github.com/google/uuid"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/simondxu/conductor/internal/harness"
	"github.com/simondxu/conductor/proto"
)

// Harness is a harness.Harness served by a HarnessService at a fixed address.
// Each Run is one Connect carrying everything queued since the last Run.
type Harness struct {
	agentID string
	conn    *grpc.ClientConn
	client  proto.HarnessServiceClient
}

var _ harness.Harness = (*Harness)(nil)

// NewHarness returns a Harness for the HarnessService at addr. agentID is sent
// on every request. The connection is established lazily.
func NewHarness(addr, agentID string) (*Harness, error) {
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()))
	if err != nil {
		return nil, fmt.Errorf("remote harness %s: %w", addr, err)
	}
	return &Harness{agentID: agentID, conn: conn, client: proto.NewHarnessServiceClient(conn)}, nil
}

// Close closes the connection.
func (h *Harness) Close() error { return h.conn.Close() }

// Start implements harness.Harness.
func (h *Harness) Start(ctx context.Context, conversationID string, config []byte) (harness.Execution, error) {
	return &execution{h: h, conversationID: conversationID, id: uuid.NewString(), config: config}, nil
}

type execution struct {
	h              *Harness
	conversationID string
	id             string
	config         []byte

	mu     sync.Mutex
	queued []*proto.Step
}

func (e *execution) ID() string                      { return e.id }
func (e *execution) Close(ctx context.Context) error { return nil }

func (e *execution) Queue(ctx context.Context, steps ...*proto.Step) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.queued = append(e.queued, steps...)
	return nil
}

func (e *execution) Run(ctx context.Context, handler harness.Handler) error {
	e.mu.Lock()
	inputs := e.queued
	e.queued = nil
	e.mu.Unlock()
	return Turn(ctx, e.h.client, e.conversationID, e.h.agentID, e.config, inputs, e.id, handler)
}
