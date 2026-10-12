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

// Package server implements the gRPC server for AXService,
// exposing execution management and agent registration APIs.

package server

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/simondxu/conductor/internal/controller"
	"github.com/simondxu/conductor/internal/controller/lease"
	"github.com/simondxu/conductor/proto"
)

// Server implements the AXService gRPC service.
type Server struct {
	proto.UnimplementedInteractionsServiceServer
	proto.UnimplementedConductorServiceServer

	controller *controller.Controller
	grpcServer *grpc.Server
	inFlight   map[string]struct{}
	inFlightMu sync.Mutex
}

// New creates a new controller server.
func New(c *controller.Controller) *Server {
	return &Server{
		controller: c,
		inFlight:   make(map[string]struct{}),
	}
}

// CreateInteraction executes a new agentic task with streaming responses.
func (s *Server) CreateInteraction(req *proto.CreateInteractionEvent, stream grpc.ServerStreamingServer[proto.CreateInteractionResponse]) error {
	ctx := stream.Context()
	slog.InfoContext(ctx, "Executing request",
		slog.String("request", req.String()),
	)

	inFlight, cleanup := s.markInFlight(req.ConversationId)
	if inFlight {
		return status.Errorf(codes.FailedPrecondition, "conversation %q is already in flight", req.ConversationId)
	}
	defer cleanup()

	outputHandler := controller.ExecHandler(func(resp *proto.CreateInteractionResponse) error {
		return stream.Send(resp)
	})
	err := s.controller.Exec(ctx, req, outputHandler)
	if errors.Is(err, lease.ErrHeld) {
		// Another replica or the reconciler is driving it (PRD.md §6.1).
		return status.Errorf(codes.FailedPrecondition, "conversation %q is already in flight: %v", req.ConversationId, err)
	}
	return err
}

// Attach re-attaches a client to a conversation: the steps it missed, then
// the live ones until the run ends (PRD.md §6.1). It is not in-flight
// guarded, since it only reads the log, so a client that reconnects while
// its previous turn is still unwinding is not refused. If the run is PENDING
// it is also resumed once here, in case nobody is driving it: on SQLite
// there is no reconciler, and on PostgreSQL this saves waiting for one.
func (s *Server) Attach(req *proto.AttachRequest, stream grpc.ServerStreamingServer[proto.CreateInteractionResponse]) error {
	ctx := stream.Context()
	var driving sync.WaitGroup
	defer driving.Wait()
	// Resume at most once ourselves. While someone else drives the run (in
	// flight here, or its lease held elsewhere) we may try again later; once
	// our own resume has run, success or not, retrying is the reconciler's
	// job, under its poison guard.
	var mu sync.Mutex
	busy, done := false, false
	resume := func() {
		mu.Lock()
		defer mu.Unlock()
		if busy || done {
			return
		}
		inFlight, cleanup := s.markInFlight(req.ConversationId)
		if inFlight {
			return
		}
		busy = true
		driving.Go(func() {
			defer cleanup()
			err := s.controller.Exec(ctx, &proto.CreateInteractionEvent{ConversationId: req.ConversationId}, nil)
			if err != nil && !errors.Is(err, lease.ErrHeld) && ctx.Err() == nil {
				slog.WarnContext(ctx, "Resuming an attached run failed", slog.String("conversation_id", req.ConversationId), slog.Any("error", err))
			}
			mu.Lock()
			busy, done = false, !errors.Is(err, lease.ErrHeld)
			mu.Unlock()
		})
	}
	err := s.controller.Attach(ctx, req.ConversationId, req.AfterStep, func(resp *proto.CreateInteractionResponse) error {
		return stream.Send(resp)
	}, resume)
	if errors.Is(err, controller.ErrNoConversation) {
		return status.Error(codes.NotFound, err.Error())
	}
	return err
}

// Serve starts the gRPC server on the specified address.
func (s *Server) Serve(address string, opts ...grpc.ServerOption) error {
	lis, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	opts = append(opts,
		grpc.ChainUnaryInterceptor(LoggingInterceptor),
		grpc.ChainStreamInterceptor(StreamLoggingInterceptor),
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	)

	s.grpcServer = grpc.NewServer(opts...)
	proto.RegisterInteractionsServiceServer(s.grpcServer, s)
	proto.RegisterConductorServiceServer(s.grpcServer, s)

	// Register standard gRPC Health Check server.
	hs := health.NewServer()
	hs.SetServingStatus("conductor", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(s.grpcServer, hs)

	if err := s.grpcServer.Serve(lis); err != nil {
		return fmt.Errorf("failed to serve: %w", err)
	}
	return nil
}

// GracefulStop stops the gRPC server gracefully.
// It does not close the controller: runs still unwinding need its database
// connections to log and release their leases. The caller closes it.
func (s *Server) GracefulStop() {
	slog.Info("Stopping server gracefully...")
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
}

// Stop closes every connection and cancels the RPCs in flight.
func (s *Server) Stop() {
	if s.grpcServer != nil {
		s.grpcServer.Stop()
	}
}

func (s *Server) markInFlight(id string) (exists bool, cleanup func()) {
	s.inFlightMu.Lock()
	defer s.inFlightMu.Unlock()

	_, ok := s.inFlight[id]
	if ok {
		return true, func() {}
	}
	s.inFlight[id] = struct{}{}

	return false, func() {
		s.inFlightMu.Lock()
		delete(s.inFlight, id)
		s.inFlightMu.Unlock()
	}
}
