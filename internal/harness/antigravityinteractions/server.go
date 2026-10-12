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

package antigravityinteractions

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/simondxu/conductor/internal/harness/remote"
	"github.com/simondxu/conductor/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// Serve builds the Antigravity Interactions harness from cfg and serves the
// HarnessService (plus gRPC health) on host:port, and an HTTP /readyz endpoint on
// readyzPort that reflects this server's own serving state. It blocks until ctx
// is cancelled or a termination signal (SIGINT/SIGTERM) is received, then shuts
// down gracefully.
func Serve(ctx context.Context, cfg AntigravityInteractionsConfig, host string, port, readyzPort int) error {
	h, err := New(cfg)
	if err != nil {
		return fmt.Errorf("creating antigravity interactions harness: %w", err)
	}

	lis, err := net.Listen("tcp", net.JoinHostPort(host, strconv.Itoa(port)))
	if err != nil {
		return fmt.Errorf("listen on %s:%d: %w", host, port, err)
	}

	grpcServer := grpc.NewServer()
	proto.RegisterHarnessServiceServer(grpcServer, remote.NewServer(h))

	// gRPC health: report SERVING for the default service so the controller's
	// health checks pass.
	hs := health.NewServer()
	hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(grpcServer, hs)

	// Go-based readiness: /readyz is OK once this server is up and serving.
	readyzSrv := serveReadyz(host, readyzPort)

	// Shut down on ctx cancellation or termination signals.
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		log.Printf("antigravity interactions harness: shutting down")
		if readyzSrv != nil {
			_ = readyzSrv.Close()
		}
		grpcServer.GracefulStop()
	}()

	log.Printf("antigravity interactions harness serving on %s:%d (readyz :%d)", host, port, readyzPort)
	if err := grpcServer.Serve(lis); err != nil {
		return fmt.Errorf("harness gRPC server: %w", err)
	}
	return nil
}

// serveReadyz starts an HTTP server exposing /readyz that returns 200 as long as
// this process's gRPC server is running. Readiness is intrinsic to this server.
func serveReadyz(host string, readyzPort int) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	srv := &http.Server{
		Addr:    net.JoinHostPort(host, strconv.Itoa(readyzPort)),
		Handler: mux,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("readyz server on %s exited: %v", srv.Addr, err)
		}
	}()
	return srv
}
