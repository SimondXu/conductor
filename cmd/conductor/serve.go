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

package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/simondxu/conductor/cmd/conductor/internal/cliutil"
	"github.com/simondxu/conductor/internal/controller"
	"github.com/simondxu/conductor/internal/server"
	"github.com/simondxu/conductor/internal/telemetry"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
)

var (
	serveConfigFile  string
	serveMetricsAddr string
)

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Run controller as a gRPC server",
	Long: `Run the Conductor controller as a gRPC server.
Loads configuration from a YAML file (default: conductor.yaml; ax.yaml is still read when it is absent).`,
	RunE: runServe,
}

func init() {
	serveCmd.Flags().StringVar(&serveConfigFile, "config", defaultConfigFile, "Path to YAML configuration file")
	serveCmd.Flags().StringVar(&serveMetricsAddr, "metrics-addr", ":9090", "Address for the Prometheus /metrics endpoint; empty disables it")
}

func runServe(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	// Initialize structured logging (JSON to stdout)
	slog.SetDefault(slog.New(telemetry.LogHandler{Handler: slog.NewJSONHandler(os.Stdout, nil)}))

	cfg, err := newConfig(cmd, serveConfigFile)
	if err != nil {
		return err
	}

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	// Initialize OpenTelemetry SDK
	if cfg.Telemetry.OTLP.Enabled {
		var opts []otlptracegrpc.Option
		endpoint := cfg.Telemetry.OTLP.Endpoint
		if endpoint == "" {
			endpoint = defaultEndpoint
		}
		opts = append(opts, otlptracegrpc.WithEndpoint(endpoint))
		if gcpOpts, ok := gcpTelemetryOpts(endpoint); ok {
			opts = append(opts, gcpOpts...)
		} else {
			opts = append(opts, otlptracegrpc.WithInsecure())
		}

		shutdown, err := telemetry.SetTraceProvider(ctx, "conductor-server", opts...)
		if err != nil {
			return fmt.Errorf("failed to initialize telemetry: %w", err)
		}
		defer func() {
			if err := shutdown(context.Background()); err != nil {
				slog.Error("failed to shutdown telemetry", "error", err)
			}
		}()
	}

	c, err := cliutil.NewControllerFromConfig(ctx, cfg)
	if err != nil {
		return fmt.Errorf("error creating controller: %w", err)
	}
	defer c.Close()

	if serveMetricsAddr != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		metricsSrv := &http.Server{Addr: serveMetricsAddr, Handler: mux}
		go func() {
			if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				slog.ErrorContext(ctx, "metrics server exited", slog.Any("error", err))
			}
		}()
		defer metricsSrv.Close()
	}

	// Create server
	srv := server.New(c)

	// The reconciler finishes runs nobody is driving (ARCHITECTURE.md §6.1).
	// It needs PostgreSQL; on SQLite ownership is process-local and there is
	// nothing to recover from.
	recCtx, stopReconciler := context.WithCancel(ctx)
	recDone := make(chan struct{})
	if cfg.EventLog.PostgresConfig.DSN != "" {
		go func() {
			defer close(recDone)
			if err := c.RunReconciler(recCtx, controller.ReconcilerConfig{}); err != nil {
				slog.ErrorContext(ctx, "reconciler exited", slog.Any("error", err))
			}
		}()
	} else {
		close(recDone)
		slog.WarnContext(ctx, "SQLite event log: ownership is process-local and no reconciler runs; use PostgreSQL for recovery")
	}
	defer func() {
		stopReconciler()
		<-recDone
	}()

	// Setup signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		slog.InfoContext(ctx, "Received interrupt, shutting down...")
		srv.GracefulStop()
	}()
	slog.InfoContext(ctx, "Starting Conductor server", slog.String("address", cfg.Server.Address))
	if err := srv.Serve(cfg.Server.Address); err != nil {
		return fmt.Errorf("error serving: %w", err)
	}

	return nil
}
