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

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/simondxu/conductor/internal/bench"
	"github.com/spf13/cobra"
)

var (
	benchCfg        bench.Config
	benchOut        string
	benchPrometheus string
)

var benchCmd = &cobra.Command{
	Use:   "bench",
	Short: "Load plus every failure class against the compose stack, with the report of PRD §8",
	Long: `Runs load against the compose stack (deploy/compose), then injects each
failure class, waits for every run to end and checks every invariant. Prints
the report and writes results.json to --out. Used by make bench.`,
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		s, err := openStack()
		if err != nil {
			return err
		}
		defer s.Close()
		benchCfg.Prometheus = benchPrometheus
		benchCfg.Progress = os.Stderr
		r, err := s.Bench(cmd.Context(), benchCfg)
		if err != nil {
			return err
		}
		r.Render(os.Stdout)
		out := benchOut
		if out == "" {
			out = filepath.Join("bench", "results", r.Started.UTC().Format("20060102T150405Z"))
		}
		if err := os.MkdirAll(out, 0o755); err != nil {
			return err
		}
		data, err := json.MarshalIndent(r, "", "  ")
		if err != nil {
			return err
		}
		path := filepath.Join(out, "results.json")
		if err := os.WriteFile(path, append(data, '\n'), 0o644); err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "wrote %s\n", path)
		if r.Result != "PASS" {
			return fmt.Errorf("bench %s", r.Result)
		}
		return nil
	},
}

func init() {
	registerStackFlags(benchCmd)
	f := benchCmd.Flags()
	f.IntVar(&benchCfg.Load.Concurrency, "concurrency", 8, "Load: sessions at a time")
	f.IntVar(&benchCfg.Load.Turns, "turns", 3, "Load: user turns per session")
	f.IntVar(&benchCfg.Load.ToolCalls, "tool-calls", 2, "Load: tool calls per turn")
	f.DurationVar(&benchCfg.Load.Duration, "duration", time.Minute, "Load: how long new sessions are started")
	f.StringSliceVar(&benchCfg.Classes, "classes", bench.Classes, "Failure classes to inject")
	f.IntVar(&benchCfg.Repeat, "repeat", 3, "Injections per class")
	f.StringVar(&benchOut, "out", "", "Directory for results.json (default bench/results/<timestamp>/)")
	f.StringVar(&benchPrometheus, "prometheus", "http://localhost:9090", "The stack's Prometheus, from the host")
	rootCmd.AddCommand(benchCmd)
}
