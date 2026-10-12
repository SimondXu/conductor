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
	"fmt"
	"strings"
	"time"

	"github.com/simondxu/conductor/internal/bench"
	"github.com/simondxu/conductor/internal/chaos"
	"github.com/simondxu/conductor/internal/invariant"
	"github.com/spf13/cobra"
)

var (
	stackComposeFile string
	stackDSN         string
	stackServers     []string

	chaosClass  string
	chaosAt     string
	chaosRepeat int
)

var chaosCmd = &cobra.Command{
	Use:   "chaos",
	Short: "Inject failures into the compose stack and check every invariant",
}

var chaosRunCmd = &cobra.Command{
	Use:   "run",
	Short: "Inject one failure class into new runs, then check each run's invariants",
	Long: `Drives the compose stack (deploy/compose) from the host: it controls
containers with docker compose, sends runs to each replica over gRPC and checks
the invariants of docs/conductor/correctness.md in PostgreSQL after each run.`,
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		s, err := openStack()
		if err != nil {
			return err
		}
		defer s.Close()
		ctx := cmd.Context()
		at := ""
		if chaosClass == bench.ControllerKillSIGKILL {
			at = chaosAt
			if at == "" {
				at = bench.DefaultKillAt
			}
		}
		fmt.Printf("class=%s", chaosClass)
		if at != "" {
			fmt.Printf(" at=%s", at)
		}
		fmt.Printf(" repeat=%d\n", chaosRepeat)
		prefix := fmt.Sprintf("chaos-%s-%d", chaosClass, time.Now().Unix())
		n, recovered, violations := 0, 0, 0
		err = s.Chaos(ctx, chaosClass, at, chaosRepeat, prefix, func(scope string, ins []bench.Injection) {
			results, err := invariant.Check(ctx, s.DB(), scope)
			var bad []string
			for _, r := range results {
				if r.Violations > 0 {
					bad = append(bad, fmt.Sprintf("%s=%d %v", r.ID, r.Violations, r.Examples))
					violations += r.Violations
				}
			}
			verdict := "invariants ok"
			switch {
			case err != nil:
				verdict = "invariants not checked: " + err.Error()
			case len(bad) > 0:
				verdict = "VIOLATED " + strings.Join(bad, " ")
			}
			for _, in := range ins {
				n++
				if !in.Recovered() {
					fmt.Printf("  run %d  %s  not recovered: %s  %s\n", n, in.Conversation, in.Error, verdict)
					continue
				}
				recovered++
				fmt.Printf("  run %d  %s  injected at %s  %s in %dms  %s\n", n, in.Conversation,
					in.Injected.Format(time.RFC3339), in.State, in.RecoveryTime().Milliseconds(), verdict)
			}
		})
		fmt.Printf("recovered %d/%d   invariant violations: %d\n", recovered, n, violations)
		if err != nil {
			return err
		}
		if violations > 0 {
			return fmt.Errorf("%d invariant violation(s)", violations)
		}
		return nil
	},
}

var chaosListCmd = &cobra.Command{
	Use:   "list",
	Short: "List the failure classes and the kill boundaries",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("classes:    " + strings.Join(bench.Classes, " "))
		fmt.Println("boundaries: " + strings.Join(chaos.Boundaries, " ") + "  (tool_* take :<tool>)")
	},
}

// openStack connects to the compose stack named by the stack flags.
func openStack() (*bench.Stack, error) {
	services := []string{"conductor", "conductor-2", "conductor-3"}
	if len(stackServers) != len(services) {
		return nil, fmt.Errorf("--servers needs one address for each of %s", strings.Join(services, ", "))
	}
	replicas := make([]bench.Replica, len(services))
	for i, svc := range services {
		replicas[i] = bench.Replica{Service: svc, Addr: stackServers[i]}
	}
	return bench.Open(stackComposeFile, stackDSN, replicas)
}

func registerStackFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().StringVar(&stackComposeFile, "compose-file", "deploy/compose/docker-compose.yml", "The compose stack to drive")
	cmd.PersistentFlags().StringVar(&stackDSN, "dsn", "postgres://conductor:conductor@localhost:5432/conductor?sslmode=disable", "The stack's PostgreSQL, from the host")
	cmd.PersistentFlags().StringSliceVar(&stackServers, "servers", []string{"localhost:8494", "localhost:8495", "localhost:8496"}, "gRPC addresses of conductor, conductor-2 and conductor-3, from the host")
}

func init() {
	registerStackFlags(chaosCmd)
	chaosRunCmd.Flags().StringVar(&chaosClass, "class", "", "Failure class (see chaos list)")
	chaosRunCmd.Flags().StringVar(&chaosAt, "at", "", "Kill boundary for controller_kill_sigkill, <boundary>[:<tool>] (default "+bench.DefaultKillAt+")")
	chaosRunCmd.Flags().IntVar(&chaosRepeat, "repeat", 1, "Injections")
	_ = chaosRunCmd.MarkFlagRequired("class")
	chaosCmd.AddCommand(chaosRunCmd, chaosListCmd)
	rootCmd.AddCommand(chaosCmd)
}
