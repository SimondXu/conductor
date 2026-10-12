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
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/simondxu/conductor/internal/controller/lease"
	"github.com/simondxu/conductor/internal/controller/toolledger"
	"github.com/simondxu/conductor/proto"
)

var (
	inspectJSON          bool
	inspectFromStep      int64
	inspectUnsettledOnly bool
	inspectState         string
	inspectLimit         int
	inspectSettle        string
	inspectOutcome       string
	inspectReason        string
)

var inspectCmd = &cobra.Command{
	Use:   "inspect",
	Short: "Read a conversation's log, tool ledger and runs from PostgreSQL",
	Long: `Reads the durable state directly from PostgreSQL. Everything is read-only
except inspect tool --settle, the operator's override for an ABANDONED call.
The DSN comes from --dsn, else from eventlog.postgres.dsn in the config file.`,
}

// inspectDB opens the database named by --dsn or the config.
func inspectDB(cmd *cobra.Command) (*sql.DB, string, error) {
	dsn, err := resolveMigrateDSN(cmd)
	if err != nil {
		return nil, "", err
	}
	db, err := sql.Open("pgx", dsn)
	return db, dsn, err
}

// table writes rows to stdout, as aligned columns or, with --json, as a JSON
// array of objects keyed by header.
func table(header []string, rows [][]string) error {
	if inspectJSON {
		out := make([]map[string]string, len(rows))
		for i, r := range rows {
			out[i] = map[string]string{}
			for j, h := range header {
				out[i][h] = r[j]
			}
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(out)
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, strings.Join(header, "\t"))
	for _, r := range rows {
		fmt.Fprintln(w, strings.Join(r, "\t"))
	}
	return w.Flush()
}

var inspectConversationCmd = &cobra.Command{
	Use:   "conversation <id>",
	Short: "The conversation's log, one line per step",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		db, _, err := inspectDB(cmd)
		if err != nil {
			return err
		}
		defer db.Close()
		rows, err := db.QueryContext(cmd.Context(), `SELECT step, COALESCE(generation, 0), payload FROM conversation_log
			WHERE conversation_id = $1 AND step >= $2 ORDER BY step`, args[0], inspectFromStep)
		if err != nil {
			return err
		}
		defer rows.Close()
		var out [][]string
		for rows.Next() {
			var step, gen int64
			var payload string
			if err := rows.Scan(&step, &gen, &payload); err != nil {
				return err
			}
			ev := &proto.StepEvent{}
			if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal([]byte(payload), ev); err != nil {
				return fmt.Errorf("step %d: %w", step, err)
			}
			state := strings.TrimPrefix(ev.State.String(), "STATE_")
			if len(ev.Steps) == 0 {
				out = append(out, []string{fmt.Sprint(step), fmt.Sprint(gen), state, "-", "turn " + strings.ToLower(state)})
			}
			for _, s := range ev.Steps {
				kind, summary := describeStep(s)
				out = append(out, []string{fmt.Sprint(step), fmt.Sprint(gen), state, kind, summary})
			}
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if len(out) == 0 {
			return fmt.Errorf("conversation %q has no log at or after step %d", args[0], inspectFromStep)
		}
		return table([]string{"step", "gen", "state", "kind", "summary"}, out)
	},
}

// describeStep is a step's kind and a one-line summary of it.
func describeStep(s *proto.Step) (string, string) {
	switch {
	case s.GetToolCall() != nil:
		c := s.GetToolCall()
		return "tool_call", fmt.Sprintf("%s call_id=%s", c.GetFunctionCall().GetName(), short(c.GetId()))
	case s.GetToolResult() != nil:
		r := s.GetToolResult()
		verdict := "ok"
		if r.GetFunctionResult().GetIsError() {
			verdict = "error"
		}
		return "tool_result", fmt.Sprintf("%s %s call_id=%s", r.GetFunctionResult().GetName(), verdict, short(r.GetCallId()))
	case s.GetContent() != nil:
		var text []string
		for _, c := range s.GetContent().GetContent() {
			if t := c.GetText(); t != nil {
				text = append(text, t.GetText())
			}
		}
		return "content", fmt.Sprintf("%s %q", s.GetContent().GetRole(), clip(strings.Join(text, " "), 60))
	case s.GetThought() != nil:
		return "thought", "-"
	}
	return "other", "-"
}

func short(id string) string { return clip(id, 12) }

func clip(s string, n int) string {
	if r := []rune(s); len(r) > n {
		return string(r[:n]) + "..."
	}
	return s
}

var inspectToolsCmd = &cobra.Command{
	Use:   "tools <conversation-id>",
	Short: "The conversation's tool calls from the ledger",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		db, _, err := inspectDB(cmd)
		if err != nil {
			return err
		}
		defer db.Close()
		q := `SELECT call_id, tool_name, class, state, attempts, COALESCE(to_char(started_at, 'YYYY-MM-DD"T"HH24:MI:SSOF'), '-'),
			generation, COALESCE(error, '') FROM tool_calls WHERE conversation_id = $1`
		if inspectUnsettledOnly {
			q += ` AND state IN ('STARTED', 'OUTCOME_UNKNOWN')`
		}
		rows, err := db.QueryContext(cmd.Context(), q+` ORDER BY step`, args[0])
		if err != nil {
			return err
		}
		defer rows.Close()
		var out [][]string
		for rows.Next() {
			r := make([]string, 8)
			if err := rows.Scan(&r[0], &r[1], &r[2], &r[3], &r[4], &r[5], &r[6], &r[7]); err != nil {
				return err
			}
			if !inspectJSON {
				r[0] = short(r[0])
			}
			out = append(out, r)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if err := table([]string{"call_id", "tool", "class", "state", "attempts", "started_at", "gen", "error"}, out); err != nil {
			return err
		}
		if inspectUnsettledOnly && !inspectJSON {
			fmt.Printf("%d unsettled call(s)\n", len(out))
		}
		return nil
	},
}

var inspectRunsCmd = &cobra.Command{
	Use:   "runs",
	Short: "Runs from the runs projection, most recently updated first",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		db, _, err := inspectDB(cmd)
		if err != nil {
			return err
		}
		defer db.Close()
		rows, err := db.QueryContext(cmd.Context(), `SELECT conversation_id, state, COALESCE(agent_id, ''), last_step,
			recovery_attempts, to_char(updated_at, 'YYYY-MM-DD"T"HH24:MI:SSOF') FROM runs
			WHERE $1 = '' OR state = $1 ORDER BY updated_at DESC LIMIT $2`, strings.ToUpper(inspectState), inspectLimit)
		if err != nil {
			return err
		}
		defer rows.Close()
		var out [][]string
		for rows.Next() {
			r := make([]string, 6)
			if err := rows.Scan(&r[0], &r[1], &r[2], &r[3], &r[4], &r[5]); err != nil {
				return err
			}
			out = append(out, r)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		return table([]string{"conversation_id", "state", "harness", "last_step", "recovery_attempts", "updated_at"}, out)
	},
}

var inspectToolCmd = &cobra.Command{
	Use:   "tool --settle <call_id> --outcome SUCCEEDED|FAILED --reason <text>",
	Short: "Record what an ABANDONED call really did, after checking outside Conductor",
	Long: `The operator's override for an ABANDONED call (ARCHITECTURE.md §10): an UNSAFE
call whose outcome Conductor could not know fails its run as ABANDONED. Once
you have checked the external system, record what the call did. It takes the
conversation's lease for the write, so it is refused while a controller owns
the conversation. The run stays failed.`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		if inspectSettle == "" || inspectReason == "" {
			return errors.New("--settle and --reason are required")
		}
		outcome := toolledger.State(strings.ToUpper(inspectOutcome))
		db, dsn, err := inspectDB(cmd)
		if err != nil {
			return err
		}
		defer db.Close()
		// A unique prefix will do, as inspect tools prints call ids short.
		var callID, conv string
		var n int
		if err := db.QueryRowContext(cmd.Context(), `SELECT min(call_id), min(conversation_id), count(*) FROM tool_calls
			WHERE starts_with(call_id, $1)`, strings.TrimSuffix(inspectSettle, "...")).Scan(&callID, &conv, &n); err != nil && n != 0 {
			return err
		}
		if n != 1 {
			return fmt.Errorf("%q matches %d tool calls, want exactly one", inspectSettle, n)
		}
		return overrideCall(cmd.Context(), dsn, conv, callID, outcome, inspectReason)
	},
}

// overrideCall writes the override under the conversation's lease, so the
// write is fenced like any other (I4).
func overrideCall(ctx context.Context, dsn, conv, callID string, outcome toolledger.State, reason string) error {
	leases, err := lease.OpenPostgres(dsn)
	if err != nil {
		return err
	}
	defer leases.Close()
	ledger, err := toolledger.OpenPostgres(dsn)
	if err != nil {
		return err
	}
	defer ledger.Close()
	host, _ := os.Hostname()
	held, err := leases.Acquire(ctx, conv, fmt.Sprintf("operator/%s/%d", host, os.Getpid()), 30*time.Second)
	if err != nil {
		return fmt.Errorf("conversation %s: %w", conv, err)
	}
	defer held.Release(context.WithoutCancel(ctx))
	if err := ledger.Override(ctx, held.Generation(), callID, outcome, reason); err != nil {
		return err
	}
	fmt.Printf("%s %s: %s\n", callID, outcome, reason)
	return nil
}

func init() {
	inspectCmd.PersistentFlags().StringVar(&migrateDSN, "dsn", "", "PostgreSQL DSN (default: eventlog.postgres.dsn from the config)")
	inspectCmd.PersistentFlags().StringVar(&migrateConfig, "config", defaultConfigFile, "Path to YAML configuration file")
	inspectCmd.PersistentFlags().BoolVar(&inspectJSON, "json", false, "Print JSON")
	inspectConversationCmd.Flags().Int64Var(&inspectFromStep, "from-step", 1, "First step to print")
	inspectToolsCmd.Flags().BoolVar(&inspectUnsettledOnly, "unsettled-only", false, "Only calls in STARTED or OUTCOME_UNKNOWN")
	inspectRunsCmd.Flags().StringVar(&inspectState, "state", "", "Only runs in this state: PENDING, COMPLETED, FAILED or CANCELED")
	inspectRunsCmd.Flags().IntVar(&inspectLimit, "limit", 20, "Most runs to print")
	inspectToolCmd.Flags().StringVar(&inspectSettle, "settle", "", "call_id of the ABANDONED call, or a unique prefix of it")
	inspectToolCmd.Flags().StringVar(&inspectOutcome, "outcome", "", "What the call did: SUCCEEDED or FAILED")
	inspectToolCmd.Flags().StringVar(&inspectReason, "reason", "", "How you know, recorded in the ledger")
	inspectCmd.AddCommand(inspectConversationCmd, inspectToolsCmd, inspectRunsCmd, inspectToolCmd)
	rootCmd.AddCommand(inspectCmd)
}
