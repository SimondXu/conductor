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

// Package bench drives the compose stack (deploy/compose) from the host:
// failure injection for `conductor chaos`, load for `conductor bench`
// (docs/conductor/PRD.md §5, §8). It controls containers with the docker
// compose CLI, sends runs over gRPC to each replica's published port, and
// observes outcomes in PostgreSQL.
package bench

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/simondxu/conductor/internal/chaos"
	"github.com/simondxu/conductor/proto"
)

// Replica is one controller service and its gRPC address on the host.
type Replica struct {
	Service, Addr string
}

// Stack is a running compose stack.
type Stack struct {
	composeFile string
	replicas    []Replica
	conns       []*grpc.ClientConn
	db          *sql.DB
}

// Open connects to the stack's database and replicas. The first replica is
// the one chaos kills.
func Open(composeFile, dsn string, replicas []Replica) (*Stack, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	s := &Stack{composeFile: composeFile, replicas: replicas, db: db}
	for _, r := range replicas {
		conn, err := grpc.NewClient(r.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			s.Close()
			return nil, fmt.Errorf("connect %s: %w", r.Service, err)
		}
		s.conns = append(s.conns, conn)
	}
	return s, nil
}

// DB is the stack's database.
func (s *Stack) DB() *sql.DB { return s.db }

// Close closes the connections.
func (s *Stack) Close() error {
	for _, c := range s.conns {
		c.Close()
	}
	return s.db.Close()
}

// compose runs docker compose against the stack, with env added to ours.
func (s *Stack) compose(ctx context.Context, env []string, args ...string) error {
	cmd := exec.CommandContext(ctx, "docker", append([]string{"compose", "-f", s.composeFile}, args...)...)
	cmd.Env = append(os.Environ(), env...)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("docker compose %s: %w: %s", strings.Join(args, " "), err, out)
	}
	return nil
}

// up starts services that are not running and waits until they are healthy,
// with the chaos hook armed at spec ("" disarms it). A changed spec
// recreates the container.
func (s *Stack) up(ctx context.Context, spec string, services ...string) error {
	return s.compose(ctx, []string{chaos.Env + "=" + spec}, append([]string{"up", "-d", "--wait", "--no-deps"}, services...)...)
}

// state is a service's container state: running, exited, paused, ...
func (s *Stack) state(ctx context.Context, service string) (string, error) {
	cmd := exec.CommandContext(ctx, "docker", "compose", "-f", s.composeFile, "ps", "-a", "--format", "{{.State}}", service)
	out, err := cmd.Output()
	return strings.TrimSpace(string(out)), err
}

// Send runs one turn of conv on replica i, with the fake's agent config cfg,
// and reads its stream to the end.
func (s *Stack) Send(ctx context.Context, i int, conv, cfg, input string) error {
	stream, err := proto.NewInteractionsServiceClient(s.conns[i]).CreateInteraction(ctx, &proto.CreateInteractionEvent{
		ConversationId: conv,
		AgentConfig:    []byte(cfg),
		Inputs: []*proto.Step{{Type: &proto.Step_Content{Content: &proto.ContentStep{
			Role: "user", Content: []*proto.Content{{Type: &proto.Content_Text{Text: &proto.TextContent{Text: input}}}},
		}}}},
	})
	if err != nil {
		return err
	}
	for {
		if _, err := stream.Recv(); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
}

// runState is conv's state in the runs projection, "" before it has one.
func (s *Stack) runState(ctx context.Context, conv string) (string, error) {
	var st string
	err := s.db.QueryRowContext(ctx, "SELECT state FROM runs WHERE conversation_id = $1", conv).Scan(&st)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return st, err
}

// callState is the ledger state of conv's call to tool, "" before it has one.
func (s *Stack) callState(ctx context.Context, conv, tool string) (string, error) {
	var st string
	err := s.db.QueryRowContext(ctx, "SELECT state FROM tool_calls WHERE conversation_id = $1 AND tool_name = $2 ORDER BY step DESC LIMIT 1", conv, tool).Scan(&st)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return st, err
}

// pollTimeout bounds one poll, so a paused database stalls a wait, not
// the driver.
const pollTimeout = 2 * time.Second

// waitFor polls cond every 50ms until it holds or timeout passes. A failing
// poll is retried; the last error is reported on timeout.
func waitFor(ctx context.Context, timeout time.Duration, what string, cond func(context.Context) (bool, error)) error {
	deadline := time.Now().Add(timeout)
	var last error
	for {
		pctx, cancel := context.WithTimeout(ctx, pollTimeout)
		ok, err := cond(pctx)
		cancel()
		if ok {
			return nil
		}
		if err != nil {
			last = err
		}
		if time.Now().After(deadline) {
			if last != nil {
				return fmt.Errorf("waited %s for %s: %w", timeout, what, last)
			}
			return fmt.Errorf("waited %s for %s", timeout, what)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// waitTerminal waits for conv to leave PENDING and returns its state and
// when it was seen.
func (s *Stack) waitTerminal(ctx context.Context, conv string, timeout time.Duration) (string, time.Time, error) {
	var st string
	err := waitFor(ctx, timeout, conv+" to end", func(ctx context.Context) (bool, error) {
		var err error
		st, err = s.runState(ctx, conv)
		return st != "" && st != "PENDING", err
	})
	return st, time.Now(), err
}

// waitCall waits for conv's call to tool to reach state.
func (s *Stack) waitCall(ctx context.Context, conv, tool, state string) error {
	return waitFor(ctx, 30*time.Second, conv+" "+tool+" "+state, func(ctx context.Context) (bool, error) {
		st, err := s.callState(ctx, conv, tool)
		return st == state, err
	})
}

// waitService waits for service's container to reach state.
func (s *Stack) waitService(ctx context.Context, service, state string) error {
	return waitFor(ctx, 60*time.Second, service+" "+state, func(ctx context.Context) (bool, error) {
		st, err := s.state(ctx, service)
		return st == state, err
	})
}
