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

package executor

import (
	"context"
	"log"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/simondxu/conductor/internal/harness"
	"github.com/simondxu/conductor/internal/harness/fake"
	"github.com/simondxu/conductor/internal/harness/harnesstest"
	"github.com/simondxu/conductor/internal/harness/remote"
	"github.com/simondxu/conductor/proto"
)

// TestMain doubles as the harness subprocess: with CONDUCTOR_TEST_HARNESS
// set, the test binary serves a fake harness the way `conductor harness
// fake` does, and logs its address the same way.
func TestMain(m *testing.M) {
	if os.Getenv("CONDUCTOR_TEST_HARNESS") == "1" {
		serveFakeHarness()
		return
	}
	os.Exit(m.Run())
}

func serveFakeHarness() {
	var effects fake.EffectSink = &fake.MemoryEffects{}
	if dsn := os.Getenv("PG_TEST_DSN"); dsn != "" {
		pg, err := fake.OpenPostgresEffects(context.Background(), dsn)
		if err != nil {
			log.Fatal(err)
		}
		effects = pg
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	srv := grpc.NewServer()
	proto.RegisterHarnessServiceServer(srv, remote.NewServer(fake.New(effects)))
	log.Printf("fake harness serving on %s", lis.Addr())
	_ = srv.Serve(lis)
}

// newLocal runs the test binary as the harness subprocess.
func newLocal() *Local {
	return &Local{Command: []string{os.Args[0]}, Env: []string{"CONDUCTOR_TEST_HARNESS=1"}}
}

func (l *Local) proc(conv string) *localProc {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.procs[conv]
}

func exited(p *localProc) bool {
	select {
	case <-p.exited:
		return true
	case <-time.After(2 * time.Second):
		return false
	}
}

// One execution plays a model turn in its own subprocess, and closing it
// stops that subprocess.
func TestHarness_ExecutionRunsInItsOwnSubprocess(t *testing.T) {
	l := newLocal()
	t.Cleanup(func() { l.Close() })
	h := &Harness{Executor: l, AgentID: "fake"}
	ctx := WithGeneration(context.Background(), 3)
	e, err := h.Start(ctx, "conv", []byte(`{"turns":[{"content":"hi"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	p := l.proc("conv")
	if p == nil || p.gen != 3 {
		t.Fatalf("subprocess = %+v, want one at generation 3", p)
	}
	if err := e.Queue(ctx, harnesstest.UserStep("go")); err != nil {
		t.Fatal(err)
	}
	var got []string
	if err := e.Run(ctx, handlerFunc(func(s *proto.Step) { got = append(got, s.GetContent().GetContent()[0].GetText().GetText()) })); err != nil {
		t.Fatal(err)
	}
	if strings.Join(got, ",") != "hi" {
		t.Fatalf("steps = %v, want [hi]", got)
	}
	if err := e.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if !exited(p) || l.proc("conv") != nil {
		t.Fatal("subprocess still running after Close")
	}
}

// A newer generation replaces the older subprocess, and the older owner's
// late Release leaves the newer one running.
func TestLocal_NewerGenerationReplacesOlder(t *testing.T) {
	l := newLocal()
	t.Cleanup(func() { l.Close() })
	ctx := context.Background()
	if _, err := l.Acquire(ctx, "conv", Spec{Generation: 1}); err != nil {
		t.Fatal(err)
	}
	old := l.proc("conv")
	if _, err := l.Acquire(ctx, "conv", Spec{Generation: 2}); err != nil {
		t.Fatal(err)
	}
	if !exited(old) {
		t.Fatal("generation 1 still running")
	}
	if err := l.Release(ctx, "conv", 1); err != nil {
		t.Fatal(err)
	}
	if p := l.proc("conv"); p == nil || p.gen != 2 {
		t.Fatalf("after a stale release: %+v, want generation 2 running", p)
	}
}

type handlerFunc func(*proto.Step)

func (f handlerFunc) OnMessage(_ context.Context, _ string, s *proto.Step) error { f(s); return nil }
func (f handlerFunc) OnComplete(context.Context, string) error                   { return nil }

var _ harness.Handler = handlerFunc(nil)
