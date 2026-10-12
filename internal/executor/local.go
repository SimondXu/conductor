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
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"
)

// readyMarker precedes the address in the line a harness logs once it
// serves: `conductor harness fake` logs "fake harness serving on <addr>".
const readyMarker = "serving on "

// startTimeout bounds how long a subprocess may take to report its address.
const startTimeout = 10 * time.Second

// stopTimeout is how long a subprocess gets after SIGTERM before SIGKILL.
const stopTimeout = 5 * time.Second

// Local runs each conversation's harness as a subprocess on this host. It is
// what chaos kills when it kills "the harness process".
type Local struct {
	// Command is the harness program and its arguments; Local appends
	// --host 127.0.0.1 --port 0 and reads the address from its stderr.
	Command []string
	// Env is added to the inherited environment.
	Env []string

	mu    sync.Mutex
	procs map[string]*localProc // by conversation
}

var _ Executor = (*Local)(nil)

type localProc struct {
	cmd    *exec.Cmd
	gen    int64
	exited chan struct{}
}

// Acquire starts a subprocess for the conversation, stopping the one it had
// before: the newest acquisition wins.
func (l *Local) Acquire(ctx context.Context, conversationID string, spec Spec) (Target, error) {
	l.mu.Lock()
	if l.procs == nil {
		l.procs = map[string]*localProc{}
	}
	old := l.procs[conversationID]
	delete(l.procs, conversationID)
	l.mu.Unlock()
	if old != nil {
		old.stop()
	}

	args := append(append([]string{}, l.Command[1:]...), "--host", "127.0.0.1", "--port", "0")
	cmd := exec.Command(l.Command[0], args...)
	cmd.Env = append(os.Environ(), l.Env...)
	cmd.Stdout = os.Stderr
	setParentDeathSignal(cmd)
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return Target{}, err
	}
	if err := cmd.Start(); err != nil {
		return Target{}, fmt.Errorf("local executor: start %s: %w", l.Command[0], err)
	}
	p := &localProc{cmd: cmd, gen: spec.Generation, exited: make(chan struct{})}
	addrs := make(chan string, 1)
	go func() {
		// Forward the subprocess's log after picking out its address.
		sc := bufio.NewScanner(stderr)
		for sc.Scan() {
			line := sc.Text()
			if i := strings.Index(line, readyMarker); i >= 0 {
				select {
				case addrs <- strings.TrimSpace(line[i+len(readyMarker):]):
				default:
				}
			}
			fmt.Fprintln(os.Stderr, line)
		}
		_, _ = io.Copy(os.Stderr, stderr)
	}()
	go func() {
		_ = cmd.Wait()
		close(p.exited)
	}()

	timer := time.NewTimer(startTimeout)
	defer timer.Stop()
	select {
	case addr := <-addrs:
		l.mu.Lock()
		l.procs[conversationID] = p
		l.mu.Unlock()
		return Target{Addr: addr, Kind: "local", Generation: spec.Generation}, nil
	case <-p.exited:
		return Target{}, errors.New("local executor: harness exited before serving")
	case <-timer.C:
		p.stop()
		return Target{}, fmt.Errorf("local executor: harness did not serve within %v", startTimeout)
	case <-ctx.Done():
		p.stop()
		return Target{}, ctx.Err()
	}
}

// Release stops the conversation's subprocess if it is of generation gen.
func (l *Local) Release(ctx context.Context, conversationID string, gen int64) error {
	l.mu.Lock()
	p := l.procs[conversationID]
	if p == nil || p.gen != gen {
		l.mu.Unlock()
		return nil
	}
	delete(l.procs, conversationID)
	l.mu.Unlock()
	p.stop()
	return nil
}

// Close stops every subprocess.
func (l *Local) Close() error {
	l.mu.Lock()
	procs := l.procs
	l.procs = nil
	l.mu.Unlock()
	for _, p := range procs {
		p.stop()
	}
	return nil
}

// stop sends SIGTERM, then SIGKILL after stopTimeout, and waits for exit.
func (p *localProc) stop() {
	_ = p.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-p.exited:
	case <-time.After(stopTimeout):
		_ = p.cmd.Process.Kill()
		<-p.exited
	}
}
