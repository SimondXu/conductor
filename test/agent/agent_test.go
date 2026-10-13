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

// Package agent drives a small refund agent through real `conductor serve`
// processes on PostgreSQL, kills them at chosen breakpoints, and checks what
// recovery did: the run's end state, how many real refunds and emails the
// tool backend saw, the ledger, and every invariant in correctness.md. See
// README.md for how to run it against a mock or a real model.
package agent

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"net"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"google.golang.org/protobuf/encoding/protojson"

	schema "github.com/simondxu/conductor/db"
	"github.com/simondxu/conductor/internal/invariant"
	"github.com/simondxu/conductor/proto"
)

const task = "Order 1042 arrived damaged. Use the tools, one call per turn, and wait for each result: " +
	"first look the order up, then refund the full amount it shows, then email the customer a one-line confirmation. " +
	"Do not ask me anything."

var (
	mode      = os.Getenv("CONDUCTOR_E2E_AGENT") // mock | anthropic
	dsn       = os.Getenv("PG_TEST_DSN")
	modelID   = "claude-haiku-5-5"
	bin       string // the conductor binary
	model     *meter
	mock      *mockModel // the mock model, nil with a real one
	modelBase string     // the meter's URL, the harness's base_url
)

func TestMain(m *testing.M) {
	if mode != "mock" && mode != "anthropic" || dsn == "" {
		fmt.Println("skipping: set CONDUCTOR_E2E_AGENT=mock|anthropic and PG_TEST_DSN (see README.md)")
		os.Exit(0)
	}
	if v := os.Getenv("CONDUCTOR_E2E_MODEL"); v != "" {
		modelID = v
	}
	if mode == "anthropic" && !strings.HasPrefix(modelID, haikuPrefix) {
		fmt.Printf("CONDUCTOR_E2E_MODEL=%q: the real API may only be called with %s* models\n", modelID, haikuPrefix)
		os.Exit(1)
	}
	if mode == "anthropic" && os.Getenv("ANTHROPIC_API_KEY") == "" {
		fmt.Println("CONDUCTOR_E2E_AGENT=anthropic needs ANTHROPIC_API_KEY")
		os.Exit(1)
	}
	if _, err := schema.Up(dsn); err != nil {
		fmt.Println("migrate up:", err)
		os.Exit(1)
	}
	if err := parkLeftovers(); err != nil {
		fmt.Println("parking leftover runs:", err)
		os.Exit(1)
	}
	dir, err := os.MkdirTemp("", "conductor-e2e-")
	if err != nil {
		panic(err)
	}
	bin = filepath.Join(dir, "conductor")
	build := exec.Command("go", "build", "-o", bin, "./cmd/conductor")
	build.Dir = "../.."
	if out, err := build.CombinedOutput(); err != nil {
		fmt.Printf("go build: %v\n%s", err, out)
		os.Exit(1)
	}

	upstream := "https://api.anthropic.com"
	if mode == "mock" {
		mock = &mockModel{}
		upstream = httptest.NewServer(mock).URL
	}
	max := 250
	fmt.Sscan(os.Getenv("CONDUCTOR_E2E_MAX_REQUESTS"), &max)
	model = newMeter(upstream, max, mode == "anthropic")
	srv := httptest.NewServer(model)
	modelBase = srv.URL

	code := m.Run()
	srv.Close()
	fmt.Println("model:", model)
	if a := model.aborted(); a != "" {
		fmt.Println("ABORTED:", a)
		code = 1
	}
	os.Exit(code)
}

// parkLeftovers defers, for a day, the recovery of every run an earlier
// test run left PENDING. Otherwise this run's replicas would adopt it, call
// this run's tool backends and model, and skew every count.
func parkLeftovers() error {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	res, err := db.Exec(`UPDATE runs SET next_attempt_at = now() + interval '1 day'
		WHERE conversation_id LIKE 'e2e-agent-%' AND state = 'PENDING'`)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n > 0 {
		fmt.Printf("parked %d PENDING run(s) left by an earlier run\n", n)
	}
	return nil
}

// rig is one scenario: a tool backend, a conversation, and the replicas.
type rig struct {
	t     *testing.T
	db    *sql.DB
	tools *tools
	conv  string
	dir   string
	cfg   map[string]string // replica name -> config file
	addr  map[string]string
	procs map[string]*proc
	req0  int // model requests before the scenario
}

type proc struct {
	cmd  *exec.Cmd
	done chan struct{}
}

func newRig(t *testing.T) *rig {
	t.Helper()
	if a := model.aborted(); a != "" {
		t.Skip("aborted earlier: " + a)
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	r := &rig{t: t, db: db, tools: newTools(), dir: t.TempDir(),
		conv: "e2e-agent-" + t.Name() + "-" + uuid.NewString()[:8],
		cfg:  map[string]string{}, addr: map[string]string{}, procs: map[string]*proc{}, req0: model.count()}
	t.Cleanup(func() { t.Logf("model requests: %d", r.modelRequests()) })
	// A run this scenario leaves PENDING must not be adopted by a later one.
	t.Cleanup(func() {
		db.Exec(`UPDATE runs SET next_attempt_at = now() + interval '1 day' WHERE conversation_id = $1 AND state = 'PENDING'`, r.conv)
	})
	ts := httptest.NewServer(r.tools)
	t.Cleanup(ts.Close)
	// CONDUCTOR_E2E_EFFORT sets output_config.effort: a real model at a
	// low effort may not think at all, and then no thinking block is sent
	// back for the API to check.
	effort := ""
	if e := os.Getenv("CONDUCTOR_E2E_EFFORT"); e != "" {
		effort = "    effort: " + e + "\n"
	}
	for _, name := range []string{"a", "b"} {
		r.addr[name] = freeAddr(t)
		path := filepath.Join(r.dir, name+".yaml")
		cfg := fmt.Sprintf(`version: v1alpha
server:
  address: %s
eventlog:
  postgres:
    dsn: ${PG_TEST_DSN}
    lease_ttl: 3s
registry:
  antigravity:
    disabled: true
  anthropic:
    default: true
    model: %s
    base_url: %s
    max_tokens: 4000
    request_timeout: 60s
%s%s`, r.addr[name], modelID, modelBase, effort, toolConfig(ts.URL))
		if err := os.WriteFile(path, []byte(cfg), 0o600); err != nil {
			t.Fatal(err)
		}
		r.cfg[name] = path
	}
	t.Cleanup(r.stopAll)
	return r
}

// start runs `conductor serve` as replica name; chaos arms a breakpoint.
func (r *rig) start(name, chaos string) {
	r.t.Helper()
	log, err := os.OpenFile(filepath.Join(r.dir, name+".log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		r.t.Fatal(err)
	}
	fmt.Fprintf(log, "--- start, CONDUCTOR_CHAOS_KILL_AT=%q\n", chaos)
	cmd := exec.Command(bin, "serve", "--config", r.cfg[name], "--metrics-addr", "",
		"--drain-timeout", "2s", "--recovery-interval", "500ms", "--owner-id", r.conv+"-"+name)
	cmd.Env = append(os.Environ(), "CONDUCTOR_CHAOS_KILL_AT="+chaos)
	cmd.Stdout, cmd.Stderr = log, log
	if err := cmd.Start(); err != nil {
		r.t.Fatal(err)
	}
	p := &proc{cmd: cmd, done: make(chan struct{})}
	r.procs[name] = p
	go func() { cmd.Wait(); close(p.done) }()
	for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
		if c, err := net.Dial("tcp", r.addr[name]); err == nil {
			c.Close()
			return
		}
	}
	r.t.Fatalf("replica %s did not listen on %s:\n%s", name, r.addr[name], r.logs(name))
}

// send starts the run on replica name and does not wait for it.
func (r *rig) send(name string) *client { return r.client(name, "--input", task) }

// client runs `conductor run` against replica name, on the rig's
// conversation, and does not wait for it.
func (r *rig) client(name string, args ...string) *client {
	r.t.Helper()
	c := &client{cmd: exec.Command(bin, append([]string{"run", "--server", r.addr[name], "--conversation", r.conv}, args...)...),
		done: make(chan struct{})}
	c.cmd.Stdout, c.cmd.Stderr = &c.out, &c.out
	if err := c.cmd.Start(); err != nil {
		r.t.Fatal(err)
	}
	go func() { c.cmd.Wait(); close(c.done) }()
	return c
}

// client is a `conductor run` process.
type client struct {
	cmd  *exec.Cmd
	out  bytes.Buffer
	done chan struct{}
}

// wait waits for the client to exit and returns its output and exit code.
func (c *client) wait(t *testing.T) (string, int) {
	t.Helper()
	select {
	case <-c.done:
	case <-time.After(90 * time.Second):
		c.cmd.Process.Kill()
		<-c.done
		t.Fatalf("client did not exit:\n%s", c.out.String())
	}
	return c.out.String(), c.cmd.ProcessState.ExitCode()
}

// signal sends sig to replica name and waits for it to exit.
func (r *rig) signal(name string, sig syscall.Signal) {
	r.t.Helper()
	r.procs[name].cmd.Process.Signal(sig)
	r.waitExit(name)
}

func (r *rig) waitExit(name string) {
	r.t.Helper()
	select {
	case <-r.procs[name].done:
	case <-time.After(60 * time.Second):
		r.t.Fatalf("replica %s did not exit:\n%s", name, r.logs(name))
	}
}

func (r *rig) stopAll() {
	for _, p := range r.procs {
		p.cmd.Process.Kill()
		<-p.done
	}
}

func (r *rig) logs(name string) string {
	b, _ := os.ReadFile(filepath.Join(r.dir, name+".log"))
	s := string(b)
	if len(s) > 4000 {
		s = "…" + s[len(s)-4000:]
	}
	return s
}

func (r *rig) query(q string) string {
	var s sql.NullString
	if err := r.db.QueryRow(q, r.conv).Scan(&s); err != nil && err != sql.ErrNoRows {
		r.t.Fatalf("%s: %v", q, err)
	}
	return s.String
}

// waitState waits for the run to leave PENDING and returns its state.
func (r *rig) waitState(timeout time.Duration) string {
	r.t.Helper()
	for deadline := time.Now().Add(timeout); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		if s := r.query(`SELECT state FROM runs WHERE conversation_id = $1`); s != "" && s != "PENDING" {
			return s
		}
		if a := model.aborted(); a != "" {
			r.t.Fatal("aborted: " + a)
		}
	}
	r.t.Fatalf("run still %q after %s\n--- a ---\n%s\n--- b ---\n%s", r.query(`SELECT state FROM runs WHERE conversation_id = $1`), timeout, r.logs("a"), r.logs("b"))
	return ""
}

// waitCall waits until the call to tool is in state.
func (r *rig) waitCall(tool, state string) {
	r.t.Helper()
	q := `SELECT state FROM tool_calls WHERE conversation_id = $1 AND tool_name = '` + tool + `' ORDER BY step DESC LIMIT 1`
	for deadline := time.Now().Add(60 * time.Second); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
		if r.query(q) == state {
			return
		}
	}
	r.t.Fatalf("%s never reached %s (now %q)\n%s", tool, state, r.query(q), r.logs("a"))
}

// ledger is "tool:STATE:attempts" per call, in step order.
func (r *rig) ledger() string {
	rows, err := r.db.Query(`SELECT tool_name, state, attempts FROM tool_calls WHERE conversation_id = $1 ORDER BY step`, r.conv)
	if err != nil {
		r.t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var tool, state string
		var attempts int
		rows.Scan(&tool, &state, &attempts)
		out = append(out, fmt.Sprintf("%s:%s:%d", tool, state, attempts))
	}
	return strings.Join(out, " ")
}

// check asserts the end state, the real effects and every invariant.
func (r *rig) check(state string, refunds, emails int) {
	r.t.Helper()
	got := r.waitState(90 * time.Second)
	gotRefunds, gotEmails, reqs := r.tools.counts()
	r.t.Logf("state %s; refunds %d, emails %d; requests %v; ledger %s", got, gotRefunds, gotEmails, reqs, r.ledger())
	if got != state || gotRefunds != refunds || gotEmails != emails {
		r.t.Errorf("got %s with %d refund(s), %d email(s); want %s with %d, %d\n--- a ---\n%s\n--- b ---\n%s",
			got, gotRefunds, gotEmails, state, refunds, emails, r.logs("a"), r.logs("b"))
	}
	r.invariants()
}

// invariants asserts every invariant over the conversation.
func (r *rig) invariants() {
	r.t.Helper()
	for _, v := range r.violations() {
		r.t.Error(v)
	}
}

// violations checks every invariant over the conversation and describes
// each one violated.
func (r *rig) violations() []string {
	r.t.Helper()
	results, err := invariant.Check(context.Background(), r.db, r.conv)
	if err != nil {
		r.t.Fatal(err)
	}
	var out []string
	for _, res := range results {
		if res.Violations > 0 {
			out = append(out, fmt.Sprintf("%s violated: %s %v", res.ID, res.Assertion, res.Examples))
		}
	}
	return out
}

// attempts is the attempts of the last call to tool.
func (r *rig) attempts(tool string) string {
	return r.query(`SELECT attempts::text FROM tool_calls WHERE conversation_id = $1 AND tool_name = '` + tool + `' ORDER BY step DESC LIMIT 1`)
}

// modelRequests is how many model requests the scenario has made.
func (r *rig) modelRequests() int { return model.count() - r.req0 }

// lastStep is the run's last logged step.
func (r *rig) lastStep() int64 {
	n, _ := strconv.ParseInt(r.query(`SELECT last_step::text FROM runs WHERE conversation_id = $1`), 10, 64)
	return n
}

// waitPast waits until the log has a step after step.
func (r *rig) waitPast(step int64) {
	r.t.Helper()
	for deadline := time.Now().Add(30 * time.Second); r.lastStep() <= step; time.Sleep(100 * time.Millisecond) {
		if time.Now().After(deadline) {
			r.t.Fatalf("nothing logged after step %d\n--- a ---\n%s\n--- b ---\n%s", step, r.logs("a"), r.logs("b"))
		}
	}
}

// events is the conversation's log.
func (r *rig) events() []*proto.StepEvent {
	r.t.Helper()
	rows, err := r.db.Query(`SELECT payload FROM conversation_log WHERE conversation_id = $1 ORDER BY step`, r.conv)
	if err != nil {
		r.t.Fatal(err)
	}
	defer rows.Close()
	var out []*proto.StepEvent
	for rows.Next() {
		var payload string
		ev := &proto.StepEvent{}
		if err := rows.Scan(&payload); err != nil {
			r.t.Fatal(err)
		}
		if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal([]byte(payload), ev); err != nil {
			r.t.Fatal(err)
		}
		out = append(out, ev)
	}
	return out
}

// history is every logged step, in order.
func (r *rig) history() []*proto.Step {
	var out []*proto.Step
	for _, ev := range r.events() {
		out = append(out, ev.Steps...)
	}
	return out
}

// userInputs counts the user messages in the log.
func (r *rig) userInputs() int {
	n := 0
	for _, s := range r.history() {
		if s.GetContent().GetRole() == "user" {
			n++
		}
	}
	return n
}

// skipBackoff keeps clearing the run's recovery backoff (5 s, doubling)
// until the scenario ends, so a run that fails every recovery reaches the
// poison guard in seconds rather than 155 s. It changes when recoveries
// happen, not how many a run gets: next_attempt_at is the reconciler's
// bookkeeping, not part of the runs projection (ARCHITECTURE.md §2.3). It
// returns a func that stops it sooner.
func (r *rig) skipBackoff() (stop func()) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	stop = func() { cancel(); <-done }
	r.t.Cleanup(stop)
	go func() {
		defer close(done)
		for ctx.Err() == nil {
			r.db.ExecContext(ctx, `UPDATE runs SET next_attempt_at = NULL WHERE conversation_id = $1 AND next_attempt_at IS NOT NULL`, r.conv)
			select {
			case <-ctx.Done():
			case <-time.After(100 * time.Millisecond):
			}
		}
	}()
	return stop
}

func freeAddr(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

// No failure: the agent looks up, refunds once, emails once.
func TestBaseline(t *testing.T) {
	r := newRig(t)
	r.start("a", "")
	r.send("a")
	r.check("COMPLETED", 1, 1)
}

// The controller dies at a chaos boundary and a fresh replica recovers the
// run. Each row says what the ledger must show for refund_payment.
func TestKillAtBoundary(t *testing.T) {
	for _, tc := range []struct {
		at             string
		refundAttempts string // "" = not checked
	}{
		{"model_started", "1"},   // before a model call: nothing to redo but the call
		{"model_completed", "1"}, // calls logged, not planned: planned and run on resume
		{"tool_planned:refund_payment", "1"},
		{"tool_started:refund_payment", "2"}, // STARTED, never sent: IDEMPOTENT, so run again
		{"tool_settled:refund_payment", "1"}, // settled: never run again
	} {
		t.Run(strings.ReplaceAll(tc.at, ":", "_"), func(t *testing.T) {
			r := newRig(t)
			r.start("a", tc.at)
			r.send("a")
			r.waitExit("a")
			r.start("b", "")
			r.check("COMPLETED", 1, 1)
			if got := r.attempts("refund_payment"); tc.refundAttempts != "" && got != tc.refundAttempts {
				t.Errorf("refund_payment attempts = %s, want %s", got, tc.refundAttempts)
			}
		})
	}
}

// The refund happened but its result was lost with the controller. The
// call is run again under the same Idempotency-Key, which the backend
// honours: one real refund, two requests.
func TestRefundDoneResultLost(t *testing.T) {
	r := newRig(t)
	r.tools.hangAfter = "refund_payment"
	r.start("a", "")
	r.send("a")
	<-r.tools.effectDone
	r.signal("a", syscall.SIGKILL)
	r.start("b", "")
	r.check("COMPLETED", 1, 1)
	if _, _, reqs := r.tools.counts(); reqs["refund_payment"] != 2 {
		t.Errorf("refund_payment requests = %d, want 2 under one key", reqs["refund_payment"])
	}
}

// The email was sent but its result was lost. send_email is UNSAFE, so it is
// never sent again: the run fails instead, with exactly one email out.
func TestEmailSentResultLost(t *testing.T) {
	r := newRig(t)
	r.tools.hangAfter = "send_email"
	r.start("a", "")
	r.send("a")
	<-r.tools.effectDone
	r.signal("a", syscall.SIGKILL)
	r.start("b", "")
	r.check("FAILED", 1, 1)
}

// Graceful handover: the owner gets SIGTERM during a slow refund; after its
// drain it releases the lease and the other replica takes over at once.
func TestGracefulHandover(t *testing.T) {
	r := newRig(t)
	r.tools.slow, r.tools.delay = "refund_payment", 10*time.Second
	r.start("a", "")
	r.start("b", "")
	r.send("a")
	<-r.tools.started
	begin := time.Now()
	r.signal("a", syscall.SIGTERM)
	r.check("COMPLETED", 1, 1)
	t.Logf("handover to completion: %s", time.Since(begin).Round(100*time.Millisecond))
}

// The owner is SIGKILLed after the refund's effect: the other replica waits
// out the dead lease, takes over at a higher generation and completes.
func TestOwnerKilledTakeover(t *testing.T) {
	r := newRig(t)
	r.tools.hangAfter = "refund_payment"
	r.start("a", "")
	r.start("b", "")
	r.send("a")
	<-r.tools.effectDone
	r.signal("a", syscall.SIGKILL)
	r.check("COMPLETED", 1, 1)
	if g, _ := strconv.Atoi(r.query(`SELECT max(generation)::text FROM tool_calls WHERE conversation_id = $1`)); g < 2 {
		t.Errorf("max generation = %d, want 2 or more after a takeover", g)
	}
}
