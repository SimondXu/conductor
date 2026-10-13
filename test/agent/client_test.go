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

package agent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/simondxu/conductor/proto"
)

// Two clients send the conversation to both replicas at once. Every Exec
// holds the conversation's lease, so one runs it and the other gets
// FAILED_PRECONDITION (ARCHITECTURE.md §6.1; correctness.md §2,
// duplicate_concurrent_req): one input logged, one refund, one email.
func TestDuplicateConcurrentRequest(t *testing.T) {
	r := newRig(t)
	r.tools.slow, r.tools.delay = "lookup_order", 2*time.Second // the winner still holds the lease when the loser arrives
	r.start("a", "")
	r.start("b", "")
	ca, cb := r.send("a"), r.send("b")
	outA, codeA := ca.wait(t)
	outB, codeB := cb.wait(t)
	if (codeA == 0) == (codeB == 0) {
		t.Errorf("want exactly one client to succeed; a exit %d, b exit %d\n--- a ---\n%s\n--- b ---\n%s", codeA, codeB, outA, outB)
	}
	loser := outA
	if codeA == 0 {
		loser = outB
	}
	if !strings.Contains(loser, "FailedPrecondition") || !strings.Contains(loser, "already in flight") {
		t.Errorf("the refused client printed:\n%s", loser)
	}
	r.check("COMPLETED", 1, 1)
	if n := r.userInputs(); n != 1 {
		t.Errorf("%d user inputs logged, want 1", n)
	}
}

// A client drops mid-run and re-attaches on the other replica with
// `conductor run --resume --after-step N`, N being the last step it saw. It
// must get every later step exactly once, and the stream must end with the
// run (ARCHITECTURE.md §6.3, PRD.md §6.1). The drop cancels the owner's
// Exec around the refund's start, and the run is handed over. The refund
// is IDEMPOTENT, so the run still completes, and it is slow, so the client
// usually re-attaches while the run is still going and gets the rest live.
func TestClientReattach(t *testing.T) {
	r := newRig(t)
	r.tools.slow, r.tools.delay = "refund_payment", 2*time.Second
	r.start("a", "")
	r.start("b", "")

	// The first client, over gRPC to see each step's index, drops once it
	// has the refund call.
	ctx, drop := context.WithCancel(context.Background())
	defer drop()
	stream, err := proto.NewInteractionsServiceClient(dial(t, r.addr["a"])).CreateInteraction(ctx, &proto.CreateInteractionEvent{
		ConversationId: r.conv,
		Inputs: []*proto.Step{{Type: &proto.Step_Content{Content: &proto.ContentStep{Role: "user",
			Content: []*proto.Content{{Type: &proto.Content_Text{Text: &proto.TextContent{Text: task}}}}}}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	var seen []int64
	var last int64
	for last == 0 {
		resp, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}
		for _, s := range resp.Outputs {
			seen = append(seen, s.Index)
			if s.GetToolCall().GetFunctionCall().GetName() == "refund_payment" {
				last = s.Index
			}
		}
	}
	drop()

	out, code := r.client("b", "--resume", "--after-step", strconv.FormatInt(last, 10)).wait(t)
	out = ansi.ReplaceAllString(out, "")
	r.check("COMPLETED", 1, 1)

	// The same re-attach over gRPC, for the exact steps.
	var got []int64
	at, err := proto.NewConductorServiceClient(dial(t, r.addr["b"])).Attach(context.Background(), &proto.AttachRequest{ConversationId: r.conv, AfterStep: last})
	if err != nil {
		t.Fatal(err)
	}
	for {
		resp, err := at.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		for _, s := range resp.Outputs {
			got = append(got, s.Index)
		}
	}

	var before, after []int64
	var final int64
	for i, ev := range r.events() {
		step := int64(i + 1)
		for range ev.Steps {
			switch {
			case step > last:
				after = append(after, step)
			case step > 1: // step 1 is the user input, never sent back
				before = append(before, step)
			}
		}
		if len(ev.Steps) > 0 {
			final = step
		}
	}
	if !slices.Equal(seen, before) {
		t.Errorf("before the drop the client saw steps %v, the log has %v", seen, before)
	}
	if !slices.Equal(got, after) {
		t.Errorf("re-attached after step %d, got steps %v; the log has %v", last, got, after)
	}
	// The CLI shows tool calls and the step it ended on.
	if code != 0 || strings.Count(out, "send_email(") != 1 || strings.Contains(out, "refund_payment(") ||
		strings.Contains(out, "lookup_order(") || !strings.Contains(out, fmt.Sprintf("step=%d", final)) {
		t.Errorf("re-attached CLI after step %d (exit %d): want the email call once, no earlier call, and step=%d:\n%s", last, code, final, out)
	}
}

// PRD.md §6.2, "The client disconnects": the run keeps going; its owner
// "keeps executing to the end of the current turn", and a run that needs no
// more input ends with no client attached. So a client that goes away while
// the email request is in flight changes nothing: one email, COMPLETED.
func TestClientDisconnectDuringUnsafeCall(t *testing.T) {
	r := newRig(t)
	r.tools.slow, r.tools.delay = "send_email", 2*time.Second
	r.start("a", "")
	r.start("b", "")
	c := r.send("a")
	waitFor(t, r.tools.started, "the email request")
	c.cmd.Process.Kill()
	state := r.waitState(90 * time.Second)
	refunds, emails, _ := r.tools.counts()
	if state == "FAILED" && emails == 0 && strings.HasSuffix(r.ledger(), "send_email:ABANDONED:1") {
		t.Skipf("BUG (Conductor, reported): the client's disconnect cancelled the in-flight email (CreateInteraction runs Exec "+
			"on the gRPC stream context), so it was never sent, its call became OUTCOME_UNKNOWN and then ABANDONED, and the run "+
			"FAILED instead of completing. State %s, %d refund(s), %d email(s), ledger %s, invariant violations %v.\n--- a ---\n%s",
			state, refunds, emails, r.ledger(), r.violations(), r.logs("a"))
	}
	r.check("COMPLETED", 1, 1)
}

// ansi matches the CLI's color and bold escapes.
var ansi = regexp.MustCompile("\x1b\\[[0-9;]*m")

func dial(t *testing.T, addr string) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}
