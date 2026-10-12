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

package controller

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/simondxu/conductor/internal/controller/eventlog"
	"github.com/simondxu/conductor/internal/controller/eventlog/eventlogtest"
	"github.com/simondxu/conductor/internal/harness/harnesstest"
	"github.com/simondxu/conductor/proto"
)

// Attach sends every step after after_step exactly once, in order, catch-up
// first and then live, and returns at the terminal state.
func TestAttach_CatchUpThenLive(t *testing.T) {
	log := &eventlogtest.MemoryEventLog{}
	c, err := New(context.Background(), Config{
		Registry:        NewRegistry(),
		EventLogBuilder: func() (eventlog.EventLog, error) { return log, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	add := func(state proto.State, steps ...*proto.Step) {
		if _, err := log.Append(ctx, &proto.StepEvent{ConversationId: "conv", State: state, Steps: steps}, 0); err != nil {
			t.Fatal(err)
		}
	}
	add(proto.State_STATE_PENDING, harnesstest.UserStep("go")) // 1
	add(proto.State_STATE_PENDING, harnesstest.UserStep("a"))  // 2
	add(proto.State_STATE_PENDING, harnesstest.UserStep("b"))  // 3

	var mu sync.Mutex
	var got []int64
	pending := 0
	done := make(chan error)
	go func() {
		done <- c.Attach(ctx, "conv", 1, func(r *proto.CreateInteractionResponse) error {
			mu.Lock()
			defer mu.Unlock()
			for _, s := range r.Outputs {
				got = append(got, s.Index)
			}
			return nil
		}, func() { pending++ })
	}()
	time.Sleep(3 * attachPoll)
	add(proto.State_STATE_PENDING, harnesstest.UserStep("c")) // 4
	add(proto.State_STATE_COMPLETED)                          // 5, no steps
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Attach did not return at the terminal state")
	}
	if want := []int64{2, 3, 4}; !slices.Equal(got, want) || pending != 1 {
		t.Fatalf("got steps %v with %d onPending calls, want %v and 1", got, pending, want)
	}

	for _, after := range []int64{0, 9} {
		conv := "conv"
		if after == 0 {
			conv = "missing"
		}
		if err := c.Attach(ctx, conv, after, func(*proto.CreateInteractionResponse) error { return nil }, nil); !errors.Is(err, ErrNoConversation) {
			t.Errorf("Attach(%s, %d) = %v, want ErrNoConversation", conv, after, err)
		}
	}
}
