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
	"fmt"

	"github.com/simondxu/conductor/proto"
	"google.golang.org/protobuf/encoding/protojson"
)

// runState is what the event log alone says about a conversation.
type runState struct {
	State     proto.State
	HarnessID string
	// AgentConfig is the last logged agent_config as JSON, nil if none was.
	AgentConfig []byte
	// History is every logged step, in order.
	History []*proto.Step
}

// rebuild derives a conversation's state from its events. It is pure (I6):
// the same events always give the same state.
func rebuild(events []*proto.StepEvent) (runState, error) {
	var st runState
	for _, ev := range events {
		if st.HarnessID == "" && ev.AgentId != "" {
			st.HarnessID = ev.AgentId
		}
		if ev.State != proto.State_STATE_UNSPECIFIED {
			st.State = ev.State
		}
		if ev.AgentConfig != nil {
			cfg, err := protojson.Marshal(ev.AgentConfig)
			if err != nil {
				return runState{}, fmt.Errorf("encoding logged agent_config: %w", err)
			}
			st.AgentConfig = cfg
		}
		st.History = append(st.History, ev.Steps...)
	}
	return st, nil
}

// Rebuild is rebuild for checkers outside the package (I6): the state and
// the history the log alone gives a conversation.
func Rebuild(events []*proto.StepEvent) (proto.State, []*proto.Step, error) {
	st, err := rebuild(events)
	return st.State, st.History, err
}
