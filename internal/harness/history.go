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

package harness

import "github.com/simondxu/conductor/proto"

// UnansweredToolCalls returns, in history order, the tool calls in history
// that have no ToolResultStep with a matching call id. Under the owned-loop
// turn protocol (docs/conductor/ARCHITECTURE.md §1.4) a non-empty result
// means the next turn is a tool turn.
func UnansweredToolCalls(history []*proto.Step) []*proto.ToolCallStep {
	answered := map[string]bool{}
	for _, s := range history {
		if r := s.GetToolResult(); r != nil {
			answered[r.GetCallId()] = true
		}
	}
	var out []*proto.ToolCallStep
	for _, s := range history {
		if c := s.GetToolCall(); c != nil && !answered[c.GetId()] {
			out = append(out, c)
		}
	}
	return out
}

// NeedsTurn reports whether an owned-loop conversation whose history is
// history has more work to do: it ends in user input, a tool call, or a tool
// result.
func NeedsTurn(history []*proto.Step) bool {
	if len(history) == 0 {
		return false
	}
	last := history[len(history)-1]
	switch {
	case last.GetToolCall() != nil, last.GetToolResult() != nil:
		return true
	case last.GetContent() != nil:
		return last.GetContent().GetRole() == "user"
	}
	return false
}
