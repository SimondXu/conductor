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

package chaos

import (
	"context"
	"testing"
)

func TestArmAndAt(t *testing.T) {
	t.Cleanup(func() { _ = Arm("", nil) })
	for _, bad := range []string{"nowhere", "model_started:charge_card"} {
		if err := Arm(bad, nil); err == nil {
			t.Errorf("Arm(%q) accepted", bad)
		}
	}
	var exits []int
	if err := Arm("tool_started:send_email", func(code int) { exits = append(exits, code) }); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	At(ctx, ToolStarted, "charge_card")
	At(ctx, ToolPlanned, "send_email")
	if len(exits) != 0 {
		t.Fatalf("fired at the wrong point: %v", exits)
	}
	At(ctx, ToolStarted, "send_email")
	if len(exits) != 1 || exits[0] != ExitCode {
		t.Fatalf("exits = %v, want [%d]", exits, ExitCode)
	}
}
