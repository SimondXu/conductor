// Copyright 2026 Google LLC
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

package cliutil

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/simondxu/conductor/internal/config"
	"github.com/simondxu/conductor/internal/harness/harnesstest"
	"github.com/simondxu/conductor/proto"
)

func TestNewControllerFromConfig_BuiltinSubstrate(t *testing.T) {
	t.Setenv("CONDUCTOR_SUBSTRATE", "1")

	cfg := &config.Config{
		EventLog: config.EventLogConfig{
			SQLiteConfig: config.SQLiteConfig{
				Filename: filepath.Join(t.TempDir(), "log.sqlite"),
			},
		},
		Registry: config.RegistryConfig{
			Antigravity: config.AntigravityHarnessConfig{
				Default: true,
			},
		},
	}

	c, err := NewControllerFromConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewControllerFromConfig: %v", err)
	}
	if c == nil {
		t.Fatal("expected non-nil controller")
	}
	c.Close()
}

func TestNewControllerFromConfig_InteractionsSubstrate(t *testing.T) {
	t.Setenv("CONDUCTOR_SUBSTRATE", "1")

	cfg := &config.Config{
		EventLog: config.EventLogConfig{
			SQLiteConfig: config.SQLiteConfig{
				Filename: filepath.Join(t.TempDir(), "log.sqlite"),
			},
		},
		Registry: config.RegistryConfig{
			Antigravity: config.AntigravityHarnessConfig{Default: true},
		},
	}

	c, err := NewControllerFromConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewControllerFromConfig: %v", err)
	}
	if c == nil {
		t.Fatal("expected non-nil controller")
	}
	defer c.Close()

	if _, err := c.Registry().Harness(config.AntigravityInteractionsHarnessID); err != nil {
		t.Errorf("interactions harness not registered in substrate mode: %v", err)
	}
}

func TestNewControllerFromConfig_CustomHarnessRequiresSubstrateMode(t *testing.T) {
	t.Setenv("CONDUCTOR_SUBSTRATE", "")

	cfg := &config.Config{
		EventLog: config.EventLogConfig{
			SQLiteConfig: config.SQLiteConfig{
				Filename: filepath.Join(t.TempDir(), "log.sqlite"),
			},
		},
		Registry: config.RegistryConfig{
			Substrate: []config.SubstrateHarnessConfig{
				{ID: "custom", Namespace: "team-ns", Template: "custom-template"},
			},
		},
	}

	_, err := NewControllerFromConfig(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected error for custom substrate harness without CONDUCTOR_SUBSTRATE=1, got nil")
	}
	if !strings.Contains(err.Error(), "CONDUCTOR_SUBSTRATE=1") {
		t.Errorf("expected error to mention CONDUCTOR_SUBSTRATE=1, got: %v", err)
	}
}

func TestNewControllerFromConfig_CustomHarnessInSubstrateMode(t *testing.T) {
	t.Setenv("CONDUCTOR_SUBSTRATE", "1")

	cfg := &config.Config{
		EventLog: config.EventLogConfig{
			SQLiteConfig: config.SQLiteConfig{
				Filename: filepath.Join(t.TempDir(), "log.sqlite"),
			},
		},
		Registry: config.RegistryConfig{
			Substrate: []config.SubstrateHarnessConfig{
				{ID: "custom", Namespace: "team-ns", Template: "custom-template"},
			},
		},
	}

	c, err := NewControllerFromConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewControllerFromConfig: %v", err)
	}
	if c == nil {
		t.Fatal("expected non-nil controller")
	}
	c.Close()
}

// With antigravity disabled, a controller builds without Python, defaults to
// the fake harness, and runs its script to completion on SQLite.
func TestNewControllerFromConfig_FakeWithoutPython(t *testing.T) {
	t.Setenv("CONDUCTOR_SUBSTRATE", "")
	t.Setenv("CONDUCTOR_DURABLE_DIR", t.TempDir())

	cfg := &config.Config{
		EventLog: config.EventLogConfig{
			SQLiteConfig: config.SQLiteConfig{Filename: filepath.Join(t.TempDir(), "log.sqlite")},
		},
		Registry: config.RegistryConfig{
			Antigravity: config.AntigravityHarnessConfig{Disabled: true},
		},
	}
	c, err := NewControllerFromConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewControllerFromConfig: %v", err)
	}
	defer c.Close()

	var outputs int
	err = c.Exec(context.Background(), &proto.CreateInteractionEvent{
		ConversationId: "fake-local",
		Inputs:         []*proto.Step{harnesstest.UserStep("go")},
	}, func(r *proto.CreateInteractionResponse) error {
		outputs += len(r.Outputs)
		return nil
	})
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if outputs != 6 {
		t.Errorf("outputs = %d, want 6", outputs)
	}
}
