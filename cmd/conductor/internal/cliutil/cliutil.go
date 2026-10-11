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
	"fmt"
	"os"

	"github.com/simondxu/conductor/internal/config"
	"github.com/simondxu/conductor/internal/controller"
	"github.com/simondxu/conductor/internal/controller/eventlog"
	"github.com/simondxu/conductor/internal/harness"
	"github.com/simondxu/conductor/internal/harness/antigravity"
	"github.com/simondxu/conductor/internal/harness/antigravityinteractions"
	"github.com/simondxu/conductor/internal/harness/fake"
	"github.com/simondxu/conductor/internal/harness/remote"
	"github.com/simondxu/conductor/internal/harness/substrate"
	"github.com/simondxu/conductor/internal/skills"
	"github.com/simondxu/conductor/internal/skills/geminienterprise"
	"github.com/simondxu/conductor/internal/skills/local"
)

// Controller is the active controller type for this build.
type Controller = *controller.Controller

// ExecHandler is the handler type accepted by Controller.Exec.
type ExecHandler = controller.ExecHandler

// Config is the configuration type for this build.
type Config = config.Config

// LoadFromFile loads configuration from a YAML file.
func LoadFromFile(path string) (*Config, error) {
	return config.LoadFromFile(path)
}

// DefaultConfig returns a configuration with default values set.
func DefaultConfig() *Config {
	return config.DefaultConfig()
}

// NewControllerFromConfig creates a controller.Controller instance based on the provided configuration.
func NewControllerFromConfig(ctx context.Context, cfg *Config) (*controller.Controller, error) {
	reg := controller.NewRegistry()

	// CONDUCTOR_SUBSTRATE selects how built-in harnesses run: locally (unset) or as
	// substrate actors ("1").
	substrateMode := os.Getenv("CONDUCTOR_SUBSTRATE") == "1"

	// Validate config before local-mode antigravity.New forks the Python
	// sidecar, so a config error surfaces without spawning a subprocess.
	// Validate config before local-mode antigravity.New forks the Python
	// sidecar, so a config error surfaces without spawning a subprocess.
	if len(cfg.Registry.Substrate) > 0 && !substrateMode {
		return nil, fmt.Errorf("custom substrate harnesses require CONDUCTOR_SUBSTRATE=1")
	}

	var defaultHarnessID string
	var err error

	// Resolve skills once, up front (skills config is top-level and
	// harness-agnostic; each actor runs a single harness that consumes the
	// resulting folder). Two parallel sources feed the same result:
	//   - Registries: fetched from the Gemini Enterprise Skill Registry and
	//     materialized to their target_dir.
	//   - Local: directories already on disk, used as-is.
	// Both are unconditional when configured and fail-safe: a source error
	// degrades capability but never blocks harness creation. The interactions
	// harness (no SKILLS_DIR concept) is told where the skills are via a pointer
	// appended to its system instruction. Only the local process path resolves
	// skills; substrate/pod does not yet read ax.yaml.
	//
	// TODO(joycel): wire the Antigravity SDK harness too. It discovers skills via
	// SKILLS_DIR, so its SKILLS_DIR needs to be pointed at the resolved skill
	// directories (registry target_dir(s) and/or local path(s)); currently only
	// the interactions harness is fully wired.
	var skillsPointer string
	if !substrateMode {
		var groups []skills.Group
		groups = append(groups, geminienterprise.Materialize(ctx, cfg.Skills)...)
		groups = append(groups, local.Discover(cfg.Skills)...)
		skillsPointer = antigravityinteractions.SkillsSystemInstruction(groups)
	}

	// Built-in Antigravity harness.
	antigravityDisabled := cfg.Antigravity.Disabled || cfg.Registry.Antigravity.Disabled
	var antigravityHarness harness.Harness
	switch {
	case antigravityDisabled:
		// Not registered.
	case !substrateMode:
		address := cfg.Antigravity.Endpoint
		if address == "" {
			address = cfg.Registry.Antigravity.Endpoint
		}
		if address == "" {
			address = "127.0.0.1:50053"
		}
		// Local mode: the harness owns the Python sidecar. AX owns the
		// trajectory storage path (a harness implementation detail), so it's
		// derived internally rather than exposed in ax.yaml.
		stateDir, sErr := antigravity.DefaultStateDir()
		if sErr != nil {
			return nil, fmt.Errorf("antigravity harness: %w", sErr)
		}
		antigravityHarness, err = antigravity.New(ctx, address, stateDir, true)
		if err != nil {
			return nil, fmt.Errorf("antigravity harness: %w", err)
		}
	default:
		antigravityHarness, err = substrate.New(config.AntigravityHarnessID, "", "", config.AntigravityHarnessTemplate, 80)
		if err != nil {
			return nil, fmt.Errorf("antigravity harness: %w", err)
		}
	}
	if antigravityHarness != nil {
		if err := reg.RegisterHarness(config.AntigravityHarnessID, antigravityHarness); err != nil {
			return nil, fmt.Errorf("register antigravity harness: %w", err)
		}
	}
	if cfg.Antigravity.Default || cfg.Registry.Antigravity.Default {
		defaultHarnessID = config.AntigravityHarnessID
	}

	// Built-in Antigravity Interactions harness.
	var antigravityInteractionsHarness harness.Harness
	if !substrateMode {
		aiCfg := cfg.Registry.AntigravityInteractions
		agent := aiCfg.Agent
		if agent == "" {
			agent = antigravityinteractions.DefaultAgent
		}
		// AX owns the resume-cursor path (a harness implementation detail), so
		// it's derived internally rather than exposed in ax.yaml.
		stateDir, sErr := antigravityinteractions.DefaultStateDir()
		if sErr != nil {
			return nil, fmt.Errorf("antigravity-interactions harness: %w", sErr)
		}
		// WorkDir is the agent's working directory, authoritative for built-in env
		// tools (see AntigravityInteractionsConfig.WorkDir). Mirrors the substrate
		// path (cmd/ax harness): sourced from CONDUCTOR_HARNESS_WORKDIR, empty falls back
		// to the process cwd.
		workDir := os.Getenv("CONDUCTOR_HARNESS_WORKDIR")
		// skillsPointer was built once, up front, from the top-level skills
		// config (see above). Append it, plus the workspace pointer, to any
		// configured system instruction.
		systemInstruction := antigravityinteractions.JoinSystemInstruction(
			antigravityinteractions.JoinSystemInstruction(aiCfg.SystemInstruction, skillsPointer),
			antigravityinteractions.WorkspaceSystemInstruction(workDir),
		)
		antigravityInteractionsHarness, err = antigravityinteractions.New(antigravityinteractions.AntigravityInteractionsConfig{
			Agent:             agent,
			SystemInstruction: systemInstruction,
			StateDir:          stateDir,
			WorkDir:           workDir,
		})
	} else {
		antigravityInteractionsHarness, err = substrate.New(config.AntigravityInteractionsHarnessID, "", "", config.AntigravityInteractionsTemplate, 80)
	}
	if err != nil {
		return nil, fmt.Errorf("antigravity-interactions harness: %w", err)
	}
	if err := reg.RegisterHarness(config.AntigravityInteractionsHarnessID, antigravityInteractionsHarness); err != nil {
		return nil, fmt.Errorf("register antigravity-interactions harness: %w", err)
	}
	if cfg.Registry.AntigravityInteractions.Default {
		defaultHarnessID = config.AntigravityInteractionsHarnessID
	}

	// Built-in fake harness: deterministic, zero-key, recovery:full.
	fakeHarness, err := newFakeHarness(ctx, cfg.Registry.Fake)
	if err != nil {
		return nil, fmt.Errorf("fake harness: %w", err)
	}
	if err := reg.Register(config.FakeHarnessID, fakeHarness, controller.RecoveryFull); err != nil {
		return nil, fmt.Errorf("register fake harness: %w", err)
	}
	if cfg.Registry.Fake.Default {
		defaultHarnessID = config.FakeHarnessID
	}

	for _, sc := range cfg.Registry.Substrate {
		h, err := sc.NewHarness("")
		if err != nil {
			return nil, fmt.Errorf("substrate harness %q: %w", sc.ID, err)
		}
		if err := reg.RegisterHarness(sc.ID, h); err != nil {
			return nil, fmt.Errorf("register substrate harness %q: %w", sc.ID, err)
		}
		if sc.Default {
			defaultHarnessID = sc.ID
		}
	}

	// Antigravity is the default harness by default if no other harness set
	// Default: true, or fake when antigravity is disabled.
	if defaultHarnessID == "" {
		defaultHarnessID = config.AntigravityHarnessID
		if antigravityDisabled {
			defaultHarnessID = config.FakeHarnessID
		}
	}

	// Set the default harness.
	if err := reg.SetDefaultHarness(defaultHarnessID); err != nil {
		return nil, fmt.Errorf("set default harness %q: %w", defaultHarnessID, err)
	}

	return controller.New(ctx, controller.Config{
		Registry: reg,
		EventLogBuilder: func() (eventlog.EventLog, error) {
			if cfg.EventLog.PostgresConfig.DSN != "" {
				dsn := os.ExpandEnv(cfg.EventLog.PostgresConfig.DSN)
				if dsn == "" {
					return nil, fmt.Errorf("eventlog: postgres dsn %q expanded to empty", cfg.EventLog.PostgresConfig.DSN)
				}
				return eventlog.OpenPostgresEventLog(dsn)
			}
			return eventlog.OpenSQLiteEventLog(cfg.EventLog.SQLiteConfig.Filename)
		},
	})
}

// newFakeHarness builds the fake harness: a client of a remote
// `conductor harness fake` when Endpoint is set, in-process otherwise.
func newFakeHarness(ctx context.Context, c config.FakeHarnessConfig) (harness.Harness, error) {
	if c.Endpoint != "" {
		return remote.NewHarness(c.Endpoint, config.FakeHarnessID)
	}
	if c.EffectsDSN == "" {
		return fake.New(&fake.MemoryEffects{}), nil
	}
	effects, err := fake.OpenPostgresEffects(ctx, os.ExpandEnv(c.EffectsDSN))
	if err != nil {
		return nil, err
	}
	return fake.New(effects), nil
}
