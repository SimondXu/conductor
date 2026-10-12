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

package controller

import (
	"fmt"
	"sync"

	"github.com/simondxu/conductor/internal/controller/toolledger"
	"github.com/simondxu/conductor/internal/harness"
)

// Recovery declares whether Conductor can recover a harness's conversations
// (docs/conductor/correctness.md §0.3).
type Recovery string

const (
	// RecoveryNone: the harness runs its own tool loop; Conductor cannot
	// rebuild it from the log.
	RecoveryNone Recovery = "none"
	// RecoveryFull: Conductor owns the tool loop and drives the harness one
	// turn at a time (docs/conductor/ARCHITECTURE.md §1.4).
	RecoveryFull Recovery = "full"
)

// Registry manages a collection of harnesses.
type Registry struct {
	mu             sync.RWMutex
	harnesses      map[string]harness.Harness
	recovery       map[string]Recovery
	tools          map[string]map[string]toolledger.Class
	defaultHarness string
}

// NewRegistry creates a new harness registry.
func NewRegistry() *Registry {
	return &Registry{
		harnesses: make(map[string]harness.Harness),
		recovery:  make(map[string]Recovery),
		tools:     make(map[string]map[string]toolledger.Class),
	}
}

// RegisterHarness registers a harness under the given id with RecoveryNone.
func (r *Registry) RegisterHarness(id string, h harness.Harness) error {
	return r.Register(id, h, RecoveryNone)
}

// Register registers a harness under the given id with its recovery class.
func (r *Registry) Register(id string, h harness.Harness, rec Recovery) error {
	if err := validateID(id); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.harnesses[id]; ok {
		return fmt.Errorf("harness %q already registered", id)
	}
	r.harnesses[id] = h
	r.recovery[id] = rec
	return nil
}

// Recovery returns the recovery class of a registered harness.
func (r *Registry) Recovery(id string) Recovery {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.recovery[id]
}

// Harness retrieves a harness by id.
func (r *Registry) Harness(id string) (harness.Harness, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.harnesses[id]
	if !ok {
		return nil, fmt.Errorf("harness %s not found", id)
	}
	return h, nil
}

// DeclareTools records the class of each of a harness's tools.
func (r *Registry) DeclareTools(id string, classes map[string]toolledger.Class) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tools[id] = classes
}

// ToolClass returns a tool's declared class. An undeclared tool is UNSAFE:
// classes are never inferred, and that is the only safe assumption.
func (r *Registry) ToolClass(id, tool string) toolledger.Class {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if c, ok := r.tools[id][tool]; ok {
		return c
	}
	return toolledger.Unsafe
}

// SetDefaultHarness marks a registered harness id as the default.
func (r *Registry) SetDefaultHarness(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.harnesses[id]; !ok {
		return fmt.Errorf("harness %q not found", id)
	}
	r.defaultHarness = id
	return nil
}

// Close releases resources held by the registry.
func (r *Registry) Close() error {
	return nil
}
