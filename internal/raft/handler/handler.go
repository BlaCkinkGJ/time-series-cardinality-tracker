// Copyright 2026 BlaCkinkGJ
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

// Package handler holds the Raft command registry and one file per
// command type. Each command exposes a Register* function that adds
// itself to a Registry; production code calls DefaultRegistry, tests
// use NewRegistry for isolation.
package handler

import (
	"errors"
	"fmt"
	"sync"

	pb "github.com/yourorg/cardinality-tracker/gen/cardinality/v1"
)

var (
	// ErrUnknownCommand is returned when no handler is registered for cmd.Type.
	ErrUnknownCommand = errors.New("raft: unknown command type")
	// ErrBadPayload is returned when a handler cannot decode cmd.Payload.
	ErrBadPayload = errors.New("raft: bad payload")
	// ErrUnknownAlgorithm is returned when a MERGE_SKETCH references an
	// algorithm name the engine doesn't know how to parse.
	// ponytail: only "hll" is registered today. New algorithms (e.g.
	// "bitmap") are added by extending Adder.Merge's accepted algo names;
	// no handler change required.
	ErrUnknownAlgorithm = errors.New("raft: unknown algorithm")
)

// Adder is the minimum engine surface a handler needs. Handlers never
// touch a concrete engine type — they accept any Adder. Today the only
// implementation is hllAdder wrapping *hll.Engine; once the engine
// gains bitmap support and per-group algorithm override,
// *cardinality.Engine will satisfy this directly and hllAdder goes away.
type Adder interface {
	// Add inserts id into group's sketch, creating the group if absent.
	Add(group string, id uint64) error
	// Merge unions sketch (opaque bytes, parsed by Adder using algoName)
	// into group's existing sketch; if the group does not exist, the
	// engine is expected to seed it from the provided sketch.
	Merge(group string, algoName string, sketch []byte) error
}

// Handler applies a single command type to an Adder.
type Handler func(cmd *pb.Command, apply Adder) error

// Registry maps command type strings to their Handler. Construct with
// NewRegistry for an empty registry or DefaultRegistry for one
// preloaded with every built-in command type. Registry is safe for
// concurrent Register and Dispatch; each Node should own its own.
type Registry struct {
	mu sync.RWMutex
	m  map[string]Handler
}

// NewRegistry returns an empty Registry.
func NewRegistry() *Registry {
	return &Registry{m: make(map[string]Handler)}
}

// DefaultRegistry returns a Registry preloaded with every built-in
// command handler. This is the registry production Nodes use; tests
// typically use NewRegistry plus individual Register* calls for
// isolation.
func DefaultRegistry() *Registry {
	r := NewRegistry()
	RegisterAdd(r)
	RegisterBatch(r)
	RegisterMerge(r)
	return r
}

// Register associates a command type with its handler. Assumes each
// type is registered at most once per Registry.
func (r *Registry) Register(t string, h Handler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.m[t] = h
}

// Dispatch routes cmd to its registered handler.
func (r *Registry) Dispatch(cmd *pb.Command, apply Adder) error {
	if cmd == nil {
		return fmt.Errorf("%w: nil command", ErrUnknownCommand)
	}
	r.mu.RLock()
	h, ok := r.m[cmd.Type]
	r.mu.RUnlock()
	if !ok {
		return fmt.Errorf("%w: %q", ErrUnknownCommand, cmd.Type)
	}
	if apply == nil {
		return errors.New("raft: nil adder")
	}
	return h(cmd, apply)
}
