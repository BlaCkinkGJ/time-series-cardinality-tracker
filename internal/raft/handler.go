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

package raft

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
	// ponytail: only "hll" is registered today; per-group algorithm
	// override and the cardinality.Algorithm registry arrive in #13.
	ErrUnknownAlgorithm = errors.New("raft: unknown algorithm")
)

// Adder is the minimum engine surface a handler needs. Both *hll.Engine
// (current) and *cardinality.Engine (after #13) satisfy this directly or
// through a small adapter.
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

var handlers struct {
	sync.RWMutex
	m map[string]Handler
}

// RegisterHandler associates a command type with its handler. Intended for
// package init() registrations; safe for concurrent use but assumes each
// type is registered at most once.
func RegisterHandler(t string, h Handler) {
	handlers.Lock()
	defer handlers.Unlock()
	if handlers.m == nil {
		handlers.m = make(map[string]Handler)
	}
	handlers.m[t] = h
}

// dispatch routes cmd to its registered handler.
func dispatch(cmd *pb.Command, apply Adder) error {
	if cmd == nil {
		return fmt.Errorf("%w: nil command", ErrUnknownCommand)
	}
	handlers.RLock()
	h, ok := handlers.m[cmd.Type]
	handlers.RUnlock()
	if !ok {
		return fmt.Errorf("%w: %q", ErrUnknownCommand, cmd.Type)
	}
	if apply == nil {
		return errors.New("raft: nil adder")
	}
	return h(cmd, apply)
}
