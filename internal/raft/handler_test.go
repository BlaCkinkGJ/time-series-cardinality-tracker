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
	"encoding/binary"
	"errors"
	"sync"
	"testing"

	pb "github.com/yourorg/cardinality-tracker/gen/cardinality/v1"
)

// fakeAdder records every Add call. Concurrency-safe for parallel tests.
type fakeAdder struct {
	mu     sync.Mutex
	ids    map[string][]uint64
	errOn  map[string]error
	failOn map[string]bool // group → next Add returns nil adder error
}

func newFakeAdder() *fakeAdder {
	return &fakeAdder{
		ids:    map[string][]uint64{},
		errOn:  map[string]error{},
		failOn: map[string]bool{},
	}
}

func (f *fakeAdder) Add(group string, id uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err, ok := f.errOn[group]; ok {
		return err
	}
	f.ids[group] = append(f.ids[group], id)
	return nil
}

func (f *fakeAdder) count(group string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.ids[group])
}

// registerScoped registers h for tp and arranges cleanup to remove only that
// registration when the test ends. init()-registered handlers survive.
func registerScoped(t *testing.T, tp string, h Handler) {
	t.Helper()
	RegisterHandler(tp, h)
	t.Cleanup(func() {
		handlers.Lock()
		delete(handlers.m, tp)
		handlers.Unlock()
	})
}

func TestRegisterHandler_GetHandler(t *testing.T) {
	const tp = "TEST_REGISTER_GET"
	registerScoped(t, tp, func(cmd *pb.Command, apply Adder) error { return nil })
	got, ok := lookupHandler(tp)
	if !ok {
		t.Fatalf("expected handler for %q", tp)
	}
	if got == nil {
		t.Fatalf("handler is nil")
	}
}

func TestDispatch_Unknown(t *testing.T) {
	cmd := &pb.Command{Type: "NEVER_REGISTERED"}
	err := dispatch(cmd, newFakeAdder())
	if err == nil {
		t.Fatal("expected error for unknown type")
	}
	if !errors.Is(err, ErrUnknownCommand) {
		t.Fatalf("expected ErrUnknownCommand, got %v", err)
	}
}

func TestApplyAdd_ValidPayload(t *testing.T) {
	registerScoped(t, "ADD_VALID", applyAdd)
	a := newFakeAdder()
	cmd := &pb.Command{Type: "ADD_VALID", Group: "g", Payload: binary.AppendUvarint(nil, 42)}
	if err := dispatch(cmd, a); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if got := a.count("g"); got != 1 {
		t.Fatalf("count = %d, want 1", got)
	}
}

func TestApplyAdd_Multiple(t *testing.T) {
	registerScoped(t, "ADD_MULTI", applyAdd)
	a := newFakeAdder()
	for _, id := range []uint64{7, 11} {
		cmd := &pb.Command{Type: "ADD_MULTI", Group: "g", Payload: binary.AppendUvarint(nil, id)}
		if err := dispatch(cmd, a); err != nil {
			t.Fatalf("dispatch(%d): %v", id, err)
		}
	}
	if got := a.count("g"); got != 2 {
		t.Fatalf("count = %d, want 2", got)
	}
}

func TestApplyAdd_EmptyPayload(t *testing.T) {
	registerScoped(t, "ADD_EMPTY", applyAdd)
	a := newFakeAdder()
	err := dispatch(&pb.Command{Type: "ADD_EMPTY", Group: "g", Payload: nil}, a)
	if !errors.Is(err, ErrBadPayload) {
		t.Fatalf("expected ErrBadPayload, got %v", err)
	}
	if a.count("g") != 0 {
		t.Fatalf("adder should not have been called")
	}
}

func TestApplyAdd_TruncatedVarint(t *testing.T) {
	registerScoped(t, "ADD_TRUNC", applyAdd)
	a := newFakeAdder()
	// First byte of a varint with continuation bit set, no trailing bytes.
	err := dispatch(&pb.Command{Type: "ADD_TRUNC", Group: "g", Payload: []byte{0x80}}, a)
	if !errors.Is(err, ErrBadPayload) {
		t.Fatalf("expected ErrBadPayload, got %v", err)
	}
}

func TestApplyAdd_ExtraBytes(t *testing.T) {
	registerScoped(t, "ADD_EXTRA", applyAdd)
	a := newFakeAdder()
	// 1-byte varint(7) + 1 trailing byte
	err := dispatch(&pb.Command{Type: "ADD_EXTRA", Group: "g", Payload: []byte{0x07, 0x00}}, a)
	if !errors.Is(err, ErrBadPayload) {
		t.Fatalf("expected ErrBadPayload, got %v", err)
	}
}

func TestApplyAdd_NilAdder(t *testing.T) {
	registerScoped(t, "ADD_NIL_ADDER", applyAdd)
	cmd := &pb.Command{Type: "ADD_NIL_ADDER", Group: "g", Payload: binary.AppendUvarint(nil, 1)}
	err := dispatch(cmd, nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if errors.Is(err, ErrUnknownCommand) {
		t.Fatalf("expected non-unknown error, got %v", err)
	}
}

func TestDispatch_NoHandlerRegistered(t *testing.T) {
	cmd := &pb.Command{Type: "TOTALLY_UNKNOWN"}
	err := dispatch(cmd, newFakeAdder())
	if err == nil {
		t.Fatal("expected ErrUnknownCommand")
	}
	if !errors.Is(err, ErrUnknownCommand) {
		t.Fatalf("got %v, want ErrUnknownCommand", err)
	}
}

func TestApplyAdd_AdderErrorPropagates(t *testing.T) {
	registerScoped(t, "ADD_ADDER_ERR", applyAdd)
	a := newFakeAdder()
	a.errOn["g"] = errors.New("boom")
	cmd := &pb.Command{Type: "ADD_ADDER_ERR", Group: "g", Payload: binary.AppendUvarint(nil, 99)}
	err := dispatch(cmd, a)
	if err == nil || err.Error() != "boom" {
		t.Fatalf("expected boom, got %v", err)
	}
}

func TestInitRegistersAdd(t *testing.T) {
	// The init() in handler_add.go registers applyAdd under TypeAdd.
	// Test isolation must not strip this registration.
	if _, ok := lookupHandler(TypeAdd); !ok {
		t.Fatalf("TypeAdd handler not registered by init()")
	}
}

// lookupHandler exposes a read-only accessor for tests; package-private.
func lookupHandler(t string) (Handler, bool) {
	handlers.RLock()
	defer handlers.RUnlock()
	h, ok := handlers.m[t]
	return h, ok
}
