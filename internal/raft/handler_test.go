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
	mu        sync.Mutex
	ids       map[string][]uint64
	sketches  map[string][]string // group → ordered list of merged algo names
	errOn     map[string]error
	errOnAlgo map[string]error // algo name → returned error from Merge
}

func newFakeAdder() *fakeAdder {
	return &fakeAdder{
		ids:       map[string][]uint64{},
		sketches:  map[string][]string{},
		errOn:     map[string]error{},
		errOnAlgo: map[string]error{},
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

func (f *fakeAdder) Merge(group, algoName string, sketch []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err, ok := f.errOn[group]; ok {
		return err
	}
	if err, ok := f.errOnAlgo[algoName]; ok {
		return err
	}
	f.sketches[group] = append(f.sketches[group], algoName)
	return nil
}

func (f *fakeAdder) count(group string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.ids[group])
}

func (f *fakeAdder) sketchCount(group string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.sketches[group])
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

// --- BATCH_ADD ---

func TestApplyBatchAdd_Empty(t *testing.T) {
	registerScoped(t, "BATCH_EMPTY", applyBatchAdd)
	a := newFakeAdder()
	cmd := &pb.Command{
		Type:    "BATCH_EMPTY",
		Group:   "g",
		Payload: binary.AppendUvarint(nil, 0),
	}
	if err := dispatch(cmd, a); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if got := a.count("g"); got != 0 {
		t.Fatalf("count = %d, want 0", got)
	}
}

func TestApplyBatchAdd_One(t *testing.T) {
	registerScoped(t, "BATCH_ONE", applyBatchAdd)
	a := newFakeAdder()
	var buf []byte
	buf = binary.AppendUvarint(buf, 1)
	buf = binary.AppendUvarint(buf, 42)
	cmd := &pb.Command{Type: "BATCH_ONE", Group: "g", Payload: buf}
	if err := dispatch(cmd, a); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if got := a.count("g"); got != 1 {
		t.Fatalf("count = %d, want 1", got)
	}
	if a.ids["g"][0] != 42 {
		t.Fatalf("id = %d, want 42", a.ids["g"][0])
	}
}

func TestApplyBatchAdd_N(t *testing.T) {
	registerScoped(t, "BATCH_N", applyBatchAdd)
	a := newFakeAdder()
	var buf []byte
	buf = binary.AppendUvarint(buf, 4)
	for _, id := range []uint64{10, 20, 30, 40} {
		buf = binary.AppendUvarint(buf, id)
	}
	cmd := &pb.Command{Type: "BATCH_N", Group: "g", Payload: buf}
	if err := dispatch(cmd, a); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if got := a.count("g"); got != 4 {
		t.Fatalf("count = %d, want 4", got)
	}
	want := []uint64{10, 20, 30, 40}
	for i, w := range want {
		if a.ids["g"][i] != w {
			t.Fatalf("ids[%d] = %d, want %d", i, a.ids["g"][i], w)
		}
	}
}

func TestApplyBatchAdd_MissingCount(t *testing.T) {
	registerScoped(t, "BATCH_NOCOUNT", applyBatchAdd)
	a := newFakeAdder()
	err := dispatch(&pb.Command{Type: "BATCH_NOCOUNT", Group: "g", Payload: nil}, a)
	if !errors.Is(err, ErrBadPayload) {
		t.Fatalf("expected ErrBadPayload, got %v", err)
	}
	if a.count("g") != 0 {
		t.Fatalf("adder should not have been called")
	}
}

func TestApplyBatchAdd_TruncatedID(t *testing.T) {
	registerScoped(t, "BATCH_TRUNC", applyBatchAdd)
	a := newFakeAdder()
	var buf []byte
	buf = binary.AppendUvarint(buf, 2)
	buf = binary.AppendUvarint(buf, 1)
	// second id: continuation bit set, no following byte
	buf = append(buf, 0x80)
	cmd := &pb.Command{Type: "BATCH_TRUNC", Group: "g", Payload: buf}
	err := dispatch(cmd, a)
	if !errors.Is(err, ErrBadPayload) {
		t.Fatalf("expected ErrBadPayload, got %v", err)
	}
	if got := a.count("g"); got != 1 {
		t.Fatalf("count = %d, want 1 (only the first id should be applied)", got)
	}
}

func TestApplyBatchAdd_AdderErrorPropagates(t *testing.T) {
	registerScoped(t, "BATCH_ADDER_ERR", applyBatchAdd)
	a := newFakeAdder()
	a.errOn["g"] = errors.New("boom")
	var buf []byte
	buf = binary.AppendUvarint(buf, 1)
	buf = binary.AppendUvarint(buf, 7)
	cmd := &pb.Command{Type: "BATCH_ADDER_ERR", Group: "g", Payload: buf}
	if err := dispatch(cmd, a); err == nil || err.Error() != "boom" {
		t.Fatalf("expected boom, got %v", err)
	}
}

// --- MERGE_SKETCH ---

// encodeMergePayload is a small helper for tests: builds the
// [varint algo_len][algo_bytes][sketch_payload] envelope.
func encodeMergePayload(t *testing.T, algo string, sketch []byte) []byte {
	t.Helper()
	var buf []byte
	buf = binary.AppendUvarint(buf, uint64(len(algo)))
	buf = append(buf, algo...)
	buf = append(buf, sketch...)
	return buf
}

func TestApplyMergeSketch_Valid(t *testing.T) {
	registerScoped(t, "MERGE_VALID", applyMergeSketch)
	a := newFakeAdder()
	cmd := &pb.Command{
		Type:    "MERGE_VALID",
		Group:   "g",
		Payload: encodeMergePayload(t, "hll", []byte("sketch-bytes-1")),
	}
	if err := dispatch(cmd, a); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if got := a.sketchCount("g"); got != 1 {
		t.Fatalf("sketchCount = %d, want 1", got)
	}
	if a.sketches["g"][0] != "hll" {
		t.Fatalf("algo = %q, want %q", a.sketches["g"][0], "hll")
	}
}

func TestApplyMergeSketch_TwoSameGroup(t *testing.T) {
	registerScoped(t, "MERGE_TWO", applyMergeSketch)
	a := newFakeAdder()
	for i := 0; i < 2; i++ {
		cmd := &pb.Command{
			Type:    "MERGE_TWO",
			Group:   "g",
			Payload: encodeMergePayload(t, "hll", []byte{byte(i)}),
		}
		if err := dispatch(cmd, a); err != nil {
			t.Fatalf("dispatch %d: %v", i, err)
		}
	}
	if got := a.sketchCount("g"); got != 2 {
		t.Fatalf("sketchCount = %d, want 2", got)
	}
}

func TestApplyMergeSketch_UnregisteredAlgo(t *testing.T) {
	registerScoped(t, "MERGE_UNREG", applyMergeSketch)
	a := newFakeAdder()
	a.errOnAlgo["bitmap"] = ErrUnknownAlgorithm
	cmd := &pb.Command{
		Type:    "MERGE_UNREG",
		Group:   "g",
		Payload: encodeMergePayload(t, "bitmap", []byte{1, 2, 3}),
	}
	err := dispatch(cmd, a)
	if !errors.Is(err, ErrUnknownAlgorithm) {
		t.Fatalf("expected ErrUnknownAlgorithm, got %v", err)
	}
	if a.sketchCount("g") != 0 {
		t.Fatalf("adder recorded a merge on rejection")
	}
}

func TestApplyMergeSketch_MissingAlgoLen(t *testing.T) {
	registerScoped(t, "MERGE_NOLEN", applyMergeSketch)
	a := newFakeAdder()
	err := dispatch(&pb.Command{Type: "MERGE_NOLEN", Group: "g", Payload: nil}, a)
	if !errors.Is(err, ErrBadPayload) {
		t.Fatalf("expected ErrBadPayload, got %v", err)
	}
}

func TestApplyMergeSketch_AlgoLenExceeds(t *testing.T) {
	registerScoped(t, "MERGE_OVERFLOW", applyMergeSketch)
	a := newFakeAdder()
	var buf []byte
	buf = binary.AppendUvarint(buf, 100)            // claim 100-byte algo
	buf = append(buf, []byte("short")...)           // but only 5 bytes follow
	cmd := &pb.Command{Type: "MERGE_OVERFLOW", Group: "g", Payload: buf}
	err := dispatch(cmd, a)
	if !errors.Is(err, ErrBadPayload) {
		t.Fatalf("expected ErrBadPayload, got %v", err)
	}
}

func TestInitRegistersBatchAndMerge(t *testing.T) {
	if _, ok := lookupHandler(TypeBatchAdd); !ok {
		t.Fatalf("TypeBatchAdd handler not registered by init()")
	}
	if _, ok := lookupHandler(TypeMergeSketch); !ok {
		t.Fatalf("TypeMergeSketch handler not registered by init()")
	}
}

// lookupHandler exposes a read-only accessor for tests; package-private.
func lookupHandler(t string) (Handler, bool) {
	handlers.RLock()
	defer handlers.RUnlock()
	h, ok := handlers.m[t]
	return h, ok
}
