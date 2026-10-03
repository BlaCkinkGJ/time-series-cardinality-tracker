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

package handler

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

func TestRegisterHandler_GetHandler(t *testing.T) {
	const tp = "TEST_REGISTER_GET"
	r := NewRegistry()
	r.Register(tp, func(cmd *pb.Command, apply Adder) error { return nil })
	if _, ok := r.lookup(tp); !ok {
		t.Fatalf("expected handler for %q", tp)
	}
}

func TestDispatch_Unknown(t *testing.T) {
	r := NewRegistry()
	err := r.Dispatch(&pb.Command{Type: "NEVER_REGISTERED"}, newFakeAdder())
	if err == nil {
		t.Fatal("expected error for unknown type")
	}
	if !errors.Is(err, ErrUnknownCommand) {
		t.Fatalf("expected ErrUnknownCommand, got %v", err)
	}
}

func TestApplyAdd_ValidPayload(t *testing.T) {
	r := NewRegistry()
	RegisterAdd(r)
	a := newFakeAdder()
	cmd := &pb.Command{Type: TypeAdd, Group: "g", Payload: binary.AppendUvarint(nil, 42)}
	if err := r.Dispatch(cmd, a); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if got := a.count("g"); got != 1 {
		t.Fatalf("count = %d, want 1", got)
	}
}

func TestApplyAdd_Multiple(t *testing.T) {
	r := NewRegistry()
	RegisterAdd(r)
	a := newFakeAdder()
	for _, id := range []uint64{7, 11} {
		cmd := &pb.Command{Type: TypeAdd, Group: "g", Payload: binary.AppendUvarint(nil, id)}
		if err := r.Dispatch(cmd, a); err != nil {
			t.Fatalf("Dispatch(%d): %v", id, err)
		}
	}
	if got := a.count("g"); got != 2 {
		t.Fatalf("count = %d, want 2", got)
	}
}

func TestApplyAdd_EmptyPayload(t *testing.T) {
	r := NewRegistry()
	RegisterAdd(r)
	a := newFakeAdder()
	err := r.Dispatch(&pb.Command{Type: TypeAdd, Group: "g", Payload: nil}, a)
	if !errors.Is(err, ErrBadPayload) {
		t.Fatalf("expected ErrBadPayload, got %v", err)
	}
	if a.count("g") != 0 {
		t.Fatalf("adder should not have been called")
	}
}

func TestApplyAdd_TruncatedVarint(t *testing.T) {
	r := NewRegistry()
	RegisterAdd(r)
	a := newFakeAdder()
	// First byte of a varint with continuation bit set, no trailing bytes.
	err := r.Dispatch(&pb.Command{Type: TypeAdd, Group: "g", Payload: []byte{0x80}}, a)
	if !errors.Is(err, ErrBadPayload) {
		t.Fatalf("expected ErrBadPayload, got %v", err)
	}
}

func TestApplyAdd_ExtraBytes(t *testing.T) {
	r := NewRegistry()
	RegisterAdd(r)
	a := newFakeAdder()
	// 1-byte varint(7) + 1 trailing byte
	err := r.Dispatch(&pb.Command{Type: TypeAdd, Group: "g", Payload: []byte{0x07, 0x00}}, a)
	if !errors.Is(err, ErrBadPayload) {
		t.Fatalf("expected ErrBadPayload, got %v", err)
	}
}

func TestApplyAdd_NilAdder(t *testing.T) {
	r := NewRegistry()
	RegisterAdd(r)
	cmd := &pb.Command{Type: TypeAdd, Group: "g", Payload: binary.AppendUvarint(nil, 1)}
	err := r.Dispatch(cmd, nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if errors.Is(err, ErrUnknownCommand) {
		t.Fatalf("expected non-unknown error, got %v", err)
	}
}

func TestDispatch_NilCommand(t *testing.T) {
	r := NewRegistry()
	err := r.Dispatch(nil, newFakeAdder())
	if !errors.Is(err, ErrUnknownCommand) {
		t.Fatalf("expected ErrUnknownCommand, got %v", err)
	}
}

func TestApplyAdd_AdderErrorPropagates(t *testing.T) {
	r := NewRegistry()
	RegisterAdd(r)
	a := newFakeAdder()
	a.errOn["g"] = errors.New("boom")
	cmd := &pb.Command{Type: TypeAdd, Group: "g", Payload: binary.AppendUvarint(nil, 99)}
	err := r.Dispatch(cmd, a)
	if err == nil || err.Error() != "boom" {
		t.Fatalf("expected boom, got %v", err)
	}
}

func TestDefaultRegistry_HasBuiltins(t *testing.T) {
	r := DefaultRegistry()
	for _, want := range []string{TypeAdd, TypeBatchAdd, TypeMergeSketch} {
		if _, ok := r.lookup(want); !ok {
			t.Fatalf("DefaultRegistry missing %q", want)
		}
	}
}

// --- BATCH_ADD ---

func TestApplyBatchAdd_Empty(t *testing.T) {
	r := NewRegistry()
	RegisterBatch(r)
	a := newFakeAdder()
	cmd := &pb.Command{
		Type:    TypeBatchAdd,
		Group:   "g",
		Payload: binary.AppendUvarint(nil, 0),
	}
	if err := r.Dispatch(cmd, a); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if got := a.count("g"); got != 0 {
		t.Fatalf("count = %d, want 0", got)
	}
}

func TestApplyBatchAdd_One(t *testing.T) {
	r := NewRegistry()
	RegisterBatch(r)
	a := newFakeAdder()
	var buf []byte
	buf = binary.AppendUvarint(buf, 1)
	buf = binary.AppendUvarint(buf, 42)
	cmd := &pb.Command{Type: TypeBatchAdd, Group: "g", Payload: buf}
	if err := r.Dispatch(cmd, a); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if got := a.count("g"); got != 1 {
		t.Fatalf("count = %d, want 1", got)
	}
	if a.ids["g"][0] != 42 {
		t.Fatalf("id = %d, want 42", a.ids["g"][0])
	}
}

func TestApplyBatchAdd_N(t *testing.T) {
	r := NewRegistry()
	RegisterBatch(r)
	a := newFakeAdder()
	var buf []byte
	buf = binary.AppendUvarint(buf, 4)
	for _, id := range []uint64{10, 20, 30, 40} {
		buf = binary.AppendUvarint(buf, id)
	}
	cmd := &pb.Command{Type: TypeBatchAdd, Group: "g", Payload: buf}
	if err := r.Dispatch(cmd, a); err != nil {
		t.Fatalf("Dispatch: %v", err)
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
	r := NewRegistry()
	RegisterBatch(r)
	a := newFakeAdder()
	err := r.Dispatch(&pb.Command{Type: TypeBatchAdd, Group: "g", Payload: nil}, a)
	if !errors.Is(err, ErrBadPayload) {
		t.Fatalf("expected ErrBadPayload, got %v", err)
	}
	if a.count("g") != 0 {
		t.Fatalf("adder should not have been called")
	}
}

func TestApplyBatchAdd_TruncatedID(t *testing.T) {
	r := NewRegistry()
	RegisterBatch(r)
	a := newFakeAdder()
	var buf []byte
	buf = binary.AppendUvarint(buf, 2)
	buf = binary.AppendUvarint(buf, 1)
	// second id: continuation bit set, no following byte
	buf = append(buf, 0x80)
	cmd := &pb.Command{Type: TypeBatchAdd, Group: "g", Payload: buf}
	err := r.Dispatch(cmd, a)
	if !errors.Is(err, ErrBadPayload) {
		t.Fatalf("expected ErrBadPayload, got %v", err)
	}
	if got := a.count("g"); got != 1 {
		t.Fatalf("count = %d, want 1 (only the first id should be applied)", got)
	}
}

func TestApplyBatchAdd_AdderErrorPropagates(t *testing.T) {
	r := NewRegistry()
	RegisterBatch(r)
	a := newFakeAdder()
	a.errOn["g"] = errors.New("boom")
	var buf []byte
	buf = binary.AppendUvarint(buf, 1)
	buf = binary.AppendUvarint(buf, 7)
	cmd := &pb.Command{Type: TypeBatchAdd, Group: "g", Payload: buf}
	if err := r.Dispatch(cmd, a); err == nil || err.Error() != "boom" {
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
	r := NewRegistry()
	RegisterMerge(r)
	a := newFakeAdder()
	cmd := &pb.Command{
		Type:    TypeMergeSketch,
		Group:   "g",
		Payload: encodeMergePayload(t, "hll", []byte("sketch-bytes-1")),
	}
	if err := r.Dispatch(cmd, a); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if got := a.sketchCount("g"); got != 1 {
		t.Fatalf("sketchCount = %d, want 1", got)
	}
	if a.sketches["g"][0] != "hll" {
		t.Fatalf("algo = %q, want %q", a.sketches["g"][0], "hll")
	}
}

func TestApplyMergeSketch_TwoSameGroup(t *testing.T) {
	r := NewRegistry()
	RegisterMerge(r)
	a := newFakeAdder()
	for i := 0; i < 2; i++ {
		cmd := &pb.Command{
			Type:    TypeMergeSketch,
			Group:   "g",
			Payload: encodeMergePayload(t, "hll", []byte{byte(i)}),
		}
		if err := r.Dispatch(cmd, a); err != nil {
			t.Fatalf("Dispatch %d: %v", i, err)
		}
	}
	if got := a.sketchCount("g"); got != 2 {
		t.Fatalf("sketchCount = %d, want 2", got)
	}
}

func TestApplyMergeSketch_UnregisteredAlgo(t *testing.T) {
	r := NewRegistry()
	RegisterMerge(r)
	a := newFakeAdder()
	a.errOnAlgo["bitmap"] = ErrUnknownAlgorithm
	cmd := &pb.Command{
		Type:    TypeMergeSketch,
		Group:   "g",
		Payload: encodeMergePayload(t, "bitmap", []byte{1, 2, 3}),
	}
	err := r.Dispatch(cmd, a)
	if !errors.Is(err, ErrUnknownAlgorithm) {
		t.Fatalf("expected ErrUnknownAlgorithm, got %v", err)
	}
	if a.sketchCount("g") != 0 {
		t.Fatalf("adder recorded a merge on rejection")
	}
}

func TestApplyMergeSketch_MissingAlgoLen(t *testing.T) {
	r := NewRegistry()
	RegisterMerge(r)
	a := newFakeAdder()
	err := r.Dispatch(&pb.Command{Type: TypeMergeSketch, Group: "g", Payload: nil}, a)
	if !errors.Is(err, ErrBadPayload) {
		t.Fatalf("expected ErrBadPayload, got %v", err)
	}
}

func TestApplyMergeSketch_AlgoLenExceeds(t *testing.T) {
	r := NewRegistry()
	RegisterMerge(r)
	a := newFakeAdder()
	var buf []byte
	buf = binary.AppendUvarint(buf, 100)  // claim 100-byte algo
	buf = append(buf, []byte("short")...) // but only 5 bytes follow
	cmd := &pb.Command{Type: TypeMergeSketch, Group: "g", Payload: buf}
	err := r.Dispatch(cmd, a)
	if !errors.Is(err, ErrBadPayload) {
		t.Fatalf("expected ErrBadPayload, got %v", err)
	}
}

// lookup is a read-only accessor used by tests; package-private.
func (r *Registry) lookup(t string) (Handler, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.m[t]
	return h, ok
}
