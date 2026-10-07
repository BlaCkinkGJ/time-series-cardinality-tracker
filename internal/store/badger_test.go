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

package store_test

import (
	"bytes"
	"errors"
	"os"
	"testing"

	"github.com/BlaCkinkGJ/time-series-cardinality-tracker/internal/store"
)

func tempStore(t *testing.T) (*store.BadgerStore, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "badger-test-*")
	if err != nil {
		t.Fatal(err)
	}
	s, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	return s, func() { s.Close(); os.RemoveAll(dir) }
}

func TestSaveLoad(t *testing.T) {
	s, cleanup := tempStore(t)
	defer cleanup()

	payload := []byte("opaque-sketch-bytes\x00\x01\x02")

	if err := s.Save("ts-001", payload); err != nil {
		t.Fatal(err)
	}

	got, err := s.Load("ts-001")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("loaded %v != saved %v", got, payload)
	}
}

func TestLoad_NotFound(t *testing.T) {
	s, cleanup := tempStore(t)
	defer cleanup()
	_, err := s.Load("nonexistent")
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestLoadAll(t *testing.T) {
	const (
		groupA = "ts-a"
		groupB = "ts-b"
	)
	s, cleanup := tempStore(t)
	defer cleanup()

	// Empty store: no callbacks.
	calls := 0
	if err := s.LoadAll(func(string, []byte) error { calls++; return nil }); err != nil {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Fatalf("empty store yielded %d groups", calls)
	}

	// Saved out of key order; iteration must be deterministic and complete.
	for _, g := range []string{groupB, groupA} {
		if err := s.Save(g, []byte("bytes-"+g)); err != nil {
			t.Fatal(err)
		}
	}
	var groups []string
	values := map[string]string{}
	if err := s.LoadAll(func(g string, b []byte) error {
		groups = append(groups, g)
		values[g] = string(b)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(groups) != 2 || groups[0] != groupA || groups[1] != groupB {
		t.Fatalf("want [%s %s] in key order, got %v", groupA, groupB, groups)
	}
	if values[groupA] != "bytes-"+groupA || values[groupB] != "bytes-"+groupB {
		t.Fatalf("wrong payloads: %v", values)
	}

	// Overwrite stays a single entry; callback errors abort.
	if err := s.Save(groupA, []byte("bytes-"+groupA+"2")); err != nil {
		t.Fatal(err)
	}
	errStop := errors.New("stop")
	calls = 0
	if err := s.LoadAll(func(g string, b []byte) error {
		calls++
		if g == groupA && string(b) != "bytes-"+groupA+"2" {
			t.Fatalf("stale payload for %s: %q", groupA, b)
		}
		return errStop
	}); !errors.Is(err, errStop) {
		t.Fatalf("want callback error to propagate, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("error must abort iteration, got %d calls", calls)
	}
}
