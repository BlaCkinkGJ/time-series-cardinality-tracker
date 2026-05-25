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
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/yourorg/cardinality-tracker/internal/hll"
	"github.com/yourorg/cardinality-tracker/internal/store"
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

	h := hll.New()
	for i := 0; i < 1000; i++ {
		h.Add([]byte(fmt.Sprintf("v%d", i)))
	}
	est := h.Estimate()

	if err := s.Save("ts-001", h); err != nil {
		t.Fatal(err)
	}

	h2, err := s.Load("ts-001")
	if err != nil {
		t.Fatal(err)
	}
	if h2.Estimate() != est {
		t.Fatalf("load estimate %d != saved %d", h2.Estimate(), est)
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
