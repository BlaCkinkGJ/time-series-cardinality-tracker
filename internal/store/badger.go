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

package store

import (
	"errors"
	"fmt"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/yourorg/cardinality-tracker/internal/hll"
)

// ErrNotFound is returned by Load when the series does not exist.
var ErrNotFound = errors.New("store: series not found")

// BadgerStore persists HLL state to BadgerDB.
type BadgerStore struct {
	db *badger.DB
}

// Open opens (or creates) a BadgerDB at dir.
func Open(dir string) (*BadgerStore, error) {
	opts := badger.DefaultOptions(dir).
		WithSyncWrites(false). // Raft log provides durability
		WithLogger(nil)
	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("store.Open: %w", err)
	}
	return &BadgerStore{db: db}, nil
}

// Close shuts down BadgerDB gracefully.
func (s *BadgerStore) Close() error { return s.db.Close() }

func key(seriesID string) []byte {
	return []byte("hll/" + seriesID)
}

// Save serialises h and writes it to BadgerDB.
func (s *BadgerStore) Save(seriesID string, h *hll.HLL) error {
	b, err := h.Marshal()
	if err != nil {
		return fmt.Errorf("store.Save marshal: %w", err)
	}
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key(seriesID), b)
	})
}

// Load reads and deserialises the HLL for seriesID.
func (s *BadgerStore) Load(seriesID string) (*hll.HLL, error) {
	var h *hll.HLL
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key(seriesID))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			var e error
			h, e = hll.Unmarshal(val)
			return e
		})
	})
	return h, err
}
