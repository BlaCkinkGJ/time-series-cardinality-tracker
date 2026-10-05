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
)

// ErrNotFound is returned by Load when the group does not exist.
var ErrNotFound = errors.New("store: group not found")

// BadgerStore persists opaque serialised sketches per group. It does not
// know which cardinality algorithm produced the bytes; parsing is the
// engine's job.
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

func key(group string) []byte {
	return []byte("sketch/" + group)
}

// Save writes b as the serialised sketch for group.
func (s *BadgerStore) Save(group string, b []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key(group), b)
	})
}

// Load reads the serialised sketch for group.
func (s *BadgerStore) Load(group string) ([]byte, error) {
	var b []byte
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key(group))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			b = append([]byte(nil), val...)
			return nil
		})
	})
	return b, err
}
