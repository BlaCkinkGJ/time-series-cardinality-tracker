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
	"bytes"
	"errors"
	"fmt"
	"time"

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

// keyPrefix namespaces sketch bytes from any future key kind.
const keyPrefix = "sketch/"

func key(group string) []byte {
	return []byte(keyPrefix + group)
}

// Save writes b as the serialised sketch for group.
func (s *BadgerStore) Save(group string, b []byte) error {
	start := time.Now()
	err := s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key(group), b)
	})

	metricSaveDurationSeconds.Observe(time.Since(start).Seconds())
	if err != nil {
		metricSaveErrorsTotal.Inc()
	}
	return err
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

// LoadAll calls fn for every persisted group, in key order. It is the
// startup path: the engine rebuilds each group by parsing these bytes.
// An error from fn aborts the iteration and is returned.
func (s *BadgerStore) LoadAll(fn func(group string, b []byte) error) error {
	prefix := []byte(keyPrefix)
	return s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			group := string(bytes.TrimPrefix(item.Key(), prefix))
			if err := item.Value(func(val []byte) error {
				return fn(group, append([]byte(nil), val...))
			}); err != nil {
				return err
			}
		}
		return nil
	})
}
