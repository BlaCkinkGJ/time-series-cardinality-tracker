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
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	etcdraft "go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	pb "github.com/yourorg/cardinality-tracker/gen/cardinality/v1"
	"github.com/yourorg/cardinality-tracker/internal/hll"
	"github.com/yourorg/cardinality-tracker/internal/store"
)

// hllAdder adapts *hll.Engine to the raft.Adder interface. The engine's
// Add takes a string id; the dispatcher passes a uint64. Dropped in #13
// when the cardinality engine takes uint64 directly.
//
// Merge supports the MERGE_SKETCH handler. Only "hll" is recognised
// today; per-group algorithm override and the cardinality.Algorithm
// registry arrive in #13, after which this method becomes a one-liner
// over cardinality.Engine.Merge.
type hllAdder struct{ eng *hll.Engine }

func (a hllAdder) Add(group string, id uint64) error {
	a.eng.Add(group, strconv.FormatUint(id, 10))
	return nil
}

func (a hllAdder) Merge(group, algoName string, sketch []byte) error {
	if algoName != "hll" {
		return fmt.Errorf("%w: %q", ErrUnknownAlgorithm, algoName)
	}
	remote, err := hll.Unmarshal(sketch)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrBadPayload, err)
	}
	a.eng.Merge(group, remote)
	return nil
}

const snapshotThreshold = 10_000

// Peer identifies a cluster member.
type Peer struct {
	ID   uint64
	Addr string // host:port for gRPC transport (future multi-node)
}

type propose struct {
	data    []byte
	resultC chan error
}

// Node wraps etcd raft.Node with FSM application logic.
type Node struct {
	id      uint64
	node    etcdraft.Node
	storage *etcdraft.MemoryStorage
	engine  *hll.Engine
	store   *store.BadgerStore

	proposeC chan propose
	stopC    chan struct{}
	doneC    chan struct{}

	mu          sync.Mutex
	appliedIdx  uint64
	snapCount   uint64
}

// NewNode creates a Raft node. Single-node cluster when peers=[]Peer{{ID: id}}.
func NewNode(id uint64, peers []Peer, engine *hll.Engine, st *store.BadgerStore) *Node {
	storage := etcdraft.NewMemoryStorage()
	cfg := &etcdraft.Config{
		ID:              id,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		MaxSizePerMsg:   4096,
		MaxInflightMsgs: 256,
	}
	rpeers := make([]etcdraft.Peer, 0, len(peers))
	for _, p := range peers {
		rpeers = append(rpeers, etcdraft.Peer{ID: p.ID})
	}
	rn := etcdraft.StartNode(cfg, rpeers)
	return &Node{
		id:       id,
		node:     rn,
		storage:  storage,
		engine:   engine,
		store:    st,
		proposeC: make(chan propose, 128),
		stopC:    make(chan struct{}),
		doneC:    make(chan struct{}),
	}
}

// Run drives the Raft event loop. Call in a goroutine.
func (n *Node) Run() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	defer close(n.doneC)
	for {
		select {
		case <-n.stopC:
			n.node.Stop()
			return
		case <-ticker.C:
			n.node.Tick()
		case p := <-n.proposeC:
			go func(p propose) {
				p.resultC <- n.node.Propose(context.Background(), p.data)
			}(p)
		case rd := <-n.node.Ready():
			if err := n.storage.Append(rd.Entries); err != nil {
				slog.Error("raft: storage append error", "error", err)
			}
			n.applyEntries(rd.CommittedEntries)
			n.node.Advance()
			// Snapshot if enough entries accumulated
			n.maybeSnapshot()
		}
	}
}

func (n *Node) applyEntries(entries []raftpb.Entry) {
	for _, e := range entries {
		if e.Type != raftpb.EntryNormal || len(e.Data) == 0 {
			continue
		}
		var cmd pb.Command
		if err := proto.Unmarshal(e.Data, &cmd); err != nil {
			slog.Error("raft: bad entry unmarshal error", "index", e.Index, "error", err)
			continue
		}
		if err := dispatch(&cmd, hllAdder{n.engine}); err != nil {
			slog.Error("raft: dispatch", "index", e.Index, "type", cmd.Type, "error", err)
			continue
		}
		if h, ok := n.engine.Get(cmd.Group); ok {
			if err := n.store.Save(cmd.Group, h); err != nil {
				slog.Error("raft: failed to save to BadgerDB store", "group", cmd.Group, "error", err)
			}
		}
		n.mu.Lock()
		n.appliedIdx = e.Index
		n.snapCount++
		n.mu.Unlock()
	}
}

func (n *Node) maybeSnapshot() {
	n.mu.Lock()
	count := n.snapCount
	idx := n.appliedIdx
	n.mu.Unlock()

	if count < snapshotThreshold {
		return
	}

	data, err := SnapshotEngine(n.engine)
	if err != nil {
		slog.Error("raft: snapshot encode error", "error", err)
		return
	}
	snap := raftpb.Snapshot{
		Data: data,
		Metadata: raftpb.SnapshotMetadata{
			Index: idx,
			Term:  1,
		},
	}
	if err := n.storage.ApplySnapshot(snap); err != nil {
		slog.Error("raft: apply snapshot error", "error", err)
		return
	}
	if err := n.storage.Compact(idx); err != nil && !errors.Is(err, etcdraft.ErrCompacted) {
		slog.Error("raft: compact error", "error", err)
	}
	n.mu.Lock()
	n.snapCount = 0
	n.mu.Unlock()
	slog.Info("raft: snapshot taken successfully", "index", idx)
}

// ProposeAdd submits an Add command to the Raft cluster and waits for acceptance.
func (n *Node) ProposeAdd(ctx context.Context, group string, id uint64) error {
	cmd := &pb.Command{
		Type:    TypeAdd,
		Group:   group,
		Payload: binary.AppendUvarint(nil, id),
	}
	data, err := proto.Marshal(cmd)
	if err != nil {
		return err
	}
	resultC := make(chan error, 1)
	select {
	case n.proposeC <- propose{data: data, resultC: resultC}:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-resultC:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Stop shuts down the Raft node gracefully.
func (n *Node) Stop() {
	close(n.stopC)
	<-n.doneC
}
