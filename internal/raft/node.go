// internal/raft/node.go
package raft

import (
	"context"
	"log"
	"sync"
	"time"

	etcdraft "go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	pb "github.com/yourorg/cardinality-tracker/gen/cardinality/v1"
	"github.com/yourorg/cardinality-tracker/internal/hll"
	"github.com/yourorg/cardinality-tracker/internal/store"
)

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
	var rpeers []etcdraft.Peer
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
				log.Printf("raft: storage append: %v", err)
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
			log.Printf("raft: bad entry at index %d: %v", e.Index, err)
			continue
		}
		switch cmd.Type {
		case pb.Command_ADD:
			n.engine.Add(cmd.SeriesId, cmd.Value)
			if h, ok := n.engine.Get(cmd.SeriesId); ok {
				_ = n.store.Save(cmd.SeriesId, h)
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
		log.Printf("raft: snapshot encode: %v", err)
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
		log.Printf("raft: apply snapshot: %v", err)
		return
	}
	if err := n.storage.Compact(idx); err != nil && err != etcdraft.ErrCompacted {
		log.Printf("raft: compact: %v", err)
	}
	n.mu.Lock()
	n.snapCount = 0
	n.mu.Unlock()
	log.Printf("raft: snapshot taken at index %d", idx)
}

// ProposeAdd submits an Add command to the Raft cluster and waits for acceptance.
func (n *Node) ProposeAdd(ctx context.Context, seriesID string, value []byte) error {
	cmd := &pb.Command{Type: pb.Command_ADD, SeriesId: seriesID, Value: value}
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
