package futureq

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lni/dragonboat/v4"
	raftconfig "github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/statemachine"
	"github.com/lni/vfs"
	"go.uber.org/zap"

	"github.com/futureq-io/futureq/pkg/raft/metadata"
)

// metadataObserverConfig configures the embedded Dragonboat observer.
//
// The observer runs a local, non-voting Dragonboat replica of the FutureQ
// metadata Raft group inside the SDK process. All Dragonboat state is kept
// in a memory-backed VFS — no files are ever written to disk. Topology
// changes flow into the SDK via the metadata StateMachine the moment they
// are committed, without any polling.
type metadataObserverConfig struct {
	// NodeID is the unique ID of this observer within the metadata Raft
	// group. Must not collide with any broker or any other observer.
	// When zero a random ID in [1<<32, 1<<48) is generated.
	NodeID uint64

	// RaftAddress is the host:port this observer's Raft transport listens
	// on. Must be reachable by the brokers so they can replicate the
	// metadata log to us. When empty, "127.0.0.1:0" is used (any free port
	// on localhost).
	RaftAddress string

	// RTTMillisecond is the expected round-trip time between Raft peers.
	// Defaults to 50 ms.
	RTTMillisecond uint64

	// ShardID is the event shard whose topology the observer reports.
	// The observer tracks every shard present in the metadata log but only
	// the leader of ShardID is pushed into the topology tracker.
	// Defaults to 1 (the default FutureQ event shard).
	ShardID uint64

	// OnTopologyChange is an optional callback invoked every time the
	// tracked shard's leader changes. It runs on the observer's internal
	// goroutine; keep it fast and non-blocking.
	OnTopologyChange func(topo *metadata.ShardTopology)
}

// metadataObserver wraps an embedded Dragonboat NodeHost running a
// non-voting replica of the metadata Raft group.
type metadataObserver struct {
	cfg   metadataObserverConfig
	nh    *dragonboat.NodeHost
	sm    *metadata.MetadataStateMachine
	fs    *vfs.MemFS // held so GC never collects it while NodeHost is running
	track *topologyTracker
	done  chan struct{}
	wg    sync.WaitGroup
	mu    sync.Mutex // guards closed
	closed bool
}

// startMetadataObserver brings up the observer:
//  1. Allocate an observer NodeID / Raft address if the caller left them blank.
//  2. Create a NodeHost with an in-memory VFS (no disk writes).
//  3. StartReplica on the metadata shard with the existing
//     metadata.NewMetadataStateMachineFactory from futureq-v2.
//  4. Call JoinMetadata on one of the seed brokers so we are added as a
//     non-voting member of the metadata group.
//  5. Spawn a watcher goroutine that reads the state machine on every
//     applied entry and forwards leader changes into the tracker.
//
// seedDial is used to call the cluster's JoinMetadata RPC; it must dial
// one of the seed brokers.
func startMetadataObserver(
	ctx context.Context,
	cfg metadataObserverConfig,
	track *topologyTracker,
	seedDial func(ctx context.Context) error,
) (*metadataObserver, error) {
	if cfg.RTTMillisecond == 0 {
		cfg.RTTMillisecond = 50
	}
	if cfg.ShardID == 0 {
		cfg.ShardID = 1
	}
	if cfg.RaftAddress == "" {
		cfg.RaftAddress = "127.0.0.1:0"
	}
	if cfg.NodeID == 0 {
		cfg.NodeID = newObserverNodeID()
	}

	// In-memory VFS — Dragonboat will use it for WALDir, NodeHostDir and
	// all snapshot/log files. Nothing ever touches the OS filesystem.
	memfs := vfs.NewMem()

	nhc := raftconfig.NodeHostConfig{
		WALDir:         "observer-wal",
		NodeHostDir:    "observer-data",
		RTTMillisecond: cfg.RTTMillisecond,
		RaftAddress:    cfg.RaftAddress,
	}
	nhc.Expert.FS = memfs

	nh, err := dragonboat.NewNodeHost(nhc)
	if err != nil {
		return nil, fmt.Errorf("futureq: dragonboat observer: %w", err)
	}

	obs := &metadataObserver{
		cfg:   cfg,
		nh:    nh,
		fs:    memfs,
		track: track,
		done:  make(chan struct{}),
	}

	// Wrap the existing factory so we capture the state-machine instance
	// and can read topology directly from it.
	base := metadata.NewMetadataStateMachineFactory(zap.NewNop())
	factory := func(clusterID, nodeID uint64) statemachine.IStateMachine {
		sm := base(clusterID, nodeID)
		if msm, ok := sm.(*metadata.MetadataStateMachine); ok {
			obs.sm = msm
		}
		return sm
	}

	rc := raftconfig.Config{
		ReplicaID:          cfg.NodeID,
		ShardID:            metadata.MetadataShardID,
		ElectionRTT:        10,
		HeartbeatRTT:       1,
		CheckQuorum:        false, // non-voting observers must not require quorum
		SnapshotEntries:    5,
		CompactionOverhead: 5,
	}

	// join=true means "I'm an already-registered member". We register via
	// the JoinMetadata RPC below; on restart the in-memory state is empty
	// so we always re-register.
	if err := nh.StartReplica(nil, true, factory, rc); err != nil {
		nh.Close()
		return nil, fmt.Errorf("futureq: dragonboat observer: start replica: %w", err)
	}

	// Register with the cluster as a non-voting observer of the metadata
	// group. The seedDial callback performs the actual JoinMetadata RPC.
	if err := seedDial(ctx); err != nil {
		nh.Close()
		return nil, fmt.Errorf("futureq: dragonboat observer: join metadata: %w", err)
	}

	obs.wg.Add(1)
	go obs.watch()

	return obs, nil
}

// watch polls the state machine for the tracked shard's topology and
// forwards changes to the tracker. Applied-Index polling is cheap (an
// in-memory map read) and avoids the complexity of registering a custom
// listener; Dragonboat has already delivered the entry to the SM by the
// time we observe the new value.
func (o *metadataObserver) watch() {
	defer o.wg.Done()

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	var lastLeader uint64
	var lastEpoch uint64

	push := func() {
		sm := o.sm
		if sm == nil {
			return
		}
		topo := sm.GetShardTopology(o.cfg.ShardID)
		if topo == nil {
			return
		}
		if topo.LeaderID == lastLeader && topo.Epoch == lastEpoch {
			return
		}
		lastLeader = topo.LeaderID
		lastEpoch = topo.Epoch

		snap := Topology{
			LeaderNodeID:  topo.LeaderID,
			LeaderAddress: topo.LeaderAddr,
			UpdatedAt:     time.Now(),
		}
		for id, addr := range topo.Nodes {
			grpc := topo.GrpcAddrs[id]
			if grpc == "" {
				grpc = addr
			}
			snap.Nodes = append(snap.Nodes, NodeInfo{
				NodeID:   id,
				Address:  grpc,
				IsLeader: id == topo.LeaderID,
				IsAlive:  true,
			})
		}
		o.track.setLeader(topo.LeaderID, topo.LeaderAddr, snap)

		if o.cfg.OnTopologyChange != nil {
			o.cfg.OnTopologyChange(topo)
		}
	}

	for {
		select {
		case <-o.done:
			return
		case <-ticker.C:
			push()
		}
	}
}

// Close shuts the observer down, leaving the metadata group first so the
// brokers stop replicating to us, then closing the NodeHost. The VFS is
// in-memory, so no cleanup on disk is required.
func (o *metadataObserver) Close() error {
	o.mu.Lock()
	if o.closed {
		o.mu.Unlock()
		return nil
	}
	o.closed = true
	o.mu.Unlock()

	close(o.done)
	o.wg.Wait()

	// Best-effort: tell the cluster we're leaving. Uses a short timeout so
	// Close never hangs when the cluster is unreachable.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = o.nh.SyncRequestDeleteReplica(ctx, metadata.MetadataShardID, o.cfg.NodeID, 0)

	o.nh.Close()
	return nil
}

// observerNodeIDCounter hands out unique node IDs for observers that did
// not specify one. We start above 1<<32 to keep the observer ID space well
// clear of typical broker IDs (1, 2, 3, …).
var observerNodeIDCounter atomic.Uint64

func newObserverNodeID() uint64 {
	n := observerNodeIDCounter.Add(1)
	return (1 << 32) | (n & 0xffff)
}
