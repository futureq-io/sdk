package futureq

import (
	"context"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/futureq-io/protocol/proto/go"
)

// Topology is an immutable snapshot of the cluster's view of itself.
type Topology struct {
	// LeaderNodeID is the Raft node ID of the current leader, or 0 if unknown.
	LeaderNodeID uint64

	// LeaderAddress is the gRPC address clients should dial for producing
	// and consuming. Empty when unknown.
	LeaderAddress string

	// Nodes lists every known cluster member.
	Nodes []NodeInfo

	// UpdatedAt is when this snapshot was recorded locally.
	UpdatedAt time.Time
}

// NodeInfo describes a single cluster member.
type NodeInfo struct {
	NodeID   uint64
	Address  string
	IsLeader bool
	IsAlive  bool
}

// topologyTracker tracks the current cluster leader and routes producer /
// consumer streams to it. It is refreshed either by polling GetClusterInfo
// against the seed addresses, or (when [WithDragonboatDiscovery] is on) by
// a local Dragonboat observer that receives topology changes over Raft.
//
// The zero value is not usable; construct via newTopologyTracker.
type topologyTracker struct {
	mu          sync.RWMutex
	leaderAddr  string
	leaderID    uint64
	last        Topology
	haveSnap    bool
	seedAddrs   []string
	refreshFreq time.Duration
	dial        func(ctx context.Context, addr string) (*grpc.ClientConn, error)

	// notifyCh receives a signal every time the leader address changes.
	// Producers and consumers listen on this to tear down and re-dial.
	notifyCh chan struct{}

	// cancel stops the background refresh loop.
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func newTopologyTracker(
	seeds []string,
	refreshFreq time.Duration,
	dial func(ctx context.Context, addr string) (*grpc.ClientConn, error),
) *topologyTracker {
	cp := make([]string, len(seeds))
	copy(cp, seeds)
	return &topologyTracker{
		seedAddrs:   cp,
		refreshFreq: refreshFreq,
		dial:        dial,
		notifyCh:    make(chan struct{}, 1),
	}
}

// Leader returns the current best-known leader address, or "" when unknown.
func (t *topologyTracker) Leader() (addr string, nodeID uint64) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.leaderAddr, t.leaderID
}

// Snapshot returns the most recent topology snapshot.
// The second return value is false when no snapshot has been received yet.
func (t *topologyTracker) Snapshot() (Topology, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.last, t.haveSnap
}

// Addresses returns the configured seed addresses.
func (t *topologyTracker) Addresses() []string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]string, len(t.seedAddrs))
	copy(out, t.seedAddrs)
	return out
}

// Notify returns a channel that receives a value every time the leader
// address changes. The channel has buffer 1, so slow consumers never
// block the tracker.
func (t *topologyTracker) Notify() <-chan struct{} { return t.notifyCh }

// setLeader atomically records a new leader address. When the address
// differs from the previous one, a notification is broadcast on Notify.
func (t *topologyTracker) setLeader(nodeID uint64, addr string, topo Topology) {
	t.mu.Lock()
	changed := addr != "" && (addr != t.leaderAddr || nodeID != t.leaderID)
	t.leaderAddr = addr
	t.leaderID = nodeID
	t.last = topo
	t.haveSnap = true
	t.mu.Unlock()

	if changed {
		select {
		case t.notifyCh <- struct{}{}:
		default:
		}
	}
}

// start launches the background refresh loop. The loop polls GetClusterInfo
// on each seed address in turn until one responds, then applies the result.
// It is a no-op when refreshFreq <= 0 (pure dragonboat mode with no polling).
func (t *topologyTracker) start(ctx context.Context) {
	if t.refreshFreq <= 0 {
		return
	}
	pollCtx, cancel := context.WithCancel(ctx)
	t.cancel = cancel
	t.wg.Add(1)
	go func() {
		defer t.wg.Done()
		tick := time.NewTicker(t.refreshFreq)
		defer tick.Stop()
		for {
			select {
			case <-pollCtx.Done():
				return
			case <-tick.C:
				_, _ = t.refreshOnce(pollCtx)
			}
		}
	}()
}

// stop terminates the background refresh loop and blocks until it exits.
func (t *topologyTracker) stop() {
	if t.cancel != nil {
		t.cancel()
	}
	t.wg.Wait()
}

// refreshOnce polls the seeds once and applies the first successful
// response. Returns the fetched Topology on success.
func (t *topologyTracker) refreshOnce(ctx context.Context) (Topology, error) {
	t.mu.RLock()
	seeds := make([]string, len(t.seedAddrs))
	copy(seeds, t.seedAddrs)
	t.mu.RUnlock()

	if len(seeds) == 0 {
		return Topology{}, ErrTopologyUnavailable
	}

	var lastErr error
	for _, addr := range seeds {
		topo, err := t.fetchClusterInfo(ctx, addr)
		if err != nil {
			lastErr = err
			continue
		}
		t.setLeader(topo.LeaderNodeID, topo.LeaderAddress, topo)
		return topo, nil
	}
	if lastErr == nil {
		lastErr = ErrTopologyUnavailable
	}
	return Topology{}, lastErr
}

// fetchClusterInfo dials addr and calls GetClusterInfo.
func (t *topologyTracker) fetchClusterInfo(ctx context.Context, addr string) (Topology, error) {
	callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	conn, err := t.dial(callCtx, addr)
	if err != nil {
		return Topology{}, err
	}
	defer conn.Close()

	cli := pb.NewFutureQClusterClient(conn)
	resp, err := cli.GetClusterInfo(callCtx, &pb.ClusterInfoRequest{})
	if err != nil {
		return Topology{}, err
	}

	topo := Topology{
		LeaderNodeID:  resp.GetLeaderNodeId(),
		LeaderAddress: resp.GetLeaderAddress(),
		UpdatedAt:     time.Now(),
		Nodes:         make([]NodeInfo, 0, len(resp.GetNodes())),
	}
	for _, n := range resp.GetNodes() {
		topo.Nodes = append(topo.Nodes, NodeInfo{
			NodeID:   n.GetNodeId(),
			Address:  n.GetAddress(),
			IsLeader: n.GetIsLeader(),
			IsAlive:  n.GetIsAlive(),
		})
	}
	return topo, nil
}

// isUnavailableOrNotLeader reports whether an error returned by a
// producer/consumer stream is worth triggering a topology refresh.
func isUnavailableOrNotLeader(err error) bool {
	if err == nil {
		return false
	}
	if err == ErrNotLeader || err == ErrNoLeader || err == ErrStreamClosed {
		return true
	}
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	switch st.Code() {
	case codes.Unavailable, codes.Canceled, codes.DeadlineExceeded:
		return true
	}
	return false
}
