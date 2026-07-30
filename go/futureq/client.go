package futureq

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	pb "github.com/futureq-io/protocol/proto/go"
)

// Client is the top-level entry point for the FutureQ SDK.
//
// A Client tracks the FutureQ cluster topology and routes producer and
// consumer streams to the current Raft leader. The topology can be
// discovered two ways:
//
//  1. Polling — the SDK periodically calls GetClusterInfo on one of the
//     seed addresses you passed to [New]. This is the default.
//  2. Dragonboat discovery — when [WithDragonboatDiscovery] is enabled the
//     SDK embeds a non-voting Dragonboat replica of the metadata Raft group
//     in-process and receives topology changes over Raft in real time.
//     This mode is experimental; it never writes to disk (an in-memory VFS
//     backs the embedded replica).
//
// A Client is safe for concurrent use by multiple goroutines.
type Client struct {
	opts clientOptions

	mu       sync.RWMutex
	tracker  *topologyTracker
	observer *metadataObserver // nil unless WithDragonboatDiscovery was set
	closed   bool
}

// clientOptions holds the resolved configuration used when dialling the
// cluster.
type clientOptions struct {
	seeds              []string
	dialTimeout        time.Duration
	keepAliveTime      time.Duration
	keepAliveTimeout   time.Duration
	maxRecvMsgSizeMB   int
	maxSendMsgSizeMB   int
	tlsConfig          *tls.Config
	insecure           bool
	additionalDialOpts []grpc.DialOption

	// refreshInterval is how often the SDK polls GetClusterInfo on the
	// seeds to keep its view of the leader fresh. Defaults to 10 s.
	// Ignored when dragonboat discovery is on (the observer is push-based).
	refreshInterval time.Duration

	// dragonboat is non-nil when dragonboat discovery is enabled.
	dragonboat *metadataObserverConfig
}

func defaultClientOptions() clientOptions {
	return clientOptions{
		dialTimeout:      10 * time.Second,
		keepAliveTime:    30 * time.Second,
		keepAliveTimeout: 10 * time.Second,
		maxRecvMsgSizeMB: 16,
		maxSendMsgSizeMB: 16,
		refreshInterval:  10 * time.Second,
	}
}

// Option is a functional option for configuring a [Client].
type Option func(*clientOptions)

// WithInsecure disables transport security for the connection.
// Use only in development or when TLS terminates at an external proxy.
//
// Mutually exclusive with [WithTLS].
func WithInsecure() Option {
	return func(o *clientOptions) {
		o.insecure = true
		o.tlsConfig = nil
	}
}

// WithTLS configures the client to use TLS with the provided [tls.Config].
// Pass nil to use the system default TLS configuration.
//
// Mutually exclusive with [WithInsecure].
func WithTLS(cfg *tls.Config) Option {
	return func(o *clientOptions) {
		o.insecure = false
		o.tlsConfig = cfg
	}
}

// WithDialTimeout sets the maximum duration to wait when establishing the
// initial gRPC connection. Defaults to 10 s.
func WithDialTimeout(d time.Duration) Option {
	return func(o *clientOptions) { o.dialTimeout = d }
}

// WithKeepAlive configures the client-side HTTP/2 keep-alive probes.
func WithKeepAlive(time_, timeout time.Duration) Option {
	return func(o *clientOptions) {
		o.keepAliveTime = time_
		o.keepAliveTimeout = timeout
	}
}

// WithMaxRecvMsgSize sets the maximum message size in megabytes the client
// can receive from the server. Defaults to 16 MB.
func WithMaxRecvMsgSize(mb int) Option {
	return func(o *clientOptions) { o.maxRecvMsgSizeMB = mb }
}

// WithMaxSendMsgSize sets the maximum message size in megabytes the client
// may send to the server. Defaults to 16 MB.
func WithMaxSendMsgSize(mb int) Option {
	return func(o *clientOptions) { o.maxSendMsgSizeMB = mb }
}

// WithDialOptions appends arbitrary [grpc.DialOption]s to the dialler.
// Use this escape hatch for features not covered by the typed option set
// (per-RPC credentials, custom interceptors, service-config JSON, etc.).
func WithDialOptions(opts ...grpc.DialOption) Option {
	return func(o *clientOptions) {
		o.additionalDialOpts = append(o.additionalDialOpts, opts...)
	}
}

// WithTopologyRefreshInterval overrides how often the SDK polls
// GetClusterInfo on the seed addresses to refresh its view of the cluster
// leader. Defaults to 10 s. Set to a negative value to disable polling.
// Ignored when [WithDragonboatDiscovery] is enabled.
func WithTopologyRefreshInterval(d time.Duration) Option {
	return func(o *clientOptions) { o.refreshInterval = d }
}

// WithDragonboatDiscovery enables the experimental in-process Dragonboat
// observer. The SDK starts a non-voting replica of the metadata Raft group
// inside the client process; topology changes are pushed to the SDK over
// Raft as soon as they commit, so producer / consumer streams follow the
// leader without any polling.
//
// All observer state lives in an in-memory VFS — nothing is written to
// disk. The observer is shut down when [Client.Close] is called.
//
// cfg may be nil; sensible defaults (random NodeID, 127.0.0.1:0 Raft
// address, shard 1) are used. See [metadataObserverConfig] for the
// available knobs.
func WithDragonboatDiscovery(cfg *metadataObserverConfig) Option {
	return func(o *clientOptions) {
		if cfg == nil {
			cfg = &metadataObserverConfig{}
		}
		cp := *cfg
		o.dragonboat = &cp
	}
}

// New creates a [Client] that discovers the FutureQ cluster through the
// supplied seed addresses and returns a ready client. At least one seed
// must be reachable; otherwise New returns the dial error.
//
// Seeds may point at any node in the cluster — leader or follower. The SDK
// uses GetClusterInfo on the seeds to learn the leader address, and then
// routes all producer / consumer streams to it.
//
//	client, err := futureq.New(
//	    []string{"node1.internal:9000", "node2.internal:9000"},
//	    futureq.WithInsecure(),
//	)
func New(seeds []string, opts ...Option) (*Client, error) {
	if len(seeds) == 0 {
		return nil, fmt.Errorf("futureq: at least one seed address is required")
	}
	o := defaultClientOptions()
	for _, opt := range opts {
		opt(&o)
	}

	c := &Client{opts: o}

	dial := func(ctx context.Context, addr string) (*grpc.ClientConn, error) {
		dialOpts, err := buildDialOptions(o)
		if err != nil {
			return nil, err
		}
		conn, err := grpc.NewClient(addr, dialOpts...)
		if err != nil {
			return nil, err
		}
		return conn, nil
	}

	c.tracker = newTopologyTracker(seeds, o.refreshInterval, dial)

	// Initial discovery — must succeed before we return so the caller can
	// assume the client is usable immediately.
	ctx, cancel := context.WithTimeout(context.Background(), o.dialTimeout)
	defer cancel()
	if _, err := c.tracker.refreshOnce(ctx); err != nil {
		return nil, fmt.Errorf("futureq: initial topology discovery: %w", err)
	}

	// Start the background refresh loop unless dragonboat discovery is on
	// (in which case the observer is push-based and polling is redundant).
	if o.dragonboat == nil {
		c.tracker.start(context.Background())
	} else {
		obsCfg := *o.dragonboat
		obs, err := startMetadataObserver(context.Background(), obsCfg, c.tracker,
			func(ctx context.Context) error {
				return c.joinMetadata(ctx, obsCfg.NodeID, obsCfg.RaftAddress)
			},
		)
		if err != nil {
			return nil, err
		}
		c.observer = obs
	}

	return c, nil
}

// joinMetadata calls JoinMetadata on every seed until one accepts us. It
// is invoked exactly once at startup when dragonboat discovery is on.
func (c *Client) joinMetadata(ctx context.Context, nodeID uint64, raftAddr string) error {
	var lastErr error
	for _, seed := range c.tracker.Addresses() {
		callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		conn, err := c.tracker.dial(callCtx, seed)
		if err != nil {
			cancel()
			lastErr = err
			continue
		}
		cli := pb.NewFutureQClusterClient(conn)
		resp, err := cli.JoinMetadata(callCtx, &pb.JoinMetadataRequest{
			NodeId:      nodeID,
			RaftAddress: raftAddr,
		})
		conn.Close()
		cancel()
		if err != nil {
			lastErr = err
			continue
		}
		if !resp.GetSuccess() {
			lastErr = fmt.Errorf("futureq: join metadata rejected: %s", resp.GetErrorMessage())
			continue
		}
		return nil
	}
	if lastErr == nil {
		lastErr = ErrTopologyUnavailable
	}
	return lastErr
}

// Close releases resources held by the Client, including the topology
// refresh loop and the embedded dragonboat observer (when enabled).
//
// It is safe to call Close more than once; subsequent calls are no-ops.
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()

	if c.observer != nil {
		_ = c.observer.Close()
	}
	c.tracker.stop()
	return nil
}

// Topology returns the most recent cluster topology snapshot known to the
// client. The second return value is false when no snapshot has been
// recorded yet (should not happen after a successful [New]).
func (c *Client) Topology() (Topology, bool) {
	return c.tracker.Snapshot()
}

// Leader returns the current best-known leader address, or "" when unknown.
func (c *Client) Leader() string {
	addr, _ := c.tracker.Leader()
	return addr
}

// buildDialOptions converts clientOptions into a slice of grpc.DialOption.
func buildDialOptions(o clientOptions) ([]grpc.DialOption, error) {
	var opts []grpc.DialOption

	switch {
	case o.insecure:
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	case o.tlsConfig != nil:
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(o.tlsConfig)))
	default:
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(nil, "")))
	}

	opts = append(opts,
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(o.maxRecvMsgSizeMB*1024*1024),
			grpc.MaxCallSendMsgSize(o.maxSendMsgSizeMB*1024*1024),
		),
	)

	opts = append(opts, grpc.WithKeepaliveParams(keepalive.ClientParameters{
		Time:                o.keepAliveTime,
		Timeout:             o.keepAliveTimeout,
		PermitWithoutStream: true,
	}))

	opts = append(opts, o.additionalDialOpts...)

	return opts, nil
}

// dialLeader resolves the current leader address and dials it. Returns
// ErrNoLeader when no leader is known.
func (c *Client) dialLeader(ctx context.Context) (*grpc.ClientConn, error) {
	addr, _ := c.tracker.Leader()
	if addr == "" {
		// Try a foreground refresh before failing.
		if _, err := c.tracker.refreshOnce(ctx); err != nil {
			return nil, ErrNoLeader
		}
		addr, _ = c.tracker.Leader()
		if addr == "" {
			return nil, ErrNoLeader
		}
	}
	return c.tracker.dial(ctx, addr)
}
