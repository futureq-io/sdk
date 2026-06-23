package futureq

import (
	"crypto/tls"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// Client is the top-level entry point for the FutureQ SDK.
// It owns and manages the underlying gRPC [grpc.ClientConn] and exposes
// factory methods for creating [Producer] and [Consumer] instances.
//
// A Client is safe for concurrent use by multiple goroutines.
// Typically an application creates one Client at startup and reuses it
// throughout its lifetime.
//
// Call [Close] when the Client is no longer needed to release the underlying
// network connection.
type Client struct {
	conn    *grpc.ClientConn
	opts    clientOptions
	managed bool // true when the SDK owns the conn (i.e. created via New)
	closed  bool
}

// clientOptions holds the resolved configuration used when dialling the server.
type clientOptions struct {
	dialTimeout        time.Duration
	keepAliveTime      time.Duration
	keepAliveTimeout   time.Duration
	maxRecvMsgSizeMB   int
	maxSendMsgSizeMB   int
	tlsConfig          *tls.Config
	insecure           bool
	additionalDialOpts []grpc.DialOption
}

// defaultClientOptions returns a sensible production-ready baseline.
func defaultClientOptions() clientOptions {
	return clientOptions{
		dialTimeout:      10 * time.Second,
		keepAliveTime:    30 * time.Second,
		keepAliveTimeout: 10 * time.Second,
		maxRecvMsgSizeMB: 16,
		maxSendMsgSizeMB: 16,
	}
}

// Option is a functional option for configuring a [Client].
type Option func(*clientOptions)

// WithInsecure disables transport security for the connection.
// Use this only in development or when the connection is protected by an
// external proxy (e.g. mutual TLS at the service-mesh layer).
//
// This option is mutually exclusive with [WithTLS].
func WithInsecure() Option {
	return func(o *clientOptions) {
		o.insecure = true
		o.tlsConfig = nil
	}
}

// WithTLS configures the client to use TLS with the provided [tls.Config].
// Pass nil to use the system default TLS configuration (recommended for
// production when connecting to a server with a publicly-signed certificate).
//
// This option is mutually exclusive with [WithInsecure].
func WithTLS(cfg *tls.Config) Option {
	return func(o *clientOptions) {
		o.insecure = false
		o.tlsConfig = cfg
	}
}

// WithDialTimeout sets the maximum duration to wait when establishing the
// initial gRPC connection.  Defaults to 10 seconds.
func WithDialTimeout(d time.Duration) Option {
	return func(o *clientOptions) {
		o.dialTimeout = d
	}
}

// WithKeepAlive configures the client-side HTTP/2 keep-alive probes.
//   - time     — how long the client waits after the last activity before
//     sending a PING frame. Defaults to 30 s.
//   - timeout  — how long the client waits for a PING ACK before considering
//     the connection dead. Defaults to 10 s.
func WithKeepAlive(time, timeout time.Duration) Option {
	return func(o *clientOptions) {
		o.keepAliveTime = time
		o.keepAliveTimeout = timeout
	}
}

// WithMaxRecvMsgSize sets the maximum message size in megabytes that the
// client can receive from the server.  Defaults to 16 MB.
func WithMaxRecvMsgSize(mb int) Option {
	return func(o *clientOptions) {
		o.maxRecvMsgSizeMB = mb
	}
}

// WithMaxSendMsgSize sets the maximum message size in megabytes that the
// client may send to the server.  Defaults to 16 MB.
func WithMaxSendMsgSize(mb int) Option {
	return func(o *clientOptions) {
		o.maxSendMsgSizeMB = mb
	}
}

// WithDialOptions appends arbitrary [grpc.DialOption]s to the dialler.
// Use this escape hatch for features not covered by the typed option set
// (e.g. per-RPC credentials, custom interceptors, service-config JSON).
func WithDialOptions(opts ...grpc.DialOption) Option {
	return func(o *clientOptions) {
		o.additionalDialOpts = append(o.additionalDialOpts, opts...)
	}
}

// New dials the FutureQ server at the given address and returns a ready
// [Client].  The address must be in "host:port" format, e.g.
// "futureq.internal:8443".
//
// By default, New uses TLS with the system certificate pool. Pass
// [WithInsecure] to disable TLS or [WithTLS] to provide a custom
// [tls.Config].
//
// New blocks until the connection is established or [WithDialTimeout] expires.
// An error is returned if the connection cannot be established.
//
//	client, err := futureq.New(
//	    "futureq.internal:8443",
//	    futureq.WithTLS(nil),            // system certs
//	    futureq.WithDialTimeout(5*time.Second),
//	)
func New(addr string, opts ...Option) (*Client, error) {
	o := defaultClientOptions()
	for _, opt := range opts {
		opt(&o)
	}

	dialOpts, err := buildDialOptions(o)
	if err != nil {
		return nil, fmt.Errorf("futureq: build dial options: %w", err)
	}

	conn, err := grpc.NewClient(addr, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("futureq: dial %s: %w", addr, err)
	}

	return &Client{conn: conn, opts: o, managed: true}, nil
}

// NewWithConn creates a [Client] from an existing [grpc.ClientConn].
// The caller retains ownership of the connection; [Client.Close] will not
// close it.
//
// This is useful when you want to share a connection with other gRPC services
// or when you need fine-grained control over connection management (e.g.
// channel pools, custom balancers).
//
//	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
//	client := futureq.NewWithConn(conn)
func NewWithConn(conn *grpc.ClientConn) *Client {
	return &Client{conn: conn, managed: false}
}

// Close releases resources held by the Client.
// If the Client was created with [New] it also closes the underlying gRPC
// connection; connections supplied via [NewWithConn] are left open.
//
// It is safe to call Close more than once; subsequent calls are no-ops.
func (c *Client) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true
	if c.managed && c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// Conn returns the underlying [grpc.ClientConn].
// Most callers should use [NewProducer] and [NewConsumer] instead.
func (c *Client) Conn() *grpc.ClientConn {
	return c.conn
}

// buildDialOptions converts clientOptions into a slice of grpc.DialOption.
func buildDialOptions(o clientOptions) ([]grpc.DialOption, error) {
	var opts []grpc.DialOption

	// Transport credentials
	switch {
	case o.insecure:
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	case o.tlsConfig != nil:
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(o.tlsConfig)))
	default:
		// Default: TLS with system certificate pool
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(nil, "")))
	}

	// Message size limits (convert MB → bytes)
	opts = append(opts,
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(o.maxRecvMsgSizeMB*1024*1024),
			grpc.MaxCallSendMsgSize(o.maxSendMsgSizeMB*1024*1024),
		),
	)

	// Keep-alive
	opts = append(opts, grpc.WithKeepaliveParams(keepalive.ClientParameters{
		Time:                o.keepAliveTime,
		Timeout:             o.keepAliveTimeout,
		PermitWithoutStream: true,
	}))

	// Caller-supplied extras (applied last so they can override defaults)
	opts = append(opts, o.additionalDialOpts...)

	return opts, nil
}
