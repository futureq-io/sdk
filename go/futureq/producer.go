package futureq

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/futureq-io/protocol/proto/go"
)

// Producer publishes batches of messages to the FutureQ cluster.
//
// Internally it maintains a long-lived bi-directional gRPC stream
// ([FutureQProducer.PublishStream]) connected to the current Raft leader.
// When the leader changes (a topology event delivered by the SDK's
// discovery mechanism), the producer tears down the stream and re-dials
// the new leader automatically.
//
// Create a Producer via [Client.NewProducer]. A Producer must be closed
// with [Producer.Close] when no longer needed.
//
// A Producer is safe for concurrent use by multiple goroutines.
type Producer struct {
	client *Client
	cfg    producerConfig

	mu     sync.Mutex
	stream grpc.BidiStreamingClient[pb.PublishBatch, pb.PublishBatchAck]
	conn   *grpc.ClientConn
	// leaderAddr records which address the current stream is connected to,
	// so reconnect() can detect when a re-dial is actually needed.
	leaderAddr string
	closed     bool
}

// ProducerOption is a functional option for [Client.NewProducer].
type ProducerOption func(*producerConfig)

type producerConfig struct {
	// publishTimeout is the per-batch deadline for the entire
	// send-and-wait-for-ack round trip. Defaults to 10 s.
	publishTimeout time.Duration
}

func defaultProducerConfig() producerConfig {
	return producerConfig{publishTimeout: 10 * time.Second}
}

// WithPublishTimeout sets the maximum duration to wait for a server ACK
// after sending a single batch. Defaults to 10 s.
func WithPublishTimeout(d time.Duration) ProducerOption {
	return func(c *producerConfig) { c.publishTimeout = d }
}

// NewProducer opens a publish stream to the current cluster leader and
// returns a ready [Producer].
//
// The context controls the lifetime of the underlying stream. Cancel it
// to tear the stream down asynchronously; subsequent [Producer.PublishBatch]
// calls will re-establish the stream against the then-current leader.
func (c *Client) NewProducer(ctx context.Context, opts ...ProducerOption) (*Producer, error) {
	cfg := defaultProducerConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	p := &Producer{client: c, cfg: cfg}
	if err := p.reconnect(ctx); err != nil {
		return nil, err
	}
	return p, nil
}

// reconnect establishes a fresh stream to the current leader. If the
// existing stream is already on the current leader, reconnect is a no-op.
// The caller must hold p.mu.
func (p *Producer) reconnect(ctx context.Context) error {
	addr, _ := p.client.tracker.Leader()
	if addr == "" {
		if _, err := p.client.tracker.refreshOnce(ctx); err != nil {
			return ErrNoLeader
		}
		addr, _ = p.client.tracker.Leader()
		if addr == "" {
			return ErrNoLeader
		}
	}
	if p.stream != nil && p.leaderAddr == addr {
		return nil
	}

	p.closeStreamLocked()

	conn, err := p.client.tracker.dial(ctx, addr)
	if err != nil {
		return fmt.Errorf("futureq: dial leader %s: %w", addr, err)
	}
	cli := pb.NewFutureQProducerClient(conn)
	stream, err := cli.PublishStream(ctx)
	if err != nil {
		conn.Close()
		return fmt.Errorf("futureq: open producer stream: %w", err)
	}
	p.conn = conn
	p.stream = stream
	p.leaderAddr = addr
	return nil
}

// closeStreamLocked tears down the current stream, if any.
// The caller must hold p.mu.
func (p *Producer) closeStreamLocked() {
	if p.stream != nil {
		_ = p.stream.CloseSend()
		p.stream = nil
	}
	if p.conn != nil {
		_ = p.conn.Close()
		p.conn = nil
	}
	p.leaderAddr = ""
}

// Publish schedules a single message. It is equivalent to
// [Producer.PublishBatch] with a one-element slice.
func (p *Producer) Publish(ctx context.Context, msg Message) error {
	return p.PublishBatch(ctx, []Message{msg}, AckQuorum)
}

// PublishBatch schedules msgs atomically as a single Raft log entry and
// blocks until the broker acknowledges the batch.
//
// ackLevel selects the durability guarantee:
//   - [AckQuorum] — the broker waits until the batch is replicated to a
//     quorum of Raft voters before acknowledging. Safest; default.
//   - [AckNone] — the broker acknowledges immediately. Fastest; messages
//     may be lost if the leader crashes before replicating.
//
// Possible errors:
//   - [ErrClosed] — the Producer has been closed.
//   - [ErrNoLeader] — the SDK does not currently know of a live leader.
//   - [ErrNotLeader] — the connected node is no longer the leader (the
//     SDK reconnects to the new leader before returning, so the next call
//     usually succeeds).
//   - [PublishError] via [errors.As] — the broker acknowledged the batch
//     but reported an application-level failure.
func (p *Producer) PublishBatch(ctx context.Context, msgs []Message, ackLevel AckLevel) error {
	if len(msgs) == 0 {
		return nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return ErrClosed
	}

	if err := p.reconnect(ctx); err != nil {
		return err
	}

	frame := &pb.PublishBatch{
		Messages: make([]*pb.PublishMessage, len(msgs)),
		AckLevel: toProtoAckLevel(ackLevel),
	}
	for i, m := range msgs {
		frame.Messages[i] = toProtoMessage(m)
	}

	callCtx, cancel := context.WithTimeout(ctx, p.cfg.publishTimeout)
	defer cancel()

	if err := sendFrame(callCtx, p.stream, frame); err != nil {
		// Force reconnect on next call.
		p.closeStreamLocked()
		return wrapStreamErr("publish send", err)
	}

	ack, err := recvAck(callCtx, p.stream)
	if err != nil {
		p.closeStreamLocked()
		return wrapStreamErr("publish recv ack", err)
	}

	if !ack.GetSuccess() {
		msg := ack.GetErrorMessage()
		if strings.Contains(msg, "not the cluster leader") {
			p.closeStreamLocked()
			return ErrNotLeader
		}
		return &PublishError{ServerMessage: msg}
	}
	return nil
}

// Close gracefully closes the producer stream. After Close returns,
// further calls to [Producer.Publish] / [Producer.PublishBatch] return
// [ErrClosed].
//
// Safe to call more than once.
func (p *Producer) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	p.closeStreamLocked()
	return nil
}

// ─── helpers ──────────────────────────────────────────────────────────────

// toProtoAckLevel converts the SDK enum to the wire enum.
func toProtoAckLevel(a AckLevel) pb.AckLevel {
	if a == AckNone {
		return pb.AckLevel_ACK_LEVEL_NO_ACK
	}
	return pb.AckLevel_ACK_LEVEL_QUORUM
}

// toProtoMessage converts an SDK [Message] into the wire [pb.PublishMessage].
func toProtoMessage(m Message) *pb.PublishMessage {
	pm := &pb.PublishMessage{
		Topic:   m.Topic,
		Payload: m.Payload,
	}
	if m.Delay > 0 {
		pm.DelayMs = m.Delay.Milliseconds()
	}
	if m.TTL > 0 {
		pm.TtlMs = m.TTL.Milliseconds()
	}
	if len(m.Indexes) > 0 {
		pm.Indexes = make([]*pb.Index, len(m.Indexes))
		for i, idx := range m.Indexes {
			if idx.IsString {
				pm.Indexes[i] = &pb.Index{Value: &pb.Index_StringValue{StringValue: idx.StringValue}}
			} else {
				pm.Indexes[i] = &pb.Index{Value: &pb.Index_Int64Value{Int64Value: idx.Int64Value}}
			}
		}
	}
	return pm
}

// sendFrame writes one frame to the stream, honouring ctx.
func sendFrame(
	ctx context.Context,
	stream grpc.BidiStreamingClient[pb.PublishBatch, pb.PublishBatchAck],
	frame *pb.PublishBatch,
) error {
	type result struct{ err error }
	ch := make(chan result, 1)
	go func() { ch <- result{err: stream.Send(frame)} }()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case r := <-ch:
		return r.err
	}
}

// recvAck reads one ack frame from the stream, honouring ctx.
func recvAck(
	ctx context.Context,
	stream grpc.BidiStreamingClient[pb.PublishBatch, pb.PublishBatchAck],
) (*pb.PublishBatchAck, error) {
	type result struct {
		ack *pb.PublishBatchAck
		err error
	}
	ch := make(chan result, 1)
	go func() {
		ack, err := stream.Recv()
		ch <- result{ack: ack, err: err}
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case r := <-ch:
		return r.ack, r.err
	}
}

// wrapStreamErr converts low-level send/recv errors into SDK sentinels.
func wrapStreamErr(op string, err error) error {
	if err == nil {
		return nil
	}
	if err == io.EOF {
		return ErrStreamClosed
	}
	st, ok := status.FromError(err)
	if ok {
		switch st.Code() {
		case codes.Canceled, codes.Unavailable:
			return ErrStreamClosed
		}
	}
	return fmt.Errorf("futureq: %s: %w", op, err)
}
