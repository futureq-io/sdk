package futureq

import (
	"context"
	"fmt"
	"io"
	"runtime/debug"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/futureq-io/protocol/proto/go"
)

// HandlerFunc is the callback type passed to [Consumer.Subscribe].
//
// The function is invoked once per delivered message. The return value
// controls the acknowledgement sent back to the broker:
//   - Return nil to ACK. The broker deletes the message; it will not
//     be redelivered.
//   - Return any non-nil error to NACK. The broker re-dispatches the
//     message to another consumer on the next dispatcher tick.
//
// The handler MUST NOT block indefinitely. If it panics, the consumer
// NACKs the message and logs the panic to stderr.
type HandlerFunc func(msg Delivery) error

// Consumer subscribes to a FutureQ topic and group and processes
// messages as they become due.
//
// Internally it maintains a bi-directional gRPC stream
// ([FutureQConsumer.Subscribe]) connected to the current Raft leader.
// When the leader changes, the consumer tears down the stream and
// re-subscribes against the new leader.
//
// Create a Consumer via [Client.NewConsumer].
// A Consumer is NOT safe for concurrent use across goroutines; only one
// goroutine should call [Consumer.Subscribe] at a time.
type Consumer struct {
	client *Client
	cfg    consumerConfig
	topic  string
	group  string

	mu     sync.Mutex
	stream grpc.BidiStreamingClient[pb.ConsumerFrame, pb.QueueMessage]
	conn   *grpc.ClientConn
	closed bool
	cancel context.CancelFunc
}

// ConsumerOption is a functional option for [Client.NewConsumer].
type ConsumerOption func(*consumerConfig)

type consumerConfig struct {
	// ackTimeout is the per-ACK send timeout. Defaults to 5 s.
	ackTimeout time.Duration

	// concurrency is the maximum number of handler goroutines that may
	// run in parallel. Defaults to 1 (serial, in-order processing).
	concurrency int
}

func defaultConsumerConfig() consumerConfig {
	return consumerConfig{
		ackTimeout:  5 * time.Second,
		concurrency: 1,
	}
}

// WithAckTimeout sets the maximum time to wait when sending an ACK or
// NACK back to the server. Defaults to 5 s.
func WithAckTimeout(d time.Duration) ConsumerOption {
	return func(c *consumerConfig) { c.ackTimeout = d }
}

// WithConcurrency sets the maximum number of handler goroutines that
// may run in parallel. Defaults to 1 (serial delivery order). Values
// less than 1 are clamped to 1.
func WithConcurrency(n int) ConsumerOption {
	return func(c *consumerConfig) {
		if n < 1 {
			n = 1
		}
		c.concurrency = n
	}
}

// NewConsumer opens a subscribe stream against the current cluster
// leader for (topic, group) and returns a ready [Consumer].
//
// Consumers sharing the same group compete for messages (one delivery
// per group); different groups on the same topic each receive an
// independent copy (fan-out).
//
// The context controls the lifetime of the underlying stream.
func (c *Client) NewConsumer(
	ctx context.Context,
	topic, group string,
	opts ...ConsumerOption,
) (*Consumer, error) {
	cfg := defaultConsumerConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	streamCtx, cancel := context.WithCancel(ctx)
	consumer := &Consumer{
		client: c,
		cfg:    cfg,
		topic:  topic,
		group:  group,
		cancel: cancel,
	}
	if err := consumer.reconnect(streamCtx); err != nil {
		cancel()
		return nil, err
	}
	return consumer, nil
}

// reconnect dials the current leader and re-opens the subscribe stream.
// The caller must hold c.mu.
func (c *Consumer) reconnect(ctx context.Context) error {
	addr, _ := c.client.tracker.Leader()
	if addr == "" {
		if _, err := c.client.tracker.refreshOnce(ctx); err != nil {
			return ErrNoLeader
		}
		addr, _ = c.client.tracker.Leader()
		if addr == "" {
			return ErrNoLeader
		}
	}

	c.closeStreamLocked()

	conn, err := c.client.tracker.dial(ctx, addr)
	if err != nil {
		return fmt.Errorf("futureq: dial leader %s: %w", addr, err)
	}
	cli := pb.NewFutureQConsumerClient(conn)
	stream, err := cli.Subscribe(ctx)
	if err != nil {
		conn.Close()
		return fmt.Errorf("futureq: open subscribe stream: %w", err)
	}

	// First frame must be SubscribeInit.
	init := &pb.ConsumerFrame{
		Body: &pb.ConsumerFrame_Init{
			Init: &pb.SubscribeInit{
				Topic:   c.topic,
				GroupId: c.group,
			},
		},
	}
	if err := stream.Send(init); err != nil {
		_ = stream.CloseSend()
		conn.Close()
		return fmt.Errorf("futureq: send subscribe init: %w", err)
	}

	c.conn = conn
	c.stream = stream
	return nil
}

// closeStreamLocked tears down the current stream, if any.
// The caller must hold c.mu.
func (c *Consumer) closeStreamLocked() {
	if c.stream != nil {
		_ = c.stream.CloseSend()
		c.stream = nil
	}
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
}

// Subscribe blocks and invokes handler for every message delivered by
// the broker. It returns only when the stream is closed (via [Close],
// context cancellation, or a non-recoverable transport error).
//
// # Message ordering
//
// When [WithConcurrency] is 1 (the default), messages are processed
// serially and in delivery order. With higher concurrency, ordering is
// not guaranteed.
//
// # Error handling
//
// Returning a non-nil error from handler NACKs the message; the broker
// redelivers it on the next dispatcher tick. A NACK does not stop the
// subscription loop.
//
// A panic in handler is recovered, the message is NACKed, and the panic
// value is printed to stderr.
//
// Subscribe returns nil when the stream was closed cleanly. It returns
// a non-nil error for unexpected transport failures.
func (c *Consumer) Subscribe(ctx context.Context, handler HandlerFunc) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrClosed
	}
	stream := c.stream
	c.mu.Unlock()

	sem := make(chan struct{}, c.cfg.concurrency)
	ackCh := make(chan *pb.AckRequest, c.cfg.concurrency+1)
	errCh := make(chan error, 1)

	go c.ackSender(ctx, stream, ackCh, errCh)

	for {
		msg, err := stream.Recv()
		if err != nil {
			close(ackCh)
			<-errCh

			if err == io.EOF {
				return nil
			}
			st, ok := status.FromError(err)
			if ok && (st.Code() == codes.Canceled || st.Code() == codes.Unavailable) {
				return nil
			}
			return fmt.Errorf("futureq: consumer recv: %w", err)
		}

		d := Delivery{
			Topic:       msg.GetTopic(),
			Payload:     msg.GetPayload(),
			EnqueuedAt:  time.UnixMilli(msg.GetEnqueuedAtUnixMs()),
			Delay:       time.Duration(msg.GetDelayMs()) * time.Millisecond,
			deliveryTag: msg.GetDeliveryTag(),
		}

		sem <- struct{}{}
		go func(del Delivery) {
			defer func() { <-sem }()
			ack := c.invokeHandler(handler, del)
			select {
			case ackCh <- ack:
			case <-ctx.Done():
			}
		}(d)

		select {
		case err := <-errCh:
			if err != nil {
				return fmt.Errorf("futureq: consumer ack sender: %w", err)
			}
		default:
		}
	}
}

// ackSender owns all writes to the stream after the initial
// SubscribeInit. It runs until ackCh is closed.
func (c *Consumer) ackSender(
	ctx context.Context,
	stream grpc.BidiStreamingClient[pb.ConsumerFrame, pb.QueueMessage],
	ackCh <-chan *pb.AckRequest,
	errCh chan<- error,
) {
	for ack := range ackCh {
		frame := &pb.ConsumerFrame{Body: &pb.ConsumerFrame_Ack{Ack: ack}}

		sendCtx, cancel := context.WithTimeout(ctx, c.cfg.ackTimeout)
		err := sendConsumerFrame(sendCtx, stream, frame)
		cancel()
		if err != nil {
			select {
			case errCh <- err:
			default:
			}
			return
		}
	}
	errCh <- nil
}

// invokeHandler calls handler in a deferred-recover wrapper and
// converts the result into an AckRequest.
func (c *Consumer) invokeHandler(handler HandlerFunc, d Delivery) *pb.AckRequest {
	success := true
	func() {
		defer func() {
			if r := recover(); r != nil {
				success = false
				fmt.Printf("futureq: handler panicked: %v\n%s\n", r, debug.Stack())
			}
		}()
		if err := handler(d); err != nil {
			success = false
		}
	}()
	return &pb.AckRequest{
		Success:     success,
		DeliveryTag: d.deliveryTag,
	}
}

// Close cancels the underlying stream context, causing [Subscribe] to
// return. In-flight handler invocations are allowed to finish.
//
// Safe to call more than once.
func (c *Consumer) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	c.cancel()
	c.closeStreamLocked()
	return nil
}

// sendConsumerFrame writes a single ConsumerFrame honouring ctx.
func sendConsumerFrame(
	ctx context.Context,
	stream grpc.BidiStreamingClient[pb.ConsumerFrame, pb.QueueMessage],
	frame *pb.ConsumerFrame,
) error {
	type result struct{ err error }
	ch := make(chan result, 1)
	go func() { ch <- result{err: stream.Send(frame)} }()
	select {
	case <-ctx.Done():
		return fmt.Errorf("futureq: consumer frame send: %w", ctx.Err())
	case r := <-ch:
		if r.err != nil && r.err != io.EOF {
			return fmt.Errorf("futureq: consumer frame send: %w", r.err)
		}
		return nil
	}
}
