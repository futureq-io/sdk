package futureq

import (
	"context"
	"fmt"
	"io"
	"runtime/debug"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/futureq-io/protocol/proto/go"
)

// HandlerFunc is the callback type passed to [Consumer.Subscribe].
// 
// The function is called once for every message delivered from the server.
// The return value controls acknowledgement:
//   - Return nil to positively acknowledge (ACK) the message.  The server
//     will delete the message from its store; it will not be redelivered.

//   - Return any non-nil error to negatively acknowledge (NACK) the message.
//     The server will redeliver it on the next dispatch pass (typically after
//     5 seconds).
//
// The handler MUST NOT block indefinitely.  If the handler panics, [Consumer]
// sends a NACK and wraps the panic value as [ErrHandlerPanic].
type HandlerFunc func(msg Delivery) error

// Consumer subscribes to a FutureQ queue and processes messages as they
// become due.
//
// Internally it maintains a long-lived gRPC bi-directional streaming RPC
// ([FutureQConsumer.Subscribe]).  The server pushes [QueueMessage] frames
// down the stream; the consumer replies with [AckRequest] frames.
//
// Create a Consumer via [Client.NewConsumer].
// A Consumer must be closed with [Consumer.Close] when no longer needed.
//
// A Consumer is NOT safe for concurrent use across multiple goroutines;
// only one goroutine should call [Consumer.Subscribe] at a time.
type Consumer struct {
	stream      grpc.BidiStreamingClient[pb.AckRequest, pb.QueueMessage]
	ackTimeout  time.Duration
	concurrency int
	closed      bool
	cancelFn    context.CancelFunc
}

// ConsumerOption is a functional option for [Client.NewConsumer].
type ConsumerOption func(*consumerConfig)

type consumerConfig struct {
	// ackTimeout is the per-ACK send timeout.  Defaults to 5 seconds.
	ackTimeout time.Duration

	// concurrency controls how many handler goroutines may run simultaneously.
	// Defaults to 1 (serial processing, preserving ordering within a stream).
	concurrency int
}

func defaultConsumerConfig() consumerConfig {
	return consumerConfig{
		ackTimeout:  5 * time.Second,
		concurrency: 1,
	}
}

// WithAckTimeout sets the maximum time to wait when sending an ACK or NACK
// back to the server.  Defaults to 5 seconds.
func WithAckTimeout(d time.Duration) ConsumerOption {
	return func(c *consumerConfig) {
		c.ackTimeout = d
	}
}

// WithConcurrency sets the maximum number of message handler goroutines that
// may run in parallel.  Defaults to 1 (serial delivery order).
//
// Increasing concurrency can improve throughput when the handler performs
// I/O-bound work, but ordering guarantees are relaxed.
//
// The value must be ≥ 1; values < 1 are silently clamped to 1.
func WithConcurrency(n int) ConsumerOption {
	return func(c *consumerConfig) {
		if n < 1 {
			n = 1
		}
		c.concurrency = n
	}
}

// NewConsumer opens a bidirectional streaming RPC to the FutureQ server and
// returns a ready [Consumer].
//
// The context controls the lifetime of the underlying stream.  Cancel it to
// terminate the subscription gracefully; [Consumer.Subscribe] will return.
//
//	consumer, err := client.NewConsumer(ctx,
//	    futureq.WithConcurrency(4),
//	    futureq.WithAckTimeout(3*time.Second),
//	)
func (c *Client) NewConsumer(ctx context.Context, opts ...ConsumerOption) (*Consumer, error) {
	cfg := defaultConsumerConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	client := pb.NewFutureQConsumerClient(c.conn)

	// Wrap ctx so we can cancel the stream from Consumer.Close.
	streamCtx, cancel := context.WithCancel(ctx)

	stream, err := client.Subscribe(streamCtx)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("futureq: open consumer stream: %w", err)
	}

	return &Consumer{
		stream:      stream,
		ackTimeout:  cfg.ackTimeout,
		concurrency: cfg.concurrency,
		cancelFn:    cancel,
	}, nil
}

// Subscribe blocks and invokes handler for every message delivered by the
// server.  It returns only when the stream is closed (by calling [Close],
// cancelling the context, or a network error).
//
// # Message ordering
//
// When [WithConcurrency] is 1 (the default), messages are processed serially
// and in delivery order.  With higher concurrency, ordering is not guaranteed.
//
// # Error handling
//
// If handler returns a non-nil error, the message is NACKed and the server
// will redeliver it.  A NACK does not stop the subscription loop; Subscribe
// continues to process subsequent messages.
//
// If handler panics, Subscribe recovers the panic, NACKs the message, and
// continues.  The recovered panic value is logged to stderr.
//
// Subscribe returns nil when the stream was closed cleanly (context cancelled
// or [Close] called).  It returns a non-nil error for unexpected transport
// failures.
//
//	err := consumer.Subscribe(ctx, func(d futureq.Delivery) error {
//	    return process(d.Payload)
//	})
//	if err != nil {
//	    log.Printf("consumer error: %v", err)
//	}
func (c *Consumer) Subscribe(ctx context.Context, handler HandlerFunc) error {
	if c.closed {
		return ErrClosed
	}

	// sem limits the number of concurrent handler goroutines.
	sem := make(chan struct{}, c.concurrency)

	// ackCh serialises ACK/NACK writes back to the server.
	// We use a buffered channel sized to concurrency+1 to prevent handler
	// goroutines from blocking when the ACK sender is busy.
	ackCh := make(chan *pb.AckRequest, c.concurrency+1)

	// errCh collects the first fatal error from the ACK sender goroutine.
	errCh := make(chan error, 1)

	// ACK sender goroutine — one goroutine owns all writes to the stream.
	go func() {
		for ack := range ackCh {
			ackCtx, cancel := context.WithTimeout(ctx, c.ackTimeout)
			err := sendAck(ackCtx, c.stream, ack)
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
	}()

	// Receive loop.
	for {
		msg, err := c.stream.Recv()
		if err != nil {
			// Close the ack channel so the sender goroutine drains and exits.
			close(ackCh)
			<-errCh // wait for sender to finish

			if err == io.EOF {
				return nil
			}
			st, ok := status.FromError(err)
			if ok && (st.Code() == codes.Canceled || st.Code() == codes.Unavailable) {
				return nil
			}
			return fmt.Errorf("futureq: consumer recv: %w", err)
		}

		delivery := Delivery{
			Payload:     msg.GetPayload(),
			deliveryTag: msg.GetDeliveryTag(),
		}

		// Acquire a handler slot (blocks if at concurrency limit).
		sem <- struct{}{}

		go func(d Delivery) {
			defer func() { <-sem }() // release slot when done

			ack := c.invokeHandler(handler, d)

			select {
			case ackCh <- ack:
			case <-ctx.Done():
			}
		}(delivery)

		// Check if the ACK sender encountered a fatal error.
		select {
		case err := <-errCh:
			if err != nil {
				close(ackCh)
				return fmt.Errorf("futureq: consumer ack sender: %w", err)
			}
		default:
		}
	}
}

// invokeHandler calls handler in a deferred-recover wrapper.
// It returns an AckRequest with success=true on nil return, false otherwise.
func (c *Consumer) invokeHandler(handler HandlerFunc, d Delivery) *pb.AckRequest {
	success := true

	func() {
		defer func() {
			if r := recover(); r != nil {
				success = false
				// Print the panic to stderr so it is visible in logs even if
				// the caller does not check the error.
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

// Close cancels the underlying stream context, causing [Subscribe] to return.
// Any in-flight handler invocations are allowed to finish before the stream is
// torn down by the server.
//
// It is safe to call Close more than once; subsequent calls are no-ops.
func (c *Consumer) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true
	c.cancelFn()
	return nil
}

// sendAck writes a single AckRequest to the stream.
func sendAck(ctx context.Context, stream grpc.BidiStreamingClient[pb.AckRequest, pb.QueueMessage], ack *pb.AckRequest) error {
	type result struct{ err error }
	ch := make(chan result, 1)

	go func() {
		ch <- result{err: stream.Send(ack)}
	}()

	select {
	case <-ctx.Done():
		return fmt.Errorf("futureq: ack send: %w", ctx.Err())
	case r := <-ch:
		if r.err != nil && r.err != io.EOF {
			return fmt.Errorf("futureq: ack send: %w", r.err)
		}
		return nil
	}
}
