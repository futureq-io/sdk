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

// Producer schedules messages for future delivery on a FutureQ server.
//
// Internally it maintains a single long-lived gRPC bi-directional streaming
// RPC ([FutureQProducer.PublishStream]).  Sends and receives on this stream
// are multiplexed safely across goroutines using an internal mutex.
//
// Create a Producer via [Client.NewProducer].  A Producer must be closed with
// [Producer.Close] when no longer needed to release server-side resources.
//
// A Producer is safe for concurrent use by multiple goroutines.
type Producer struct {
	stream  grpc.BidiStreamingClient[pb.StreamPublishRequest, pb.StreamPublishAck]
	mu      sync.Mutex
	closed  bool
	timeout time.Duration
}

// ProducerOption is a functional option for [Client.NewProducer].
type ProducerOption func(*producerConfig)

type producerConfig struct {
	// publishTimeout is the per-publish operation timeout for waiting for the
	// server ACK.  Defaults to 10 seconds.
	publishTimeout time.Duration
}

func defaultProducerConfig() producerConfig {
	return producerConfig{
		publishTimeout: 10 * time.Second,
	}
}

// WithPublishTimeout sets the maximum duration to wait for a server ACK after
// sending a single message.  If the server does not respond within this window,
// Publish returns a timeout error.  Defaults to 10 seconds.
func WithPublishTimeout(d time.Duration) ProducerOption {
	return func(c *producerConfig) {
		c.publishTimeout = d
	}
}

// NewProducer opens a bidirectional streaming RPC to the FutureQ server and
// returns a ready [Producer].
//
// The context controls the lifetime of the underlying stream.  Cancel it (or
// let it expire) to tear down the stream asynchronously; the producer will
// return [ErrStreamClosed] on the next [Producer.Publish] call.
//
//	producer, err := client.NewProducer(ctx, futureq.WithPublishTimeout(5*time.Second))
func (c *Client) NewProducer(ctx context.Context, opts ...ProducerOption) (*Producer, error) {
	cfg := defaultProducerConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	client := pb.NewFutureQProducerClient(c.conn)

	stream, err := client.PublishStream(ctx)
	if err != nil {
		return nil, fmt.Errorf("futureq: open producer stream: %w", err)
	}

	return &Producer{
		stream:  stream,
		timeout: cfg.publishTimeout,
	}, nil
}

// Publish schedules a [Message] for future delivery and blocks until the
// server acknowledges the write.
//
// The method is safe for concurrent use; multiple goroutines may call Publish
// on the same Producer simultaneously.
//
// Possible errors:
//   - [ErrClosed] — the Producer has been closed.
//   - [ErrNotLeader] — the server node is not the Raft leader.
//   - [ErrPublishFailed] (via [errors.As]) — the server persisted the request
//     but returned an application error; inspect [PublishError.ServerMessage].
//   - A gRPC status error — e.g. codes.Unavailable if the server is down.
//
// Example:
//
//	err := producer.Publish(ctx, futureq.Message{
//	    Topic:     "email-notifications",
//	    Payload:   []byte(`{"to":"user@example.com"}`),
//	    ExecuteAt: time.Now().Add(10 * time.Minute),
//	})
func (p *Producer) Publish(ctx context.Context, msg Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return ErrClosed
	}

	req := &pb.StreamPublishRequest{
		Topic:           msg.Topic,
		Payload:         msg.Payload,
		ExecuteAtUnixMs: msg.ExecuteAt.UnixMilli(),
	}

	// Apply the publish timeout on top of any deadline already in ctx.
	sendCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	// Send is blocking; we wrap it in a goroutine so we can respect sendCtx.
	type sendResult struct{ err error }
	sendCh := make(chan sendResult, 1)
	go func() {
		sendCh <- sendResult{err: p.stream.Send(req)}
	}()

	select {
	case <-sendCtx.Done():
		return fmt.Errorf("futureq: publish send: %w", sendCtx.Err())
	case res := <-sendCh:
		if res.err != nil {
			if res.err == io.EOF {
				return ErrStreamClosed
			}
			return fmt.Errorf("futureq: publish send: %w", res.err)
		}
	}

	// Wait for the server ACK.
	type recvResult struct {
		ack *pb.StreamPublishAck
		err error
	}
	recvCh := make(chan recvResult, 1)
	go func() {
		ack, err := p.stream.Recv()
		recvCh <- recvResult{ack: ack, err: err}
	}()

	select {
	case <-sendCtx.Done():
		return fmt.Errorf("futureq: publish recv ack: %w", sendCtx.Err())
	case res := <-recvCh:
		if res.err != nil {
			if res.err == io.EOF {
				return ErrStreamClosed
			}
			return fmt.Errorf("futureq: publish recv ack: %w", res.err)
		}

		if !res.ack.GetSuccess() {
			msg := res.ack.GetErrorMessage()
			if strings.Contains(msg, "not the cluster leader") {
				return ErrNotLeader
			}
			return &PublishError{ServerMessage: msg}
		}
	}

	return nil
}

// PublishBatch schedules multiple messages atomically and collects per-message
// acknowledgements.  It returns a [BatchResult] that maps each message index
// to its error (nil meaning success).
//
// PublishBatch is optimised for throughput: it sends all messages before
// reading ACKs, which reduces round-trip latency on high-latency links.
//
// The batch is sent under a single mutex acquisition, so no other Publish call
// can interleave between the sends.
//
//	results, err := producer.PublishBatch(ctx, []futureq.Message{
//	    {Topic: "t", Payload: []byte("a"), ExecuteAt: time.Now().Add(1*time.Minute)},
//	    {Topic: "t", Payload: []byte("b"), ExecuteAt: time.Now().Add(2*time.Minute)},
//	})
//	if err != nil {
//	    // transport-level error
//	}
//	for i, e := range results.Errors {
//	    if e != nil {
//	        fmt.Printf("message %d failed: %v\n", i, e)
//	    }
//	}
// func (p *Producer) PublishBatch(ctx context.Context, msgs []Message) (BatchResult, error) {
// 	if len(msgs) == 0 {
// 		return BatchResult{}, nil
// 	}

// 	p.mu.Lock()
// 	defer p.mu.Unlock()

// 	if p.closed {
// 		return BatchResult{}, ErrClosed
// 	}

// 	// Apply batch timeout on top of any deadline already in ctx.
// 	// Scale the timeout with the number of messages.
// 	batchTimeout := p.timeout + time.Duration(len(msgs))*10*time.Millisecond
// 	batchCtx, cancel := context.WithTimeout(ctx, batchTimeout)
// 	defer cancel()

// 	// Serialise the requests up-front so we can fail fast on marshal errors
// 	// without partially sending the batch.
// 	reqs := make([]*pb.StreamPublishRequest, len(msgs))
// 	for i, m := range msgs {
// 		reqs[i] = &pb.StreamPublishRequest{
// 			Topic:           m.Topic,
// 			Payload:         m.Payload,
// 			ExecuteAtUnixMs: m.ExecuteAt.UnixMilli(),
// 		}
// 	}

// 	// Send phase
// 	for _, req := range reqs {
// 		if err := batchCtx.Err(); err != nil {
// 			return BatchResult{}, fmt.Errorf("futureq: batch send cancelled: %w", err)
// 		}
// 		if err := p.stream.Send(req); err != nil {
// 			if err == io.EOF {
// 				return BatchResult{}, ErrStreamClosed
// 			}
// 			return BatchResult{}, fmt.Errorf("futureq: batch send: %w", err)
// 		}
// 	}

// 	// Receive phase — one ACK per sent message (server guarantees order).
// 	result := BatchResult{Errors: make([]error, len(msgs))}
// 	for i := range msgs {
// 		if err := batchCtx.Err(); err != nil {
// 			return result, fmt.Errorf("futureq: batch recv ack cancelled at index %d: %w", i, err)
// 		}

// 		ack, err := p.stream.Recv()
// 		if err != nil {
// 			if err == io.EOF {
// 				return result, ErrStreamClosed
// 			}
// 			return result, fmt.Errorf("futureq: batch recv ack at index %d: %w", i, err)
// 		}

// 		if !ack.GetSuccess() {
// 			serverMsg := ack.GetErrorMessage()
// 			if strings.Contains(serverMsg, "not the cluster leader") {
// 				result.Errors[i] = ErrNotLeader
// 			} else {
// 				result.Errors[i] = &PublishError{ServerMessage: serverMsg}
// 			}
// 		}
// 	}

// 	return result, nil
// }

// Close gracefully closes the producer stream, flushing any pending messages.
// After Close returns, further calls to [Producer.Publish] return [ErrClosed].
//
// It is safe to call Close more than once; subsequent calls are no-ops.
func (p *Producer) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}
	p.closed = true

	if err := p.stream.CloseSend(); err != nil {
		// Ignore EOF — the server has already closed its side.
		if err == io.EOF {
			return nil
		}
		st, ok := status.FromError(err)
		if ok && (st.Code() == codes.Canceled || st.Code() == codes.Unavailable) {
			return nil
		}
		return fmt.Errorf("futureq: close producer: %w", err)
	}
	return nil
}

// BatchResult holds the per-message outcomes of a [Producer.PublishBatch] call.
type BatchResult struct {
	// Errors is a slice parallel to the input messages slice.
	// Errors[i] is nil when message i was acknowledged successfully, or a
	// non-nil error describing why message i was rejected.
	Errors []error
}

// HasErrors reports whether any message in the batch was rejected.
func (r BatchResult) HasErrors() bool {
	for _, e := range r.Errors {
		if e != nil {
			return true
		}
	}
	return false
}

// FailedIndices returns the indices of messages that were rejected.
func (r BatchResult) FailedIndices() []int {
	var out []int
	for i, e := range r.Errors {
		if e != nil {
			out = append(out, i)
		}
	}
	return out
}
