package futureq

import (
	"errors"
	"fmt"
)

// Sentinel errors returned by the SDK.
// Use [errors.Is] to test for them:
//
//	if errors.Is(err, futureq.ErrNoLeader) { … }
var (
	// ErrNotLeader is returned by [Producer.PublishBatch] when the
	// connected node is not the current Raft cluster leader and therefore
	// cannot accept writes. The SDK usually recovers from this
	// automatically by re-resolving the leader through the topology
	// tracker; callers may also retry manually.
	ErrNotLeader = errors.New("futureq: node is not the cluster leader")

	// ErrNoLeader is returned when the SDK does not currently know of any
	// live leader to route a request to (e.g. immediately after startup
	// before the first topology refresh, or during a rolling restart).
	// Retrying after a short backoff usually succeeds.
	ErrNoLeader = errors.New("futureq: no known cluster leader")

	// ErrStreamClosed is returned when the underlying gRPC bi-directional
	// stream has been closed by the server or the network. The [Producer]
	// or [Consumer] should be discarded and a new one created.
	ErrStreamClosed = errors.New("futureq: stream closed")

	// ErrPublishFailed is returned by [Producer.PublishBatch] when the
	// server acknowledged the batch but reported an application-level
	// error. Use [errors.As] to recover the structured [PublishError].
	ErrPublishFailed = errors.New("futureq: publish failed")

	// ErrHandlerPanic is returned by [Consumer.Subscribe] when the message
	// handler panicked. The wrapped value contains the recovered panic value.
	ErrHandlerPanic = errors.New("futureq: handler panicked")

	// ErrClosed is returned when a method is called on a [Client],
	// [Producer] or [Consumer] that has already been closed.
	ErrClosed = errors.New("futureq: client is closed")

	// ErrTopologyUnavailable is returned when the SDK cannot obtain the
	// cluster topology from any of the configured addresses (all seeds
	// unreachable and no cached leader).
	ErrTopologyUnavailable = errors.New("futureq: cluster topology unavailable")
)

// PublishError is the structured error type returned when a batch is
// acknowledged by the server with success=false.
//
// It wraps [ErrPublishFailed] and additionally carries the server-supplied
// error message.
type PublishError struct {
	// ServerMessage is the raw error string reported by the FutureQ server.
	ServerMessage string
}

// Error implements the error interface.
func (e *PublishError) Error() string {
	return fmt.Sprintf("futureq: publish failed: %s", e.ServerMessage)
}

// Is reports whether this error matches target.
// It returns true when target is [ErrPublishFailed], allowing callers to use
// errors.Is(err, futureq.ErrPublishFailed).
func (e *PublishError) Is(target error) bool {
	return target == ErrPublishFailed
}

// Unwrap returns [ErrPublishFailed] to support errors.Is chain traversal.
func (e *PublishError) Unwrap() error {
	return ErrPublishFailed
}
