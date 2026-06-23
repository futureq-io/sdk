package futureq

import (
	"errors"
	"fmt"
)

// Sentinel errors returned by the SDK.
// Use [errors.Is] to test for them:
//
//	if errors.Is(err, futureq.ErrNotLeader) { … }
var (
	// ErrNotLeader is returned by [Producer.Publish] when the connected node is
	// not the current Raft cluster leader and therefore cannot accept writes.
	// The caller should retry against the leader node.
	ErrNotLeader = errors.New("futureq: node is not the cluster leader")

	// ErrStreamClosed is returned when the underlying gRPC bi-directional
	// stream has been closed by the server or the network.  The [Producer] or
	// [Consumer] should be discarded and a new one created.
	ErrStreamClosed = errors.New("futureq: stream closed")

	// ErrPublishFailed is returned by [Producer.Publish] when the server
	// acknowledged the message but reported an application-level error.
	// The wrapped error message contains the server's error string.
	ErrPublishFailed = errors.New("futureq: publish failed")

	// ErrHandlerPanic is returned by [Consumer.Subscribe] when the message
	// handler panicked.  The wrapped value contains the recovered panic value.
	ErrHandlerPanic = errors.New("futureq: handler panicked")

	// ErrClosed is returned when a method is called on a [Producer] or
	// [Consumer] that has already been closed.
	ErrClosed = errors.New("futureq: client is closed")
)

// PublishError is the structured error type returned when a single Publish
// call is acknowledged by the server with success=false.
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
