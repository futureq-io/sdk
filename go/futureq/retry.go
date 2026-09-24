package futureq

import (
	"context"
	"errors"
	"math"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RetryPolicy configures automatic retry behaviour for
// [Producer.PublishBatchWithRetry].
//
// Zero values are not meaningful; use [DefaultRetryPolicy] as a baseline
// and adjust individual fields as needed.
type RetryPolicy struct {
	// MaxAttempts is the maximum number of publish attempts, including the
	// initial one. A value of 1 means no retries.
	MaxAttempts int

	// InitialBackoff is the duration to wait before the first retry.
	InitialBackoff time.Duration

	// MaxBackoff caps the exponential back-off. Jitter is applied on top.
	MaxBackoff time.Duration

	// Multiplier is the factor by which the backoff grows on each attempt.
	Multiplier float64

	// RetryableFunc is an optional predicate that determines whether a
	// given error should trigger a retry. If nil, [DefaultRetryable] is used.
	RetryableFunc func(err error) bool
}

// DefaultRetryPolicy returns a RetryPolicy suitable for most production
// use cases: three attempts with exponential backoff starting at 100 ms.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts:    3,
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     5 * time.Second,
		Multiplier:     2.0,
	}
}

// DefaultRetryable is the default predicate used by
// [Producer.PublishBatchWithRetry].
//
// It retries transient errors (network timeouts, leader changes,
// Unavailable) but never permanent application errors like
// [ErrPublishFailed].
func DefaultRetryable(err error) bool {
	if err == nil {
		return false
	}
	// Leader / discovery errors are always worth retrying — the SDK
	// reconnects under the hood and the next attempt usually lands on the
	// new leader.
	if errors.Is(err, ErrNoLeader) || errors.Is(err, ErrNotLeader) || errors.Is(err, ErrStreamClosed) {
		return true
	}
	// Never retry permanent errors.
	if errors.Is(err, ErrPublishFailed) || errors.Is(err, ErrClosed) {
		return false
	}
	st, ok := status.FromError(err)
	if ok {
		switch st.Code() {
		case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted:
			return true
		}
	}
	return false
}

// PublishBatchWithRetry attempts to publish msgs up to
// policy.MaxAttempts times, pausing between attempts according to the
// exponential back-off defined in policy.
//
// It is the caller's responsibility to ensure the context has a deadline
// encompassing all attempts.
//
//	policy := futureq.DefaultRetryPolicy()
//	policy.MaxAttempts = 5
//	err := producer.PublishBatchWithRetry(ctx, msgs, futureq.AckQuorum, policy)
func (p *Producer) PublishBatchWithRetry(
	ctx context.Context,
	msgs []Message,
	ackLevel AckLevel,
	policy RetryPolicy,
) error {
	isRetryable := policy.RetryableFunc
	if isRetryable == nil {
		isRetryable = DefaultRetryable
	}

	backoff := policy.InitialBackoff
	var lastErr error

	for attempt := 0; attempt < policy.MaxAttempts; attempt++ {
		err := p.PublishBatch(ctx, msgs, ackLevel)
		if err == nil {
			return nil
		}
		lastErr = err
		if !isRetryable(err) {
			return err
		}

		if attempt < policy.MaxAttempts-1 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
			next := time.Duration(float64(backoff) * policy.Multiplier)
			backoff = time.Duration(math.Min(float64(next), float64(policy.MaxBackoff)))
		}
	}

	return lastErr
}

// PublishWithRetry is the single-message equivalent of
// [Producer.PublishBatchWithRetry].
func (p *Producer) PublishWithRetry(
	ctx context.Context,
	msg Message,
	policy RetryPolicy,
) error {
	return p.PublishBatchWithRetry(ctx, []Message{msg}, AckQuorum, policy)
}
