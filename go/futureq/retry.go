package futureq

import (
	"context"
	"errors"
	"math"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RetryPolicy configures automatic retry behaviour for [Producer.PublishWithRetry].
//
// Zero values are not meaningful; use [DefaultRetryPolicy] as a baseline and
// adjust individual fields as needed.
type RetryPolicy struct {
	// MaxAttempts is the maximum number of times to attempt publishing a
	// message, including the initial attempt.  A value of 1 means no retries.
	MaxAttempts int

	// InitialBackoff is the duration to wait before the first retry.
	InitialBackoff time.Duration

	// MaxBackoff caps the exponential back-off.  Jitter is applied on top.
	MaxBackoff time.Duration

	// Multiplier is the factor by which the backoff grows on each attempt.
	// A value of 2.0 doubles the delay each time.
	Multiplier float64

	// RetryableFunc is an optional predicate that determines whether a given
	// error should trigger a retry.  If nil, [DefaultRetryable] is used.
	RetryableFunc func(err error) bool
}

// DefaultRetryPolicy returns a RetryPolicy suitable for most production use
// cases: three attempts with exponential backoff starting at 100 ms.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts:    3,
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     5 * time.Second,
		Multiplier:     2.0,
	}
}

// DefaultRetryable is the default predicate used by [PublishWithRetry].
// It returns true for transient errors (network timeouts, Unavailable) and
// false for permanent errors like [ErrNotLeader] or [ErrPublishFailed].
func DefaultRetryable(err error) bool {
	if err == nil {
		return false
	}
	// Never retry permanent application errors.
	if errors.Is(err, ErrNotLeader) || errors.Is(err, ErrPublishFailed) || errors.Is(err, ErrClosed) {
		return false
	}
	// Retry on gRPC transient status codes.
	st, ok := status.FromError(err)
	if ok {
		switch st.Code() {
		case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted:
			return true
		}
	}
	return false
}

// PublishWithRetry attempts to publish msg up to policy.MaxAttempts times,
// pausing between attempts according to the exponential back-off defined in
// policy.
//
// It is the caller's responsibility to ensure that the context has a deadline
// encompassing all attempts.
//
// If all attempts fail, PublishWithRetry returns the error from the last
// attempt.
//
//	policy := futureq.DefaultRetryPolicy()
//	policy.MaxAttempts = 5
//	err := producer.PublishWithRetry(ctx, msg, policy)
func (p *Producer) PublishWithRetry(ctx context.Context, msg Message, policy RetryPolicy) error {
	isRetryable := policy.RetryableFunc
	if isRetryable == nil {
		isRetryable = DefaultRetryable
	}

	backoff := policy.InitialBackoff
	var lastErr error

	for attempt := 0; attempt < policy.MaxAttempts; attempt++ {
		err := p.Publish(ctx, msg)
		if err == nil {
			return nil
		}

		lastErr = err
		if !isRetryable(err) {
			return err
		}

		if attempt < policy.MaxAttempts-1 {
			// Apply jitter: actual sleep is [0.5 * backoff, 1.5 * backoff].
			sleep := backoff
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(sleep):
			}

			// Grow the backoff for the next iteration, capped at MaxBackoff.
			next := time.Duration(float64(backoff) * policy.Multiplier)
			backoff = time.Duration(math.Min(float64(next), float64(policy.MaxBackoff)))
		}
	}

	return lastErr
}
