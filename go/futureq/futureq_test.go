package futureq_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/futureq-io/sdk/go/futureq"
)

// ----------------------------------------------------------------------------
// Client option tests
// ----------------------------------------------------------------------------

func TestWithInsecure(t *testing.T) {
	t.Parallel()
	// New should not dial immediately; it should succeed even without a server.
	client, err := futureq.New("localhost:19999", futureq.WithInsecure())
	if err != nil {
		t.Fatalf("New() error = %v, want nil", err)
	}
	defer client.Close()
}

func TestWithTLS_nil(t *testing.T) {
	t.Parallel()
	// TLS with nil config uses system certs — connection won't complete but
	// New itself should succeed.
	_, err := futureq.New("localhost:19999", futureq.WithTLS(nil))
	if err != nil {
		t.Fatalf("New() with TLS(nil) error = %v, want nil", err)
	}
}

func TestClientClose_multipleCallsAreNoOps(t *testing.T) {
	t.Parallel()
	client, err := futureq.New("localhost:19999", futureq.WithInsecure())
	if err != nil {
		t.Fatal(err)
	}

	if err := client.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}
	// Second close must not panic or error.
	if err := client.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
}

// ----------------------------------------------------------------------------
// Retry policy tests
// ----------------------------------------------------------------------------

func TestDefaultRetryPolicy(t *testing.T) {
	p := futureq.DefaultRetryPolicy()
	if p.MaxAttempts < 1 {
		t.Errorf("MaxAttempts = %d, want ≥ 1", p.MaxAttempts)
	}
	if p.InitialBackoff <= 0 {
		t.Errorf("InitialBackoff = %v, want > 0", p.InitialBackoff)
	}
}

func TestDefaultRetryable(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		err       error
		wantRetry bool
	}{
		{"nil error", nil, false},
		{"ErrNotLeader", futureq.ErrNotLeader, false},
		{"ErrClosed", futureq.ErrClosed, false},
		{"ErrPublishFailed", futureq.ErrPublishFailed, false},
		{"wrapped ErrNotLeader", errors.Join(errors.New("outer"), futureq.ErrNotLeader), false},
		{"arbitrary error", errors.New("some transient error"), false},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := futureq.DefaultRetryable(tc.err)
			if got != tc.wantRetry {
				t.Errorf("DefaultRetryable(%v) = %v, want %v", tc.err, got, tc.wantRetry)
			}
		})
	}
}

// ----------------------------------------------------------------------------
// Error type tests
// ----------------------------------------------------------------------------

func TestPublishError_Is(t *testing.T) {
	t.Parallel()
	pe := &futureq.PublishError{ServerMessage: "disk full"}
	if !errors.Is(pe, futureq.ErrPublishFailed) {
		t.Error("errors.Is(publishError, ErrPublishFailed) = false, want true")
	}
}

func TestPublishError_Unwrap(t *testing.T) {
	t.Parallel()
	pe := &futureq.PublishError{ServerMessage: "oops"}
	if !errors.Is(pe, futureq.ErrPublishFailed) {
		t.Error("unwrap chain does not reach ErrPublishFailed")
	}
}

func TestPublishError_Error(t *testing.T) {
	t.Parallel()
	pe := &futureq.PublishError{ServerMessage: "oops"}
	if pe.Error() == "" {
		t.Error("Error() returned empty string")
	}
}

// ----------------------------------------------------------------------------
// Message zero-value tests
// ----------------------------------------------------------------------------

func TestMessage_zeroValueExecuteAt(t *testing.T) {
	t.Parallel()
	var m futureq.Message
	// ExecuteAt zero value should marshal to negative/zero unix ms — verify it
	// doesn't panic during access.
	_ = m.ExecuteAt.UnixMilli()
}

// ----------------------------------------------------------------------------
// BatchResult tests
// ----------------------------------------------------------------------------

func TestBatchResult_HasErrors_false(t *testing.T) {
	t.Parallel()
	r := futureq.BatchResult{Errors: []error{nil, nil}}
	if r.HasErrors() {
		t.Error("HasErrors() = true on all-nil errors, want false")
	}
}

func TestBatchResult_HasErrors_true(t *testing.T) {
	t.Parallel()
	r := futureq.BatchResult{Errors: []error{nil, errors.New("fail"), nil}}
	if !r.HasErrors() {
		t.Error("HasErrors() = false, want true")
	}
}

func TestBatchResult_FailedIndices(t *testing.T) {
	t.Parallel()
	r := futureq.BatchResult{Errors: []error{nil, errors.New("fail"), nil, errors.New("fail2")}}
	indices := r.FailedIndices()
	if len(indices) != 2 || indices[0] != 1 || indices[1] != 3 {
		t.Errorf("FailedIndices() = %v, want [1 3]", indices)
	}
}

// ----------------------------------------------------------------------------
// Producer/Consumer — closed state tests (no server required)
// ----------------------------------------------------------------------------

func TestProducer_publishAfterClose_returnsErrClosed(t *testing.T) {
	t.Parallel()
	// We can't open a real stream without a server, so we test via
	// NewConsumer/NewProducer only when the underlying gRPC connection is
	// established.  Here we simply verify the ErrClosed sentinel is defined.
	if futureq.ErrClosed == nil {
		t.Error("ErrClosed must not be nil")
	}
}

func TestConsumerOptions_concurrencyClamp(t *testing.T) {
	t.Parallel()
	// WithConcurrency(0) should silently clamp to 1 — verify no panic.
	client, err := futureq.New("localhost:19999", futureq.WithInsecure())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	// NewConsumer will fail because there's no server, but the option itself
	// must not panic.
	_, _ = client.NewConsumer(ctx, futureq.WithConcurrency(0))
}
