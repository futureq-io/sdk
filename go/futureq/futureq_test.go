package futureq

import (
	"errors"
	"testing"
	"time"
)

func TestMessageToProtoRoundTrip(t *testing.T) {
	m := Message{
		Topic:   "events",
		Payload: []byte("hello"),
		Delay:   5 * time.Second,
		TTL:     time.Hour,
		Indexes: []Index{
			Int64Index(42),
			StringIndex("user:7"),
		},
	}
	pm := toProtoMessage(m)
	if pm.GetTopic() != m.Topic {
		t.Fatalf("topic mismatch: %q", pm.GetTopic())
	}
	if string(pm.GetPayload()) != string(m.Payload) {
		t.Fatalf("payload mismatch")
	}
	if pm.GetDelayMs() != 5000 {
		t.Fatalf("delay mismatch: %d", pm.GetDelayMs())
	}
	if pm.GetTtlMs() != int64(time.Hour/time.Millisecond) {
		t.Fatalf("ttl mismatch: %d", pm.GetTtlMs())
	}
	if got := pm.GetIndexes(); len(got) != 2 {
		t.Fatalf("expected 2 indexes, got %d", len(got))
	} else {
		if got[0].GetInt64Value() != 42 {
			t.Errorf("index 0: expected 42, got %d", got[0].GetInt64Value())
		}
		if got[1].GetStringValue() != "user:7" {
			t.Errorf("index 1: expected user:7, got %q", got[1].GetStringValue())
		}
	}
}

func TestAckLevelToProto(t *testing.T) {
	if toProtoAckLevel(AckQuorum) != 0 {
		t.Errorf("AckQuorum should map to 0")
	}
	if toProtoAckLevel(AckNone) != 1 {
		t.Errorf("AckNone should map to 1")
	}
}

func TestTopologyTracker_SetLeaderNotifiesOnChange(t *testing.T) {
	tracker := newTopologyTracker([]string{"a:1"}, time.Hour, nil)

	tracker.setLeader(1, "a:9000", Topology{LeaderNodeID: 1, LeaderAddress: "a:9000"})
	select {
	case <-tracker.Notify():
	default:
		t.Fatal("expected a notification on first set")
	}

	// Same address → no notification.
	tracker.setLeader(1, "a:9000", Topology{LeaderNodeID: 1, LeaderAddress: "a:9000"})
	select {
	case <-tracker.Notify():
		t.Fatal("did not expect a notification when address is unchanged")
	default:
	}

	// New address → notification.
	tracker.setLeader(2, "b:9000", Topology{LeaderNodeID: 2, LeaderAddress: "b:9000"})
	select {
	case <-tracker.Notify():
	default:
		t.Fatal("expected a notification on address change")
	}
}

func TestDefaultRetryable(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"no leader", ErrNoLeader, true},
		{"not leader", ErrNotLeader, true},
		{"stream closed", ErrStreamClosed, true},
		{"publish failed", &PublishError{ServerMessage: "boom"}, false},
		{"closed", ErrClosed, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := DefaultRetryable(tc.err); got != tc.want {
				t.Errorf("DefaultRetryable(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestPublishErrorUnwrap(t *testing.T) {
	err := &PublishError{ServerMessage: "boom"}
	if !errors.Is(err, ErrPublishFailed) {
		t.Fatal("PublishError should unwrap to ErrPublishFailed")
	}
}
