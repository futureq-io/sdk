package futureq

import (
	"context"
	"fmt"
	"unicode/utf8"

	pb "github.com/futureq-io/protocol/proto/go"
)

// Subscription identifies durable interest in a topic. Set exactly one of Group
// (workers compete) and SubscriberID (independent universal fan-out). Disconnects
// and Consumer.Close leave interest and pending deliveries intact.
type Subscription struct {
	Topic        string
	Group        string
	SubscriberID string
	Generation   string
}

func validateSubscription(subscription Subscription) error {
	if subscription.Topic == "" || (subscription.Group == "") == (subscription.SubscriberID == "") || !utf8.ValidString(subscription.Topic+subscription.Group+subscription.SubscriberID) || len(subscription.Topic)+len(subscription.Group)+len(subscription.SubscriberID) > 4096 {
		return fmt.Errorf("futureq: subscription requires topic and exactly one of Group or SubscriberID (maximum 4096 bytes)")
	}
	return nil
}
func (s Subscription) request() *pb.SubscriptionRequest {
	return &pb.SubscriptionRequest{Topic: s.Topic, GroupId: s.Group, SubscriberId: s.SubscriberID}
}

// Subscribe idempotently declares durable interest on an existing topic.
// Messages published afterward include this subscription even offline.
func (c *Client) Subscribe(ctx context.Context, subscription Subscription) (*Subscription, error) {
	if err := validateSubscription(subscription); err != nil {
		return nil, err
	}
	c.mu.RLock()
	closed := c.closed
	c.mu.RUnlock()
	if closed {
		return nil, ErrClosed
	}
	addr, _ := c.tracker.Leader()
	if addr == "" {
		if _, err := c.tracker.refreshOnce(ctx); err != nil {
			return nil, ErrNoLeader
		}
		addr, _ = c.tracker.Leader()
	}
	if addr == "" {
		return nil, ErrNoLeader
	}
	conn, err := c.tracker.dial(ctx, addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	response, err := pb.NewFutureQConsumerClient(conn).CreateSubscription(ctx, subscription.request())
	if err != nil {
		return nil, fmt.Errorf("futureq: subscribe: %w", err)
	}
	return &Subscription{Topic: response.Topic, Group: response.GroupId, SubscriberID: response.SubscriberId, Generation: response.Generation}, nil
}

// Unsubscribe removes durable interest and discards its pending deliveries.
// Other subscriptions are unaffected. Backlog cleanup is asynchronous; sends
// already in flight can still arrive. A later Subscribe creates a new generation.
func (c *Client) Unsubscribe(ctx context.Context, subscription Subscription) error {
	if err := validateSubscription(subscription); err != nil {
		return err
	}
	c.mu.RLock()
	closed := c.closed
	c.mu.RUnlock()
	if closed {
		return ErrClosed
	}
	addr, _ := c.tracker.Leader()
	if addr == "" {
		if _, err := c.tracker.refreshOnce(ctx); err != nil {
			return ErrNoLeader
		}
		addr, _ = c.tracker.Leader()
	}
	if addr == "" {
		return ErrNoLeader
	}
	conn, err := c.tracker.dial(ctx, addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = pb.NewFutureQConsumerClient(conn).Unsubscribe(ctx, subscription.request())
	if err != nil {
		return fmt.Errorf("futureq: unsubscribe: %w", err)
	}
	return nil
}
