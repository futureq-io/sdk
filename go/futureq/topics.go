package futureq

import (
	"context"
	"fmt"

	pb "github.com/futureq-io/protocol/proto/go"
	"google.golang.org/grpc"
)

// Topic is a declared publishing destination.
type Topic struct {
	Name string
}

// InitialSubscription declares a durable destination during topic creation.
// Set exactly one of Group and SubscriberID.
type InitialSubscription struct {
	Group        string
	SubscriberID string
}

// TopicPage is a page of topics in name order. An empty NextPageToken means
// there are no more pages.
type TopicPage struct {
	Topics        []Topic
	NextPageToken string
}

// CreateTopic declares a topic and any initial subscriptions atomically.
// Repeating the request keeps active subscriptions and creates missing ones.
func (c *Client) CreateTopic(ctx context.Context, name string, initial ...InitialSubscription) (*Topic, error) {
	conn, err := c.dialTopicLeader(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	request := &pb.CreateTopicRequest{Name: name, InitialSubscriptions: make([]*pb.InitialSubscription, 0, len(initial))}
	for _, subscription := range initial {
		request.InitialSubscriptions = append(request.InitialSubscriptions, &pb.InitialSubscription{
			GroupId: subscription.Group, SubscriberId: subscription.SubscriberID,
		})
	}
	response, err := pb.NewFutureQTopicClient(conn).CreateTopic(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("futureq: create topic: %w", err)
	}
	return &Topic{Name: response.GetName()}, nil
}

// GetTopic looks up a declared topic by name. An undeclared topic returns a
// gRPC NotFound status.
func (c *Client) GetTopic(ctx context.Context, name string) (*Topic, error) {
	conn, err := c.dialTopicLeader(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	response, err := pb.NewFutureQTopicClient(conn).GetTopic(ctx, &pb.GetTopicRequest{Name: name})
	if err != nil {
		return nil, fmt.Errorf("futureq: get topic: %w", err)
	}
	return &Topic{Name: response.GetName()}, nil
}

// ListTopics lists declared topics, including topics without subscriptions.
// Use an empty pageToken for the first page and zero pageSize for the broker
// default. Pass the returned NextPageToken to fetch the next page.
func (c *Client) ListTopics(ctx context.Context, pageSize uint32, pageToken string) (*TopicPage, error) {
	conn, err := c.dialTopicLeader(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	response, err := pb.NewFutureQTopicClient(conn).ListTopics(ctx, &pb.ListTopicsRequest{
		PageSize: pageSize, PageToken: pageToken,
	})
	if err != nil {
		return nil, fmt.Errorf("futureq: list topics: %w", err)
	}
	page := &TopicPage{
		Topics:        make([]Topic, 0, len(response.GetTopics())),
		NextPageToken: response.GetNextPageToken(),
	}
	for _, topic := range response.GetTopics() {
		page.Topics = append(page.Topics, Topic{Name: topic.GetName()})
	}
	return page, nil
}

func (c *Client) dialTopicLeader(ctx context.Context) (*grpc.ClientConn, error) {
	c.mu.RLock()
	closed := c.closed
	c.mu.RUnlock()
	if closed {
		return nil, ErrClosed
	}
	return c.dialLeader(ctx)
}
