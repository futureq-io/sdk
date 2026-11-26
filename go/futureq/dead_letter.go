package futureq

import (
	"context"
	"fmt"
	"io"
	"time"

	pb "github.com/futureq-io/protocol/proto/go"
	"google.golang.org/protobuf/proto"
)

// DeadLetterReason describes how the final delivery attempt ended.
type DeadLetterReason int

const (
	DeadLetterReasonUnknown DeadLetterReason = 0
	DeadLetterReasonNack    DeadLetterReason = 1
	DeadLetterReasonTimeout DeadLetterReason = 2
)

// DeadLetterSummary identifies a retained failure for one subscription.
type DeadLetterSummary struct {
	ID             []byte
	Topic          string
	Generation     string
	Group          string
	SubscriberID   string
	Attempts       uint64
	Reason         DeadLetterReason
	DeadLetteredAt time.Time
	EnqueuedAt     time.Time
}

// DeadLetter includes the original publish fields alongside the failure summary.
type DeadLetter struct {
	Summary DeadLetterSummary
	Message Message
}

// DeadLetterPage contains a page of summaries and an optional continuation token.
type DeadLetterPage struct {
	Entries       []DeadLetterSummary
	NextPageToken []byte
}

// ListDeadLetters lists retained failures for a topic. Use the returned
// NextPageToken to continue, or an empty token to start from the beginning.
func (c *Client) ListDeadLetters(ctx context.Context, topic string, pageSize uint32, pageToken []byte) (*DeadLetterPage, error) {
	conn, err := c.dialTopicLeader(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	response, err := pb.NewFutureQDeadLetterClient(conn).ListDeadLetters(ctx, &pb.ListDeadLettersRequest{
		Topic: topic, PageSize: pageSize, PageToken: pageToken,
	})
	if err != nil {
		return nil, fmt.Errorf("futureq: list dead letters: %w", err)
	}
	page := &DeadLetterPage{Entries: make([]DeadLetterSummary, 0, len(response.Entries)), NextPageToken: response.NextPageToken}
	for _, entry := range response.Entries {
		page.Entries = append(page.Entries, deadLetterSummary(entry))
	}
	return page, nil
}

// GetDeadLetter retrieves a retained failure and its original publish fields.
func (c *Client) GetDeadLetter(ctx context.Context, topic string, id []byte) (*DeadLetter, error) {
	conn, err := c.dialTopicLeader(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	stream, err := pb.NewFutureQDeadLetterClient(conn).GetDeadLetter(ctx, &pb.DeadLetterRequest{Topic: topic, Id: id})
	if err != nil {
		return nil, fmt.Errorf("futureq: get dead letter: %w", err)
	}
	var data []byte
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("futureq: get dead letter: %w", err)
		}
		data = append(data, chunk.GetData()...)
	}
	var response pb.DeadLetter
	if err := proto.Unmarshal(data, &response); err != nil {
		return nil, fmt.Errorf("futureq: decode dead letter: %w", err)
	}
	wire := response.GetMessage()
	if wire == nil || response.GetSummary() == nil {
		return nil, fmt.Errorf("futureq: incomplete dead letter response")
	}
	message := Message{
		Topic: wire.GetTopic(), Payload: wire.GetPayload(), MaxRetries: wire.MaxRetries,
		Delay: time.Duration(wire.GetDelayMs()) * time.Millisecond,
		TTL:   time.Duration(wire.GetTtlMs()) * time.Millisecond,
	}
	for _, index := range wire.GetIndexes() {
		switch value := index.GetValue().(type) {
		case *pb.Index_Int64Value:
			message.Indexes = append(message.Indexes, Int64Index(value.Int64Value))
		case *pb.Index_StringValue:
			message.Indexes = append(message.Indexes, StringIndex(value.StringValue))
		}
	}
	return &DeadLetter{Summary: deadLetterSummary(response.GetSummary()), Message: message}, nil
}

// DeleteDeadLetter removes one retained failure. Repeated deletion succeeds.
func (c *Client) DeleteDeadLetter(ctx context.Context, topic string, id []byte) error {
	conn, err := c.dialTopicLeader(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = pb.NewFutureQDeadLetterClient(conn).DeleteDeadLetter(ctx, &pb.DeadLetterRequest{Topic: topic, Id: id})
	if err != nil {
		return fmt.Errorf("futureq: delete dead letter: %w", err)
	}
	return nil
}

func deadLetterSummary(summary *pb.DeadLetterSummary) DeadLetterSummary {
	return DeadLetterSummary{
		ID: summary.GetId(), Topic: summary.GetTopic(), Generation: summary.GetGeneration(),
		Group: summary.GetGroupId(), SubscriberID: summary.GetSubscriberId(), Attempts: summary.GetAttempts(),
		Reason: DeadLetterReason(summary.GetReason()), DeadLetteredAt: time.UnixMilli(summary.GetDeadLetteredAtUnixMs()),
		EnqueuedAt: time.UnixMilli(summary.GetEnqueuedAtUnixMs()),
	}
}
