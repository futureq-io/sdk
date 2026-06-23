package futureq_test

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/futureq-io/sdk/go/futureq"
)

// ExampleClient_NewProducer demonstrates how to create a producer and
// schedule a single message.
func ExampleClient_NewProducer() {
	client, err := futureq.New(
		"futureq.internal:8443",
		futureq.WithTLS(nil),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	producer, err := client.NewProducer(ctx, futureq.WithPublishTimeout(5*time.Second))
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	err = producer.Publish(ctx, futureq.Message{
		Topic:     "email-notifications",
		Payload:   []byte(`{"to":"alice@example.com","subject":"Welcome!"}`),
		ExecuteAt: time.Now().Add(10 * time.Minute),
	})
	if err != nil {
		log.Printf("publish error: %v", err)
		return
	}

	fmt.Println("message scheduled")
	// Output: message scheduled
}

// ExampleProducer_PublishBatch shows how to schedule multiple messages
// in a single call.
// func ExampleProducer_PublishBatch() {
// 	client, err := futureq.New("futureq.internal:8443", futureq.WithTLS(nil))
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	defer client.Close()

// 	ctx := context.Background()
// 	producer, err := client.NewProducer(ctx)
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	defer producer.Close()

// 	now := time.Now()
// 	messages := []futureq.Message{
// 		{Topic: "reminders", Payload: []byte("reminder-1"), ExecuteAt: now.Add(1 * time.Minute)},
// 		{Topic: "reminders", Payload: []byte("reminder-2"), ExecuteAt: now.Add(2 * time.Minute)},
// 		{Topic: "reminders", Payload: []byte("reminder-3"), ExecuteAt: now.Add(3 * time.Minute)},
// 	}

// 	result, err := producer.PublishBatch(ctx, messages)
// 	if err != nil {
// 		log.Fatalf("transport error: %v", err)
// 	}

// 	for i, e := range result.Errors {
// 		if e != nil {
// 			log.Printf("message %d failed: %v", i, e)
// 		}
// 	}

// 	fmt.Printf("failed: %d/%d\n", len(result.FailedIndices()), len(messages))
// }

// ExampleClient_NewConsumer demonstrates how to subscribe to the queue
// and process messages with automatic ACK/NACK.
func ExampleClient_NewConsumer() {
	client, err := futureq.New("futureq.internal:8443", futureq.WithTLS(nil))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	consumer, err := client.NewConsumer(ctx,
		futureq.WithConcurrency(4),
		futureq.WithAckTimeout(3*time.Second),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	err = consumer.Subscribe(ctx, func(d futureq.Delivery) error {
		fmt.Printf("received on topic %q: %s\n", d.Topic, d.Payload)
		// Return nil to ACK; return an error to NACK and trigger redelivery.
		return nil
	})
	if err != nil {
		log.Printf("consumer error: %v", err)
	}
}

// ExampleProducer_PublishWithRetry demonstrates the built-in retry helper.
func ExampleProducer_PublishWithRetry() {
	client, err := futureq.New("futureq.internal:8443", futureq.WithTLS(nil))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	producer, err := client.NewProducer(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	policy := futureq.DefaultRetryPolicy()
	policy.MaxAttempts = 5

	err = producer.PublishWithRetry(ctx, futureq.Message{
		Topic:     "orders",
		Payload:   []byte(`{"order_id": 9001}`),
		ExecuteAt: time.Now().Add(30 * time.Second),
	}, policy)
	if err != nil {
		log.Printf("all retries exhausted: %v", err)
	}
}
