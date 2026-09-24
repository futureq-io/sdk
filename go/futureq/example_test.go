package futureq_test

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/futureq-io/sdk/go/futureq"
)

// Example demonstrates the simplest produce-and-consume flow.
func Example() {
	client, err := futureq.New(
		[]string{"localhost:9000"},
		futureq.WithInsecure(),
	)
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

	err = producer.Publish(ctx, futureq.Message{
		Topic:   "email",
		Payload: []byte(`{"to":"user@example.com"}`),
		Delay:   5 * time.Minute,
	})
	if err != nil {
		log.Fatal(err)
	}

	consumer, err := client.NewConsumer(ctx, "email", "senders")
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	_ = consumer.Subscribe(ctx, func(d futureq.Delivery) error {
		fmt.Printf("received on %s: %s\n", d.Topic, d.Payload)
		return nil
	})
}

// ExampleClient_dragonboatDiscovery shows how to enable the
// experimental Dragonboat-based topology discovery.
func ExampleClient_dragonboatDiscovery() {
	client, err := futureq.New(
		[]string{"node1.internal:9000", "node2.internal:9000"},
		futureq.WithInsecure(),
		// Push-based topology updates; no disk I/O.
		futureq.WithDragonboatDiscovery(nil),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
}

// ExampleProducer_PublishBatch demonstrates atomic batch publishing.
func ExampleProducer_PublishBatch() {
	client, err := futureq.New([]string{"localhost:9000"}, futureq.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	producer, err := client.NewProducer(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	err = producer.PublishBatch(context.Background(), []futureq.Message{
		{Topic: "events", Payload: []byte("a"), Delay: time.Minute},
		{Topic: "events", Payload: []byte("b"), Delay: 2 * time.Minute, TTL: time.Hour},
		{Topic: "events", Payload: []byte("c"), Indexes: []futureq.Index{
			futureq.StringIndex("user:42"),
			futureq.Int64Index(1001),
		}},
	}, futureq.AckQuorum)
	if err != nil {
		log.Fatal(err)
	}
}
