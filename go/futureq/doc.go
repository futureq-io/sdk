// Package futureq provides a production-ready Go client SDK for the FutureQ
// scheduled message queue.
//
// # Overview
//
// FutureQ is a distributed, time-bucket-based scheduled queue backed by Pebble
// (an LSM key-value store) and optionally replicated via the Dragonboat Raft
// library.  This SDK abstracts the underlying gRPC bi-directional streaming
// protocol into two high-level, idiomatic Go clients:
//
//   - [Producer] — schedules messages to be delivered at a specific time.
//   - [Consumer] — subscribes to the queue and receives messages when they
//     become due, acknowledging each one to prevent redelivery.
//
// # Connecting
//
// Create a [Client] with [New] (or [NewWithConn] to supply your own
// [google.golang.org/grpc.ClientConn]):
//
//	client, err := futureq.New("localhost:8443", futureq.WithInsecure())
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer client.Close()
//
// # Producing messages
//
// Obtain a [Producer] from the client and call [Producer.Publish]:
//
//	producer, err := client.NewProducer(ctx)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer producer.Close()
//
//	err = producer.Publish(ctx, futureq.Message{
//	    Topic:     "notifications",
//	    Payload:   []byte(`{"user": 42}`),
//	    ExecuteAt: time.Now().Add(5 * time.Minute),
//	})
//
// # Consuming messages
//
// Obtain a [Consumer] from the client and call [Consumer.Subscribe]:
//
//	consumer, err := client.NewConsumer(ctx)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer consumer.Close()
//
//	err = consumer.Subscribe(ctx, func(msg futureq.Delivery) error {
//	    fmt.Printf("received: %s\n", msg.Payload)
//	    return nil // returning nil ACKs the message
//	})
//
// # Error handling
//
// All public methods return typed errors. Sentinel errors defined in this
// package (e.g. [ErrNotLeader], [ErrStreamClosed]) can be inspected with
// [errors.Is].
package futureq
