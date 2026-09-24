// Package futureq provides a production-ready Go client SDK for the
// FutureQ scheduled message queue.
//
// # Overview
//
// FutureQ is a distributed, time-bucket-based scheduled queue backed by
// Pebble and replicated via Dragonboat Raft. This SDK abstracts the
// gRPC bi-directional streaming protocol into two high-level clients:
//
//   - [Producer] — publishes batches of messages with optional delays,
//     TTLs and secondary indexes.
//   - [Consumer] — subscribes to a (topic, group) pair and invokes a
//     handler for every delivered message, ACKing or NACKing each one.
//
// # Topology discovery
//
// The SDK tracks the cluster's Raft leader and routes streams to it.
// Two discovery mechanisms are available:
//
//  1. Polling — the SDK periodically calls GetClusterInfo on one of
//     the seed addresses passed to [New]. This is the default.
//  2. Dragonboat discovery — when [WithDragonboatDiscovery] is set the
//     SDK embeds a non-voting Dragonboat replica of the metadata Raft
//     group inside the client process. Topology changes flow in over
//     Raft as they commit, with no polling. The embedded replica stores
//     all of its state in an in-memory VFS — nothing is ever written
//     to disk.
//
// Dragonboat discovery is experimental.
//
// # Connecting
//
// Create a [Client] with [New]:
//
//	client, err := futureq.New(
//	    []string{"node1.internal:9000", "node2.internal:9000"},
//	    futureq.WithInsecure(),
//	)
//	if err != nil { log.Fatal(err) }
//	defer client.Close()
//
// # Producing
//
//	producer, err := client.NewProducer(ctx)
//	if err != nil { log.Fatal(err) }
//	defer producer.Close()
//
//	err = producer.PublishBatch(ctx, []futureq.Message{
//	    {Topic: "email", Payload: []byte("…"), Delay: 5 * time.Minute},
//	}, futureq.AckQuorum)
//
// # Consuming
//
//	consumer, err := client.NewConsumer(ctx, "email", "workers")
//	if err != nil { log.Fatal(err) }
//	defer consumer.Close()
//
//	err = consumer.Subscribe(ctx, func(d futureq.Delivery) error {
//	    process(d.Payload)
//	    return nil // nil ACKs the message
//	})
//
// # Error handling
//
// All public methods return typed errors. Sentinel errors defined in
// this package (e.g. [ErrNotLeader], [ErrNoLeader], [ErrStreamClosed])
// can be inspected with [errors.Is].
package futureq
