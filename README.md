# FutureQ SDK

Official client SDKs for [FutureQ](https://github.com/futureq-io/futureq), a distributed, time-bucket-based scheduled message queue backed by Pebble and replicated via [Dragonboat](https://github.com/lni/dragonboat) Raft.

This repository contains the Go SDK. Additional languages will be added over time.

---

## Go SDK

**Module:** `github.com/futureq-io/sdk/go`

```bash
go get github.com/futureq-io/sdk/go@latest
```

Requires Go 1.26 or later.

---

### Features

- **Topology-aware routing** — the SDK tracks the cluster's Raft leader and automatically routes producer / consumer streams to it. On a leader change, streams are torn down and re-established against the new leader without application involvement.
- **Two discovery modes** — lightweight polling via `GetClusterInfo` (default), or an experimental embedded Dragonboat observer that receives push-based topology updates over Raft.
- **Atomic batch publishing** — every batch is committed as a single Raft log entry. Either the whole batch lands or none of it does.
- **Per-batch durability control** — choose between quorum-acknowledged (`AckQuorum`) and fire-and-forget (`AckNone`) per batch.
- **Delayed delivery, TTLs, secondary indexes** — schedule messages for future delivery, expire them automatically, and attach queryable indexes.
- **Consumer groups** — competing-consumer semantics within a group, fan-out across groups.
- **Safe concurrency** — `Client` and `Producer` are safe for use from multiple goroutines. `Consumer.Subscribe` supports configurable handler parallelism.

---

### Quick start

```go
package main

import (
    "context"
    "log"
    "time"

    "github.com/futureq-io/sdk/go/futureq"
)

func main() {
    client, err := futureq.New(
        []string{"node1.internal:9000", "node2.internal:9000"},
        futureq.WithInsecure(),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    ctx := context.Background()

    // ── Produce ────────────────────────────────────────────────────────
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

    // ── Consume ────────────────────────────────────────────────────────
    consumer, err := client.NewConsumer(ctx, "email", "senders")
    if err != nil {
        log.Fatal(err)
    }
    defer consumer.Close()

    err = consumer.Subscribe(ctx, func(d futureq.Delivery) error {
        log.Printf("received on %s: %s", d.Topic, d.Payload)
        return nil // nil ACKs the message
    })
    if err != nil {
        log.Fatal(err)
    }
}
```

---

### Connecting

`futureq.New` takes a list of **seed addresses**. A seed can be any node in the cluster — leader or follower. The SDK uses `GetClusterInfo` on the seeds to discover the current leader and routes all traffic to it.

```go
client, err := futureq.New(
    []string{"node1:9000", "node2:9000", "node3:9000"},
    futureq.WithInsecure(),
)
```

If the first seed is unreachable the SDK falls through to the next one. At least one seed must respond during `New`, otherwise an error is returned.

#### Transport security

```go
// Plain-text (development).
futureq.New(seeds, futureq.WithInsecure())

// TLS with system root CAs (production default).
futureq.New(seeds, futureq.WithTLS(nil))

// TLS with a custom config (mutual TLS, custom CA pool, …).
futureq.New(seeds, futureq.WithTLS(myTLSConfig))
```

#### Other dial options

| Option                              | Default | Purpose                                  |
| ----------------------------------- | ------- | ---------------------------------------- |
| `WithDialTimeout(d)`                | `10s`   | Initial connection timeout               |
| `WithKeepAlive(time, timeout)`      | `30s/10s` | HTTP/2 keep-alive                      |
| `WithMaxRecvMsgSize(mb)`            | `16`    | Max inbound message size                 |
| `WithMaxSendMsgSize(mb)`            | `16`    | Max outbound message size                |
| `WithDialOptions(opts…)`            | —       | Extra `grpc.DialOption`s (escape hatch)  |
| `WithTopologyRefreshInterval(d)`    | `10s`   | Polling frequency for `GetClusterInfo`   |

---

### Topology discovery

The SDK must know which node is the Raft leader in order to produce and consume. Two mechanisms are provided.

#### 1. Polling (default)

Every `WithTopologyRefreshInterval` the SDK calls `GetClusterInfo` on a seed and updates its view of the leader. This is the right choice for almost every deployment: zero moving parts, no extra ports, no extra dependencies.

```go
futureq.New(seeds,
    futureq.WithInsecure(),
    futureq.WithTopologyRefreshInterval(5*time.Second),
)
```

#### 2. Dragonboat discovery (experimental)

When `WithDragonboatDiscovery` is set, the SDK embeds a **non-voting Dragonboat replica** of the FutureQ metadata Raft group inside the client process. The brokers replicate committed topology entries to this replica over Raft, so leader changes are visible to the SDK the moment they commit — no polling.

**Guarantees:**

- **Non-voting** — the observer never participates in elections and never counts toward commit quorum. It is a pure follower.
- **In-memory only** — the embedded replica stores its WAL, log and snapshots in a [`vfs.NewMem()`](https://pkg.go.dev/github.com/lni/vfs) filesystem. **Nothing is ever written to disk.**
- **Self-cleaning** — when `client.Close()` is called the observer sends `LeaveMetadata` and shuts its `NodeHost` down.

```go
client, err := futureq.New(
    []string{"node1:9000"},
    futureq.WithInsecure(),
    futureq.WithDragonboatDiscovery(nil), // sensible defaults
)
```

Pass a config struct to customise the observer:

```go
futureq.WithDragonboatDiscovery(&futureq.MetadataObserverConfig{
    NodeID:      9001,                  // unique across the cluster
    RaftAddress: "0.0.0.0:16300",       // must be reachable by brokers
    ShardID:     1,                     // event shard to track
    OnTopologyChange: func(topo *metadata.ShardTopology) {
        log.Printf("new leader: %d @ %s", topo.LeaderID, topo.LeaderAddr)
    },
})
```

| Field              | Default             | Purpose                                                     |
| ------------------ | ------------------- | ----------------------------------------------------------- |
| `NodeID`           | random ≥ 2³²        | Observer's Raft node ID. Must not collide with any broker.  |
| `RaftAddress`      | `127.0.0.1:0`       | Host:port the observer's Raft transport binds.              |
| `RTTMillisecond`   | `50`                | Expected RTT between Raft peers (ms).                       |
| `ShardID`          | `1`                 | Event shard whose leader the SDK tracks.                    |
| `OnTopologyChange` | `nil`               | Optional callback fired on every leader change.             |

> The `RaftAddress` you choose must be **reachable from the brokers** so they can replicate the metadata log to you. In containerised / NAT environments, bind to a routable interface.

When dragonboat discovery is enabled the polling loop is disabled — the observer is push-based.

---

### Producing

#### Single message

```go
err := producer.Publish(ctx, futureq.Message{
    Topic:   "email",
    Payload: []byte(`{"to":"user@example.com"}`),
    Delay:   5 * time.Minute,
    TTL:     time.Hour,
    Indexes: []futureq.Index{
        futureq.StringIndex("user:42"),
        futureq.Int64Index(1001),
    },
})
```

| Field     | Type            | Notes                                                              |
| --------- | --------------- | ------------------------------------------------------------------ |
| `Topic`   | `string`        | Logical channel. Consumers subscribe by topic.                     |
| `Payload` | `[]byte`        | Opaque body. JSON / Protobuf / Avro / anything.                    |
| `Delay`   | `time.Duration` | How long the broker waits before the message becomes deliverable.  |
| `TTL`     | `time.Duration` | How long the broker keeps the message before discarding it.        |
| `Indexes` | `[]Index`       | Secondary indexes (expensive — attach only what you query on).     |

`Delay` and `TTL` are measured from the **broker's** receive time, not the client's send time.

#### Atomic batch

```go
err := producer.PublishBatch(ctx, []futureq.Message{
    {Topic: "events", Payload: []byte("a")},
    {Topic: "events", Payload: []byte("b"), Delay: time.Minute},
}, futureq.AckQuorum)
```

A batch is written as a **single Raft log entry** — atomic, all-or-nothing.

#### Durability

| `AckLevel`  | Behaviour                                                                       |
| ----------- | ------------------------------------------------------------------------------- |
| `AckQuorum` | Broker waits for the batch to be replicated to a quorum of voters before acking. **Default.** |
| `AckNone`   | Broker acks immediately. Highest throughput; messages may be lost on leader crash. |

#### Retries

```go
policy := futureq.DefaultRetryPolicy()
policy.MaxAttempts = 5

err := producer.PublishBatchWithRetry(ctx, msgs, futureq.AckQuorum, policy)
```

`DefaultRetryable` retries transient errors (network failures, `ErrNoLeader`, `ErrNotLeader`, `ErrStreamClosed`) and never retries permanent ones (`ErrPublishFailed`). Supply your own `RetryableFunc` to customise.

---

### Consuming

```go
consumer, err := client.NewConsumer(ctx,
    "email",   // topic
    "senders", // consumer group
    futureq.WithConcurrency(4),
    futureq.WithAckTimeout(3*time.Second),
)
if err != nil {
    log.Fatal(err)
}
defer consumer.Close()

err = consumer.Subscribe(ctx, func(d futureq.Delivery) error {
    // Process d.Payload…
    return nil // nil ACKs; non-nil NACKs
})
```

#### Groups

- Consumers **in the same group** compete for messages — each message is delivered to exactly one member.
- Consumers **in different groups** on the same topic each receive an independent copy (fan-out).

#### Delivery

```go
type Delivery struct {
    Topic      string
    Payload    []byte
    EnqueuedAt time.Time     // broker wall-clock when first received
    Delay      time.Duration // original delay requested by the producer
}
```

The broker assigns each delivery an opaque `deliveryTag` (the raw Pebble key). The SDK manages it internally — you never have to echo it back yourself.

#### Acknowledgement

- Return `nil` → **ACK**. Broker deletes the message.
- Return a non-nil `error` → **NACK**. Broker re-dispatches the message to another consumer on the next dispatcher tick.
- Panic in the handler → recovered, the message is NACKed, and the panic is logged to stderr.

#### Concurrency

`WithConcurrency(n)` spawns up to `n` handler goroutines. With the default `1`, messages are processed serially and in delivery order. With higher values, ordering is relaxed but throughput improves for I/O-bound handlers.

---

### Error handling

All public methods return typed errors. Use `errors.Is` to test for the sentinel errors:

```go
switch {
case errors.Is(err, futureq.ErrNoLeader):
    // SDK doesn't currently know of a live leader. Retry shortly.
case errors.Is(err, futureq.ErrNotLeader):
    // Connected node lost leadership. SDK has already re-resolved.
case errors.Is(err, futureq.ErrStreamClosed):
    // Underlying gRPC stream died. Producer/Consumer will redial on next call.
case errors.Is(err, futureq.ErrClosed):
    // Method called on a closed Client/Producer/Consumer.
case errors.Is(err, futureq.ErrPublishFailed):
    var perr *futureq.PublishError
    if errors.As(err, &perr) {
        log.Printf("broker rejected batch: %s", perr.ServerMessage)
    }
case errors.Is(err, futureq.ErrTopologyUnavailable):
    // No seed responded to GetClusterInfo.
}
```

---

### Cluster inspection

```go
topo, ok := client.Topology()
if ok {
    fmt.Printf("leader: node %d @ %s (%d nodes, updated %s ago)\n",
        topo.LeaderNodeID, topo.LeaderAddress, len(topo.Nodes),
        time.Since(topo.UpdatedAt).Round(time.Millisecond),
    )
}

fmt.Println("current leader:", client.Leader())
```

---

### Lifecycle

```go
client, _ := futureq.New(seeds, …)

producer, _ := client.NewProducer(ctx)
consumer, _ := client.NewConsumer(ctx, "topic", "group")

// On shutdown:
consumer.Close()   // cancels stream, drains in-flight handlers
producer.Close()   // closes publish stream
client.Close()     // stops discovery, shuts down observer if enabled
```

`Close` is idempotent on every type.

---

### Concurrency model

| Type       | Safe for concurrent use? |
| ---------- | ------------------------ |
| `Client`   | ✅ Yes                    |
| `Producer` | ✅ Yes (mutex-serialised) |
| `Consumer` | ⚠️ One goroutine may call `Subscribe` at a time |

---

### Example

See [`go/futureq/example_test.go`](./go/futureq/example_test.go) for complete, runnable examples.

---

## Repository layout

```
.
├── go/                  # Go SDK
│   ├── go.mod
│   └── futureq/
│       ├── client.go        # top-level entry point
│       ├── producer.go      # publish API
│       ├── consumer.go      # subscribe API
│       ├── topology.go      # leader tracker
│       ├── observer.go      # embedded dragonboat observer
│       ├── message.go       # Message / Delivery value types
│       ├── retry.go         # retry policies
│       ├── errors.go        # sentinel errors
│       └── doc.go           # package documentation
└── README.md
```

---

## Versioning

This project follows [SemVer](https://semver.org/). Breaking changes to the wire protocol or the public API are only introduced in major versions. The Dragonboat observer API is **experimental** and may change in minor releases while it matures.

---

## License

MIT — see [LICENSE](./LICENSE).
