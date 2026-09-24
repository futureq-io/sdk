package futureq

import "time"

// AckLevel controls how durable a batch publish must be before the broker
// acknowledges it. It maps directly onto the wire-level
// [pb.AckLevel] enumeration.
type AckLevel int

const (
	// AckQuorum requires the batch to be replicated to a quorum of Raft
	// voters before the broker acknowledges it. This is the default and the
	// safest option.
	AckQuorum AckLevel = iota

	// AckNone asks the broker to acknowledge the batch immediately, without
	// waiting for replication. Use this only when losing messages is
	// acceptable (e.g. high-throughput telemetry).
	AckNone
)

// Index is a single secondary index attached to a published message.
//
// Indexes are expensive in FutureQ — attach only the ones you actually need
// to query on the consumer side. A message may carry any mix of int64 and
// string indexes.
type Index struct {
	// Int64Value is set when the index is a 64-bit signed integer.
	// Exactly one of Int64Value / StringValue must be non-zero / non-empty.
	Int64Value int64

	// StringValue is set when the index is an arbitrary string.
	StringValue string

	// IsString selects which field the SDK serialises. When true the SDK
	// writes StringValue; otherwise it writes Int64Value.
	IsString bool
}

// Int64Index is a convenience constructor for a numeric index.
func Int64Index(v int64) Index {
	return Index{Int64Value: v}
}

// StringIndex is a convenience constructor for a string index.
func StringIndex(v string) Index {
	return Index{StringValue: v, IsString: true}
}

// Message is the value type passed to [Producer.Publish] and
// [Producer.PublishBatch].
//
// Every field has an idiomatic zero value:
//   - Topic may be empty (the server accepts it).
//   - Payload may be nil (a zero-byte body is stored).
//   - Delay and TTL of zero mean "deliver on the next dispatcher tick" and
//     "never expire" respectively.
//   - Indexes may be nil.
type Message struct {
	// Topic identifies the logical channel for this message.
	// Consumers subscribe to a topic and receive every message published on it.
	Topic string

	// Payload is the raw bytes to deliver to consumers.
	// There is no imposed structure; JSON, Protobuf, Avro, etc. all work.
	Payload []byte

	// Delay is how long the broker waits before the message becomes eligible
	// for delivery, measured from the broker's receive time. Zero means
	// "deliver on the next dispatcher tick".
	Delay time.Duration

	// TTL is the message time-to-live, also measured from the broker's
	// receive time. If the message has not been consumed within TTL it is
	// lazily discarded. Zero means no expiry.
	TTL time.Duration

	// Indexes are the secondary indexes attached to the message.
	// Indexes are expensive in FutureQ — see [Index].
	Indexes []Index
}

// Delivery is received by the handler function passed to [Consumer.Subscribe].
// It carries the decoded message body plus the metadata the broker recorded
// when the message was first received.
type Delivery struct {
	// Topic is the channel this message was published on.
	Topic string

	// Payload is the raw message body.
	Payload []byte

	// EnqueuedAt is the broker wall-clock time when the message was first
	// received. Useful for computing actual end-to-end delivery latency.
	EnqueuedAt time.Time

	// Delay is the original delay requested by the producer.
	Delay time.Duration

	// deliveryTag is the opaque server-assigned token (the raw Pebble key)
	// that uniquely identifies this delivery. The SDK echoes it back in the
	// ACK/NACK frame; callers should never need to touch it directly.
	deliveryTag []byte
}

// DeliveryTag returns the raw server-assigned token for this delivery.
// Most callers never need this — the SDK manages ACKs internally.
func (d Delivery) DeliveryTag() []byte {
	// Return a copy so callers cannot mutate the tag the SDK relies on.
	out := make([]byte, len(d.deliveryTag))
	copy(out, d.deliveryTag)
	return out
}
