package futureq

import "time"

// Message is the value type passed to [Producer.Publish].
// Every field has an idiomatic zero value:
//   - Topic defaults to the empty string (valid; the server accepts it).
//   - Payload may be nil (the server stores a zero-byte body).
//   - ExecuteAt defaults to time.Time{} which is treated as "execute
//     immediately" by the FutureQ server (bucket 0).
type Message struct {
	// Topic is an arbitrary string label for the message.
	// It is stored alongside the payload and surfaced in [Delivery].
	// Topics are not used for routing in the current server implementation
	// but are available for application-level filtering on the consumer side.
	Topic string

	// Payload is the raw bytes to deliver to consumers.
	// There is no imposed structure; JSON, Protobuf, Avro, etc. all work.
	Payload []byte

	// ExecuteAt is the earliest time at which the message should be
	// delivered.  The server will not dispatch the message before this
	// instant.  Pass time.Now() or a zero value to schedule for immediate
	// delivery.
	ExecuteAt time.Time
}

// Delivery is received by the handler function passed to [Consumer.Subscribe].
// It carries the decoded message body and the opaque delivery tag that must be
// echoed back in the ACK/NACK sent to the server.
type Delivery struct {
	// Topic is the topic label set by the producer.
	Topic string

	// Payload is the raw message body.
	Payload []byte

	// DeliveryTag is an opaque server-assigned token that uniquely identifies
	// You do not need to use this field directly; the SDK uses it internally
	// when generating ACK/NACK responses.
	deliveryTag []byte
}
