// Package record defines the message wrapper types used by flowgate producers and consumers.
// Teams interact with ProducerRecord to send messages and ConsumerRecord to receive them.
// Domain objects are never polluted with Kafka concerns — all metadata lives in the wrapper.
package record

// ProducerRecord is a generic wrapper around a team's domain object.
// It carries the payload alongside optional Kafka metadata like key and headers.
// Teams construct records using the RecordOf factory method.
//
// Example:
//
//	record := record.RecordOf(order)
//	record := record.RecordOf(order).WithKey("order-123")
//	record := record.RecordOf(order).WithKey("order-123").WithHeader("correlation-id", "abc")
type ProducerRecord[T any] struct {
	// Payload is the domain object to be serialized and sent to Kafka.
	Payload T

	// Key is an optional message key used for partition routing.
	// Messages with the same key always go to the same partition,
	// guaranteeing ordering per key.
	Key string

	// Headers are optional key-value metadata attached to the message.
	// Useful for tracing, correlation IDs, source service identification etc.
	Headers map[string]string
}

// RecordOf creates a new ProducerRecord wrapping the given payload.
// This is the primary factory method teams should use.
//
// Example:
//
//	r := record.RecordOf(order)
func RecordOf[T any](payload T) *ProducerRecord[T] {
	return &ProducerRecord[T]{
		Payload: payload,
		Headers: make(map[string]string),
	}
}

// WithKey sets the partition routing key on the record.
// Messages sharing the same key are guaranteed to land on the same partition,
// preserving ordering for that key.
//
// Example:
//
//	r := record.RecordOf(order).WithKey("order-123")
func (r *ProducerRecord[T]) WithKey(key string) *ProducerRecord[T] {
	r.Key = key
	return r
}

// WithHeader adds a single metadata header to the record.
// Can be chained to add multiple headers.
//
// Example:
//
//	r := record.RecordOf(order).
//	    WithHeader("correlation-id", "abc-xyz").
//	    WithHeader("source-service", "payments")
func (r *ProducerRecord[T]) WithHeader(key, value string) *ProducerRecord[T] {
	r.Headers[key] = value
	return r
}
