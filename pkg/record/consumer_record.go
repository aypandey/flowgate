package record

// ConsumerRecord is a generic wrapper around a deserialized message received from Kafka.
// Teams receive this in their handler callbacks. The Payload field contains the
// fully deserialized domain object. Other fields provide Kafka metadata if needed.
//
// In most cases teams only need record.Payload, but partition, offset and headers
// are available for advanced use cases like manual offset tracking or tracing.
//
// Example (single message handler):
//
//	consumer.Subscribe(ctx, func(r *record.ConsumerRecord[Order]) error {
//	    order := r.Payload
//	    return elasticSearch.Index(order)
//	})
type ConsumerRecord[T any] struct {
	// Payload is the fully deserialized domain object.
	Payload T

	// Topic is the Kafka topic this message was consumed from.
	Topic string

	// Partition is the Kafka partition this message was consumed from.
	Partition int32

	// Offset is the position of this message within its partition.
	Offset int64

	// Key is the partition routing key set by the producer, if any.
	Key string

	// Headers are the metadata headers set by the producer, if any.
	Headers map[string]string

	// SchemaID is the Confluent Schema Registry ID used to serialize this message.
	// Useful for debugging schema version mismatches.
	SchemaID int
}

// NewConsumerRecord constructs a ConsumerRecord. Used internally by the consumer.
func NewConsumerRecord[T any](
	payload T,
	topic string,
	partition int32,
	offset int64,
	key string,
	headers map[string]string,
	schemaID int,
) *ConsumerRecord[T] {
	return &ConsumerRecord[T]{
		Payload:   payload,
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
		Key:       key,
		Headers:   headers,
		SchemaID:  schemaID,
	}
}
