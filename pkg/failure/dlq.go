package failure

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	confluent "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// KafkaDLQFailureHandler routes failed records to a Kafka dead-letter topic.
//
// It is the recommended production FailureHandler. Failed messages are preserved
// in the DLQ topic with the original payload intact and two additional headers:
//
//   - flowgate-failure-reason — the error string
//   - flowgate-original-topic — the topic the message was originally destined for
//
// For serialization failures (where no Avro bytes exist), a JSON tombstone
// containing the key, original topic, and error is written instead.
//
// Example:
//
//	dlq, err := failure.NewKafkaDLQFailureHandler("localhost:9092", "payments.order.dlq")
//	if err != nil { ... }
//	defer dlq.Close()
//
//	p, err := producer.NewProducer[Order](
//	    producer.WithBrokers("localhost:9092"),
//	    producer.WithFailureHandler(dlq),
//	    ...
//	)
type KafkaDLQFailureHandler struct {
	producer *confluent.Producer
	topic    string
}

// NewKafkaDLQFailureHandler creates a handler that routes all failures to dlqTopic.
// The underlying producer uses idempotent delivery (acks=all) to ensure DLQ messages
// are themselves not lost.
func NewKafkaDLQFailureHandler(brokers, dlqTopic string) (*KafkaDLQFailureHandler, error) {
	p, err := confluent.NewProducer(&confluent.ConfigMap{
		"bootstrap.servers": brokers,
		"acks":              "all",
		"enable.idempotence": true,
	})
	if err != nil {
		return nil, fmt.Errorf("flowgate/failure: failed to create DLQ producer for %q: %w", dlqTopic, err)
	}
	return &KafkaDLQFailureHandler{producer: p, topic: dlqTopic}, nil
}

// OnFailure produces the failed record to the DLQ topic.
// If the context is already cancelled the write is still attempted — a failure
// event should be recorded even during shutdown.
// If the DLQ produce itself fails, the error is logged to stderr (best-effort).
func (h *KafkaDLQFailureHandler) OnFailure(_ context.Context, r RawRecord, err error) {
	payload := r.Payload
	if len(payload) == 0 {
		// Serialization failure — no Avro bytes available.
		// Write a JSON tombstone so the DLQ has something to replay from.
		payload, _ = json.Marshal(map[string]string{
			"original_topic": r.Topic,
			"key":            r.Key,
			"error":          err.Error(),
		})
	}

	headers := []confluent.Header{
		{Key: "flowgate-failure-reason", Value: []byte(err.Error())},
		{Key: "flowgate-original-topic", Value: []byte(r.Topic)},
	}
	for k, v := range r.Headers {
		headers = append(headers, confluent.Header{Key: k, Value: []byte(v)})
	}

	var keyBytes []byte
	if r.Key != "" {
		keyBytes = []byte(r.Key)
	}

	produceErr := h.producer.Produce(&confluent.Message{
		TopicPartition: confluent.TopicPartition{
			Topic:     &h.topic,
			Partition: confluent.PartitionAny,
		},
		Key:     keyBytes,
		Value:   payload,
		Headers: headers,
	}, nil)

	if produceErr != nil {
		log.Printf("flowgate/failure: DLQ produce to %q failed: %v (original error: %v)",
			h.topic, produceErr, err)
	}
}

// Close flushes any pending DLQ messages and shuts down the internal producer.
// Call this as part of your service shutdown sequence.
func (h *KafkaDLQFailureHandler) Close() {
	h.producer.Flush(10_000)
	h.producer.Close()
}
