// Package consumer provides a high-level, schema-aware Kafka consumer for flowgate.
// Teams provide a handler function and the library manages polling, deserialization,
// offset commits, and failure handling internally.
package consumer

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/aypandey/flowgate/pkg/failure"
	"github.com/aypandey/flowgate/pkg/record"
	"github.com/aypandey/flowgate/pkg/schema"
	confluent "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// HandlerFunc is the callback teams provide to process a single message.
// Returning an error immediately delegates the record to the FailureHandler.
// Offset is committed only after this function returns nil.
type HandlerFunc[T any] func(r *record.ConsumerRecord[T]) error

// BatchHandlerFunc is the callback for batch processing.
// All records in the batch share the same commit boundary.
// If the batch handler returns an error, every record in the batch
// is delegated to the FailureHandler.
type BatchHandlerFunc[T any] func(records []*record.ConsumerRecord[T]) error

// Consumer is a schema-aware Kafka consumer.
// It manages the confluent-kafka-go poll loop internally and exposes
// a clean callback-based API to teams.
type Consumer[T any] struct {
	cfg      *config
	kafka    *confluent.Consumer
	registry *schema.Registry
	once     sync.Once
}

// NewConsumer creates and initialises a new Consumer.
//
// Example:
//
//	c, err := consumer.NewConsumer[Order](
//	    consumer.WithBrokers("localhost:9092"),
//	    consumer.WithSchemaRegistry("http://localhost:8081"),
//	    consumer.WithTopic("payments.order"),
//	    consumer.WithGroupID("order-processing-service"),
//	    consumer.WithSchemaFile("schemas/order/v1/order.avsc"),
//	)
func NewConsumer[T any](opts ...Option) (*Consumer[T], error) {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	if err := validateConfig(cfg); err != nil {
		return nil, err
	}

	if cfg.failureHandler == nil {
		lh, err := failure.NewLoggingFailureHandler("failed-events-consumer.log")
		if err != nil {
			return nil, fmt.Errorf("flowgate/consumer: failed to create default failure handler: %w", err)
		}
		cfg.failureHandler = lh
	}

	reg, err := schema.NewRegistry(cfg.registryURL)
	if err != nil {
		return nil, fmt.Errorf("flowgate/consumer: failed to connect to schema registry: %w", err)
	}

	kafkaCfg := cfg.buildConsumerKafkaConfig()
	kc, err := confluent.NewConsumer(&kafkaCfg)
	if err != nil {
		return nil, fmt.Errorf("flowgate/consumer: failed to create kafka consumer: %w", err)
	}

	if err := kc.Subscribe(cfg.topic, nil); err != nil {
		return nil, fmt.Errorf("flowgate/consumer: failed to subscribe to topic %q: %w", cfg.topic, err)
	}

	log.Printf("flowgate/consumer: ready — topic=%s groupID=%s", cfg.topic, cfg.groupID)

	return &Consumer[T]{
		cfg:      cfg,
		kafka:    kc,
		registry: reg,
	}, nil
}

// Subscribe starts the poll loop and invokes handler for each message.
// Blocks until ctx is cancelled or an unrecoverable error occurs.
// Offset is committed only after the handler returns nil.
// On handler error, the record is immediately delegated to FailureHandler.
//
// Example:
//
//	err := c.Subscribe(ctx, func(r *record.ConsumerRecord[Order]) error {
//	    return elasticSearch.Index(r.Payload)
//	})
func (c *Consumer[T]) Subscribe(ctx context.Context, handler HandlerFunc[T]) error {
	log.Printf("flowgate/consumer: starting single-message subscription on topic=%s", c.cfg.topic)

	for {
		select {
		case <-ctx.Done():
			log.Printf("flowgate/consumer: context cancelled, stopping")
			return ctx.Err()
		default:
		}

		msg, err := c.kafka.ReadMessage(100 * time.Millisecond)
		if err != nil {
			// timeout is normal — just means no messages right now
			if isTimeout(err) {
				continue
			}
			return fmt.Errorf("flowgate/consumer: poll error: %w", err)
		}

		rec, err := c.deserialize(msg)
		if err != nil {
			c.cfg.failureHandler.OnFailure(ctx, rawRecordFromMsg(msg), failure.NewFailureError("deserialization", err))
			// commit anyway to avoid reprocessing a permanently undeserializable message
			c.commit(msg)
			continue
		}

		if err := handler(rec); err != nil {
			c.cfg.failureHandler.OnFailure(ctx, rawRecordFromMsg(msg), failure.NewFailureError("handler", err))
			// do NOT commit — at-least-once: message will be redelivered on restart
			// handler should be idempotent
			continue
		}

		// commit offset only after successful handler
		c.commit(msg)
	}
}

// SubscribeBatch starts the poll loop and invokes handler with batches of messages.
// Batches are flushed when batchSize is reached OR batchFlushInterval elapses.
// Offsets are committed only after the batch handler returns nil.
// On batch handler error, every record in the batch is sent to FailureHandler.
//
// Example:
//
//	err := c.SubscribeBatch(ctx, func(records []*record.ConsumerRecord[Order]) error {
//	    return elasticSearch.BulkIndex(records)
//	})
func (c *Consumer[T]) SubscribeBatch(ctx context.Context, handler BatchHandlerFunc[T]) error {
	log.Printf("flowgate/consumer: starting batch subscription on topic=%s batchSize=%d",
		c.cfg.topic, c.cfg.batchSize)

	batch := make([]*record.ConsumerRecord[T], 0, c.cfg.batchSize)
	rawBatch := make([]*confluent.Message, 0, c.cfg.batchSize) // for offset commits
	ticker := time.NewTicker(c.cfg.batchFlushInterval)
	defer ticker.Stop()

	flushBatch := func() {
		if len(batch) == 0 {
			return
		}

		if err := handler(batch); err != nil {
			// entire batch failed — send each record to FailureHandler
			for _, msg := range rawBatch {
				c.cfg.failureHandler.OnFailure(
					ctx,
					rawRecordFromMsg(msg),
					failure.NewFailureError("handler", err),
				)
			}
		} else {
			// commit the last offset in the batch (Kafka commits are per-partition watermark)
			for _, msg := range rawBatch {
				c.commit(msg)
			}
		}

		// reset batch
		batch = batch[:0]
		rawBatch = rawBatch[:0]
	}

	for {
		select {
		case <-ctx.Done():
			flushBatch() // flush partial batch on shutdown
			return ctx.Err()

		case <-ticker.C:
			flushBatch() // flush on interval even if batch not full

		default:
			msg, err := c.kafka.ReadMessage(100 * time.Millisecond)
			if err != nil {
				if isTimeout(err) {
					continue
				}
				flushBatch()
				return fmt.Errorf("flowgate/consumer: poll error: %w", err)
			}

			rec, err := c.deserialize(msg)
			if err != nil {
				c.cfg.failureHandler.OnFailure(ctx, rawRecordFromMsg(msg), failure.NewFailureError("deserialization", err))
				c.commit(msg)
				continue
			}

			batch = append(batch, rec)
			rawBatch = append(rawBatch, msg)

			if len(batch) >= c.cfg.batchSize {
				flushBatch()
				ticker.Reset(c.cfg.batchFlushInterval)
			}
		}
	}
}

// Close stops the consumer and waits for in-flight processing to complete.
// Safe to call multiple times.
func (c *Consumer[T]) Close() {
	c.once.Do(func() {
		log.Printf("flowgate/consumer: closing")
		c.kafka.Close()
	})
}

// deserialize converts a raw Kafka message into a typed ConsumerRecord.
func (c *Consumer[T]) deserialize(msg *confluent.Message) (*record.ConsumerRecord[T], error) {
	schemaID, err := schema.ExtractSchemaID(msg.Value)
	if err != nil {
		return nil, err
	}

	codec, err := c.registry.GetCodecByID(schemaID)
	if err != nil {
		return nil, err
	}

	native, err := schema.DecodeWithCodec(codec, msg.Value)
	if err != nil {
		return nil, err
	}

	var target T
	if err := schema.NativeMapToStruct(native, &target); err != nil {
		return nil, err
	}

	headers := make(map[string]string, len(msg.Headers))
	for _, h := range msg.Headers {
		headers[h.Key] = string(h.Value)
	}

	key := ""
	if msg.Key != nil {
		key = string(msg.Key)
	}

	return record.NewConsumerRecord(
		target,
		*msg.TopicPartition.Topic,
		msg.TopicPartition.Partition,
		int64(msg.TopicPartition.Offset),
		key,
		headers,
		schemaID,
	), nil
}

// commit commits the offset for a single message.
func (c *Consumer[T]) commit(msg *confluent.Message) {
	if _, err := c.kafka.CommitMessage(msg); err != nil {
		log.Printf("flowgate/consumer: failed to commit offset: %v", err)
	}
}

// rawRecordFromMsg converts a confluent message to a failure.RawRecord.
func rawRecordFromMsg(msg *confluent.Message) failure.RawRecord {
	headers := make(map[string]string, len(msg.Headers))
	for _, h := range msg.Headers {
		headers[h.Key] = string(h.Value)
	}
	key := ""
	if msg.Key != nil {
		key = string(msg.Key)
	}
	topic := ""
	if msg.TopicPartition.Topic != nil {
		topic = *msg.TopicPartition.Topic
	}
	return failure.RawRecord{
		Topic:     topic,
		Partition: msg.TopicPartition.Partition,
		Offset:    int64(msg.TopicPartition.Offset),
		Key:       key,
		Headers:   headers,
		Payload:   msg.Value,
	}
}

// isTimeout returns true if the error is a Kafka poll timeout (normal, not an error).
func isTimeout(err error) bool {
	if kafkaErr, ok := err.(confluent.Error); ok {
		return kafkaErr.Code() == confluent.ErrTimedOut
	}
	return false
}

// validateConfig checks that all required fields are set.
func validateConfig(cfg *config) error {
	if cfg.brokers == "" {
		return fmt.Errorf("flowgate/consumer: WithBrokers is required")
	}
	if cfg.topic == "" {
		return fmt.Errorf("flowgate/consumer: WithTopic is required")
	}
	if cfg.groupID == "" {
		return fmt.Errorf("flowgate/consumer: WithGroupID is required")
	}
	if cfg.registryURL == "" {
		return fmt.Errorf("flowgate/consumer: WithSchemaRegistry is required")
	}
	return nil
}
