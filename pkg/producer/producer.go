// Package producer provides a high-level, schema-aware Kafka producer for flowgate.
// Teams interact only with NewProducer, Send, SendAsync, Flush, and Close.
// All serialization, schema registration, and retry logic is internal.
package producer

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

// SendResult is returned by SendAsync to report delivery outcome.
type SendResult struct {
	Err error
}

// Producer is a schema-aware Kafka producer.
// One Producer instance maps to exactly one Kafka topic and one Avro schema.
// It is safe for concurrent use by multiple goroutines.
type Producer[T any] struct {
	cfg      *config
	kafka    *confluent.Producer
	registry *schema.Registry
	schemaID int
	retrier  *retrier

	wg   sync.WaitGroup
	once sync.Once
}

// NewProducer creates and initialises a new Producer with the given options.
// On startup it:
//  1. Validates required config (brokers, topic, registry URL, schema)
//  2. Connects to the Schema Registry and registers the schema (with retries)
//  3. Creates the underlying confluent-kafka-go producer
//  4. Starts the background delivery-events goroutine
//
// Example:
//
//	p, err := producer.NewProducer[Order](
//	    producer.WithBrokers("localhost:9092"),
//	    producer.WithSchemaRegistry("http://localhost:8081"),
//	    producer.WithTopic("payments.order"),
//	    producer.WithSchemaFile("schemas/order/v1/order.avsc"),
//	)
func NewProducer[T any](opts ...Option) (*Producer[T], error) {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	if err := validateConfig(cfg); err != nil {
		return nil, err
	}

	// set default failure handler if not provided
	if cfg.failureHandler == nil {
		lh, err := failure.NewLoggingFailureHandler("failed-events.log")
		if err != nil {
			return nil, fmt.Errorf("flowgate/producer: failed to create default failure handler: %w", err)
		}
		cfg.failureHandler = lh
	}

	// connect to schema registry
	reg, err := schema.NewRegistry(cfg.registryURL)
	if err != nil {
		return nil, fmt.Errorf("flowgate/producer: failed to connect to schema registry: %w", err)
	}

	// code-first: generate schema string outside the retry loop (deterministic)
	var schemaStr string
	if cfg.schemaStruct != nil {
		schemaStr, err = schema.GenerateAvroSchema(cfg.schemaStruct)
		if err != nil {
			return nil, fmt.Errorf("flowgate/producer: failed to generate schema from struct: %w", err)
		}
	}

	// register schema with retries for transient SR failures
	r := newRetrier(cfg.retryConfig)
	subject := schema.SubjectName(cfg.topic)
	var schemaID int
	err = r.Do(context.Background(), func() error {
		var regErr error
		if cfg.schemaPath != "" {
			schemaID, regErr = reg.RegisterFromFile(subject, cfg.schemaPath)
		} else {
			schemaID, regErr = reg.RegisterFromString(subject, schemaStr)
		}
		return regErr
	})
	if err != nil {
		return nil, fmt.Errorf("flowgate/producer: schema registration failed: %w", err)
	}

	// build confluent kafka config via three-layer merge
	kafkaCfg := cfg.buildProducerKafkaConfig()
	kp, err := confluent.NewProducer(&kafkaCfg)
	if err != nil {
		return nil, fmt.Errorf("flowgate/producer: failed to create kafka producer: %w", err)
	}

	p := &Producer[T]{
		cfg:      cfg,
		kafka:    kp,
		registry: reg,
		schemaID: schemaID,
		retrier:  r,
	}

	// start background goroutine to drain delivery events from librdkafka
	p.wg.Add(1)
	go p.runEvents()

	log.Printf("flowgate/producer: ready — topic=%s schemaID=%d", cfg.topic, schemaID)
	return p, nil
}

// Send serializes and enqueues a record into librdkafka's internal buffer.
// Returns an error only if serialization fails or the internal queue is full.
// Delivery failures are handled asynchronously via FailureHandler.
//
// Example:
//
//	err := p.Send(ctx, record.RecordOf(order))
//	err := p.Send(ctx, record.RecordOf(order).WithKey("order-123"))
func (p *Producer[T]) Send(ctx context.Context, r *record.ProducerRecord[T]) error {
	payload, err := p.serialize(r.Payload)
	if err != nil {
		p.cfg.failureHandler.OnFailure(failure.RawRecord{
			Topic:   p.cfg.topic,
			Key:     r.Key,
			Headers: r.Headers,
		}, failure.NewFailureError("serialization", err))
		return fmt.Errorf("flowgate/producer: serialization failed: %w", err)
	}

	msg := p.buildMessage(r.Key, r.Headers, payload)
	if err := p.kafka.Produce(msg, nil); err != nil {
		return fmt.Errorf("flowgate/producer: produce failed: %w", err)
	}
	return nil
}

// SendAsync serializes and enqueues a record, returning a channel that receives
// the delivery result. The channel receives exactly one SendResult and is then closed.
//
// Example:
//
//	resultCh := p.SendAsync(ctx, record.RecordOf(order))
//	go func() {
//	    result := <-resultCh
//	    if result.Err != nil { ... }
//	}()
func (p *Producer[T]) SendAsync(ctx context.Context, r *record.ProducerRecord[T]) <-chan SendResult {
	resultCh := make(chan SendResult, 1)

	payload, err := p.serialize(r.Payload)
	if err != nil {
		p.cfg.failureHandler.OnFailure(failure.RawRecord{
			Topic:   p.cfg.topic,
			Key:     r.Key,
			Headers: r.Headers,
		}, failure.NewFailureError("serialization", err))
		resultCh <- SendResult{Err: fmt.Errorf("flowgate/producer: serialization failed: %w", err)}
		close(resultCh)
		return resultCh
	}

	deliveryChan := make(chan confluent.Event, 1)
	msg := p.buildMessage(r.Key, r.Headers, payload)
	if err := p.kafka.Produce(msg, deliveryChan); err != nil {
		resultCh <- SendResult{Err: fmt.Errorf("flowgate/producer: produce failed: %w", err)}
		close(resultCh)
		return resultCh
	}

	go func() {
		e := <-deliveryChan
		m := e.(*confluent.Message)
		resultCh <- SendResult{Err: m.TopicPartition.Error}
		close(resultCh)
	}()

	return resultCh
}

// Flush waits until all enqueued messages are delivered or the context expires.
// If the context has no deadline, ShutdownTimeout is used.
//
// Example:
//
//	err := p.Flush(ctx)
func (p *Producer[T]) Flush(ctx context.Context) error {
	deadline, ok := ctx.Deadline()
	var timeoutMs int
	if ok {
		ms := time.Until(deadline).Milliseconds()
		if ms <= 0 {
			return ctx.Err()
		}
		timeoutMs = int(ms)
	} else {
		timeoutMs = int(p.cfg.shutdownTimeout.Milliseconds())
	}
	remaining := p.kafka.Flush(timeoutMs)
	if remaining > 0 {
		return fmt.Errorf("flowgate/producer: flush timed out with %d messages remaining", remaining)
	}
	return nil
}

// Close flushes remaining messages and shuts down the producer.
// Uses the configured ShutdownTimeout. Close is idempotent.
//
// Example:
//
//	defer p.Close()
func (p *Producer[T]) Close() {
	p.once.Do(func() {
		log.Printf("flowgate/producer: shutting down — flushing remaining records (timeout=%s)", p.cfg.shutdownTimeout)
		p.kafka.Flush(int(p.cfg.shutdownTimeout.Milliseconds()))
		p.kafka.Close() // closes Events() channel → runEvents goroutine exits naturally
		p.wg.Wait()
		log.Printf("flowgate/producer: shutdown complete")
	})
}

// runEvents drains librdkafka's delivery event channel.
// For each failed delivery, it calls FailureHandler.OnFailure.
// Exits naturally when kafka.Close() closes the Events() channel.
func (p *Producer[T]) runEvents() {
	defer p.wg.Done()
	for e := range p.kafka.Events() {
		msg, ok := e.(*confluent.Message)
		if !ok {
			continue
		}
		if msg.TopicPartition.Error != nil {
			headers := make(map[string]string, len(msg.Headers))
			for _, h := range msg.Headers {
				headers[h.Key] = string(h.Value)
			}
			key := ""
			if msg.Key != nil {
				key = string(msg.Key)
			}
			p.cfg.failureHandler.OnFailure(failure.RawRecord{
				Topic:   p.cfg.topic,
				Key:     key,
				Headers: headers,
				Payload: msg.Value,
			}, failure.NewFailureError("delivery", msg.TopicPartition.Error))
		}
	}
}

// serialize converts a domain struct to Confluent wire-format Avro bytes.
func (p *Producer[T]) serialize(payload T) ([]byte, error) {
	codec, err := p.registry.GetCodecByID(p.schemaID)
	if err != nil {
		return nil, err
	}

	native, err := schema.StructToNativeMap(payload)
	if err != nil {
		return nil, err
	}

	return schema.Serialize(codec, p.schemaID, native)
}

// buildMessage constructs a confluent.Message from raw fields.
func (p *Producer[T]) buildMessage(key string, headers map[string]string, payload []byte) *confluent.Message {
	kHeaders := make([]confluent.Header, 0, len(headers))
	for k, v := range headers {
		kHeaders = append(kHeaders, confluent.Header{Key: k, Value: []byte(v)})
	}

	var keyBytes []byte
	if key != "" {
		keyBytes = []byte(key)
	}

	return &confluent.Message{
		TopicPartition: confluent.TopicPartition{
			Topic:     &p.cfg.topic,
			Partition: confluent.PartitionAny,
		},
		Key:     keyBytes,
		Value:   payload,
		Headers: kHeaders,
	}
}

// validateConfig checks that all required fields are set.
func validateConfig(cfg *config) error {
	if cfg.brokers == "" {
		return fmt.Errorf("flowgate/producer: WithBrokers is required")
	}
	if cfg.topic == "" {
		return fmt.Errorf("flowgate/producer: WithTopic is required")
	}
	if cfg.registryURL == "" {
		return fmt.Errorf("flowgate/producer: WithSchemaRegistry is required")
	}
	if cfg.schemaPath == "" && cfg.schemaStruct == nil {
		return fmt.Errorf("flowgate/producer: either WithSchemaFile or WithSchemaStruct is required")
	}
	return nil
}
