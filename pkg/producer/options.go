package producer

import (
	"time"

	"github.com/aypandey/flowgate/pkg/failure"
	"github.com/aypandey/flowgate/pkg/schema"
	confluent "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// config holds all configuration for a Producer instance.
// It is populated by functional options and never exposed directly to teams.
type config struct {
	brokers             string
	topic               string
	registryURL         string
	schemaString        string                  // embedded schema JSON — set automatically when T implements schema.Provider
	schemaPath          string                  // path to .avsc file (schema-first fallback)
	schemaStruct        interface{}             // struct with avro tags (code-first fallback)
	schemaCompatibility schema.CompatibilityMode // optional: set subject-level SR compatibility before registration
	bufferConfig        BufferConfig
	retryConfig         RetryConfig
	shutdownTimeout     time.Duration
	failureHandler      failure.FailureHandler
	rawConfig           map[string]interface{}
}

// defaultConfig returns sensible production-ready defaults.
// Teams only need to override what they care about.
func defaultConfig() *config {
	return &config{
		bufferConfig: BufferConfig{
			Size:          100,
			FlushInterval: 500 * time.Millisecond,
		},
		retryConfig: RetryConfig{
			MaxAttempts:       5,
			InitialBackoff:    100 * time.Millisecond,
			MaxBackoff:        2 * time.Second,
			BackoffMultiplier: 2.0,
		},
		shutdownTimeout: 10 * time.Second,
		rawConfig:       make(map[string]interface{}),
	}
}

// Option is a functional option for configuring a Producer.
type Option func(*config)

// BufferConfig controls the double-buffer behaviour of the producer.
type BufferConfig struct {
	// Size is the maximum number of records in each buffer before a flush is triggered.
	Size int
	// FlushInterval is the maximum time to wait before flushing even if buffer is not full.
	FlushInterval time.Duration
}

// RetryConfig controls retry and backoff behaviour for failed Kafka sends.
type RetryConfig struct {
	// MaxAttempts is the total number of attempts (1 = no retry).
	MaxAttempts int
	// InitialBackoff is the wait time before the first retry.
	InitialBackoff time.Duration
	// MaxBackoff is the upper bound on backoff duration.
	MaxBackoff time.Duration
	// BackoffMultiplier is the exponential growth factor (e.g. 2.0 = double each time).
	BackoffMultiplier float64
}

// WithBrokers sets the Kafka bootstrap brokers.
//
// Example:
//
//	producer.WithBrokers("localhost:9092")
//	producer.WithBrokers("broker1:9092,broker2:9092")
func WithBrokers(brokers string) Option {
	return func(c *config) { c.brokers = brokers }
}

// WithTopic sets the Kafka topic this producer will send messages to.
// One producer instance maps to exactly one topic.
//
// Example:
//
//	producer.WithTopic("payments.order")
func WithTopic(topic string) Option {
	return func(c *config) { c.topic = topic }
}

// WithSchemaRegistry sets the Confluent Schema Registry URL.
//
// Example:
//
//	producer.WithSchemaRegistry("http://localhost:8081")
func WithSchemaRegistry(url string) Option {
	return func(c *config) { c.registryURL = url }
}

// WithSchemaFile sets the path to an Avro schema file (.avsc) for schema-first teams.
// The schema is registered on producer startup.
//
// Example:
//
//	producer.WithSchemaFile("schemas/order/v1/order.avsc")
func WithSchemaFile(path string) Option {
	return func(c *config) { c.schemaPath = path }
}

// WithSchemaStruct sets the Go struct (with avro struct tags) for code-first teams.
// The library generates and registers the Avro schema from the struct at startup.
//
// Example:
//
//	producer.WithSchemaStruct(Order{})
func WithSchemaStruct(s interface{}) Option {
	return func(c *config) { c.schemaStruct = s }
}

// WithBufferConfig overrides the default double-buffer configuration.
//
// Example:
//
//	producer.WithBufferConfig(producer.BufferConfig{
//	    Size:          200,
//	    FlushInterval: 1 * time.Second,
//	})
func WithBufferConfig(bc BufferConfig) Option {
	return func(c *config) { c.bufferConfig = bc }
}

// WithRetryConfig overrides the default retry and backoff configuration.
//
// Example:
//
//	producer.WithRetryConfig(producer.RetryConfig{
//	    MaxAttempts:       3,
//	    InitialBackoff:    50 * time.Millisecond,
//	    MaxBackoff:        1 * time.Second,
//	    BackoffMultiplier: 2.0,
//	})
func WithRetryConfig(rc RetryConfig) Option {
	return func(c *config) { c.retryConfig = rc }
}

// WithShutdownTimeout sets how long Close() waits to flush remaining buffered
// records before giving up and delegating them to the FailureHandler.
//
// Example:
//
//	producer.WithShutdownTimeout(15 * time.Second)
func WithShutdownTimeout(d time.Duration) Option {
	return func(c *config) { c.shutdownTimeout = d }
}

// WithFailureHandler sets the handler for messages that could not be delivered
// after all retries are exhausted. Defaults to LoggingFailureHandler.
//
// Example:
//
//	producer.WithFailureHandler(failure.NoOp) // allow loss explicitly
func WithFailureHandler(fh failure.FailureHandler) Option {
	return func(c *config) { c.failureHandler = fh }
}

// WithSchemaCompatibility sets the subject-level compatibility mode in the Schema
// Registry before registering the schema. Use this when the global SR default
// is not appropriate for this topic.
//
// Minor/patch versions (add optional field with Avro default):
//
//	producer.WithSchemaCompatibility(schema.CompatibilityBackward)
//
// Major versions with breaking changes must use a new topic; set the new
// subject to NONE so the first registration is always accepted:
//
//	producer.WithSchemaCompatibility(schema.CompatibilityNone)
func WithSchemaCompatibility(mode schema.CompatibilityMode) Option {
	return func(c *config) { c.schemaCompatibility = mode }
}

// WithRawConfig passes raw confluent-kafka-go ConfigMap entries directly to
// the underlying Kafka producer. Use this for advanced tuning that flowgate
// does not expose through its high-level API.
//
// Example:
//
//	producer.WithRawConfig(map[string]interface{}{
//	    "compression.type": "snappy",
//	    "linger.ms":        5,
//	})
func WithRawConfig(raw map[string]interface{}) Option {
	return func(c *config) {
		for k, v := range raw {
			c.rawConfig[k] = v
		}
	}
}

// buildProducerKafkaConfig constructs the confluent ConfigMap via a three-layer merge:
//
//	Layer 1 — opinionated defaults (enable.idempotence, acks=all)
//	Layer 2 — flowgate option translations (RetryConfig, BufferConfig, ShutdownTimeout)
//	Layer 3 — WithRawConfig entries (developer always wins)
func (c *config) buildProducerKafkaConfig() confluent.ConfigMap {
	// Layer 1: opinionated defaults
	cfg := confluent.ConfigMap{
		"bootstrap.servers": c.brokers,
		"enable.idempotence": true,
		"acks":               "all",
	}

	// Layer 2: flowgate option translations
	cfg["message.send.max.retries"] = c.retryConfig.MaxAttempts
	cfg["retry.backoff.ms"] = int(c.retryConfig.InitialBackoff.Milliseconds())
	cfg["retry.backoff.max.ms"] = int(c.retryConfig.MaxBackoff.Milliseconds())
	cfg["queue.buffering.max.messages"] = c.bufferConfig.Size
	cfg["queue.buffering.max.ms"] = int(c.bufferConfig.FlushInterval.Milliseconds())
	cfg["message.timeout.ms"] = int(c.shutdownTimeout.Milliseconds())

	// Layer 3: raw config overrides (developer always wins)
	for k, v := range c.rawConfig {
		cfg[k] = v
	}

	return cfg
}
