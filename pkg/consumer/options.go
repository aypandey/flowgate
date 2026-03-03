package consumer

import (
	"time"

	"github.com/aypandey/flowgate/pkg/failure"
	confluent "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// config holds all configuration for a Consumer instance.
type config struct {
	brokers         string
	topic           string
	groupID         string
	registryURL     string
	schemaPath      string
	schemaStruct    interface{}
	batchSize       int
	batchFlushInterval time.Duration
	shutdownTimeout time.Duration
	failureHandler  failure.FailureHandler
	rawConfig       map[string]interface{}
}

func defaultConfig() *config {
	return &config{
		batchSize:          100,
		batchFlushInterval: 500 * time.Millisecond,
		shutdownTimeout:    10 * time.Second,
		rawConfig:          make(map[string]interface{}),
	}
}

// Option is a functional option for configuring a Consumer.
type Option func(*config)

// WithBrokers sets the Kafka bootstrap brokers.
func WithBrokers(brokers string) Option {
	return func(c *config) { c.brokers = brokers }
}

// WithTopic sets the Kafka topic to consume from.
func WithTopic(topic string) Option {
	return func(c *config) { c.topic = topic }
}

// WithGroupID sets the Kafka consumer group ID.
// Messages are load-balanced across all consumers in the same group.
func WithGroupID(groupID string) Option {
	return func(c *config) { c.groupID = groupID }
}

// WithSchemaRegistry sets the Confluent Schema Registry URL.
func WithSchemaRegistry(url string) Option {
	return func(c *config) { c.registryURL = url }
}

// WithSchemaFile sets the path to the Avro schema file (.avsc) for schema-first teams.
func WithSchemaFile(path string) Option {
	return func(c *config) { c.schemaPath = path }
}

// WithSchemaStruct sets the Go struct for code-first schema teams.
func WithSchemaStruct(s interface{}) Option {
	return func(c *config) { c.schemaStruct = s }
}

// WithBatchSize sets the number of records to accumulate before invoking
// the batch handler. Superseded by fetch.max.bytes in RawConfig.
func WithBatchSize(size int) Option {
	return func(c *config) { c.batchSize = size }
}

// WithBatchFlushInterval sets the maximum time to wait before flushing
// a partial batch even if batch size hasn't been reached.
func WithBatchFlushInterval(d time.Duration) Option {
	return func(c *config) { c.batchFlushInterval = d }
}

// WithShutdownTimeout sets how long Close() waits for in-flight
// message processing to complete before forcing shutdown.
func WithShutdownTimeout(d time.Duration) Option {
	return func(c *config) { c.shutdownTimeout = d }
}

// WithFailureHandler sets the handler for messages that fail processing.
func WithFailureHandler(fh failure.FailureHandler) Option {
	return func(c *config) { c.failureHandler = fh }
}

// WithRawConfig passes raw confluent-kafka-go ConfigMap entries directly.
// Use for advanced tuning like fetch.max.bytes, max.poll.interval.ms etc.
//
// Example:
//
//	consumer.WithRawConfig(map[string]interface{}{
//	    "fetch.max.bytes":      52428800,
//	    "max.poll.interval.ms": 300000,
//	})
func WithRawConfig(raw map[string]interface{}) Option {
	return func(c *config) {
		for k, v := range raw {
			c.rawConfig[k] = v
		}
	}
}

// buildConsumerKafkaConfig constructs the confluent ConfigMap via a three-layer merge:
//
//	Layer 1 — opinionated defaults (auto.offset.reset, enable.auto.commit=false)
//	Layer 2 — no flowgate-to-Confluent translations (batch config is Go-only)
//	Layer 3 — WithRawConfig entries (developer always wins)
func (c *config) buildConsumerKafkaConfig() confluent.ConfigMap {
	// Layer 1: opinionated defaults
	cfg := confluent.ConfigMap{
		"bootstrap.servers":  c.brokers,
		"group.id":           c.groupID,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false, // flowgate commits manually after handler success
	}

	// Layer 3: raw config overrides (developer always wins)
	for k, v := range c.rawConfig {
		cfg[k] = v
	}

	return cfg
}
