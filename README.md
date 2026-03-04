# flowgate

flowgate is a Go library that gives application teams a schema-aware, version-safe way to produce and consume Kafka messages. It wraps [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go) and integrates with Confluent Schema Registry so that teams only deal with their own domain structs — serialization, schema registration, offset management, and failure routing are handled internally.

---

## Table of Contents

- [API Design Philosophy](#api-design-philosophy)
- [Schema Versioning Strategy](#schema-versioning-strategy)
- [Failure Handling](#failure-handling)
- [Example Usage by an Application Team](#example-usage-by-an-application-team)
- [Running the Examples](#running-the-examples)
- [Configuration Reference](#configuration-reference)

---

## API Design Philosophy

### Why this abstraction?

Kafka's wire protocol communicates over TCP using a binary-encoded request/response protocol between clients and brokers. The Kafka client specification defines a **bidirectional compatibility policy**: new clients can talk to old brokers, and old clients can talk to new brokers. This allows users to upgrade either side independently — there is no required coordinated downtime.

flowgate is an extension of the Kafka client that applies the same compatibility thinking one level up, to the **message schema**. The goal is to give teams a versioning strategy for their event schemas that mirrors the safety guarantee Kafka already provides at the transport level: producers and consumers can move at their own pace, as long as they follow the minor/major versioning contract.

### Design decisions

**1. The library hides boilerplate, not Kafka.**

The Kafka client is not hidden — it is wrapped. flowgate takes ownership of the parts every team rewrites:

- Avro serialization and the Confluent wire format (magic byte + 4-byte schema ID prefix)
- Schema registration, caching, and retry against Schema Registry
- Manual offset commits after handler success (at-least-once delivery)
- Delivery event draining via a background goroutine
- Graceful shutdown and flush sequencing

Every low-level librdkafka property is still reachable through `WithRawConfig`. flowgate translates its own options into the equivalent Confluent ConfigMap keys — teams only override what they care about. There is no hidden magic.

**2. Generic, type-safe API.**

```go
p, err := producer.NewProducer[order.Order](...)
c, err := consumer.NewConsumer[order.Order](...)
```

The producer and consumer are parameterised on the team's domain struct. There is no `interface{}` visible to callers, no manual type assertion, no reflection at call sites. The handler receives a fully typed `*record.ConsumerRecord[T]`.

**3. Schema is owned by the team, not a central config.**

A common pattern is to store the schema path in an environment variable or config file, separate from the code that uses it. This creates a gap: the struct and the schema can drift out of sync silently.

flowgate closes this gap by co-locating the schema with the struct. Any Go type that implements:

```go
type Provider interface {
    AvroSchema() string
}
```

is automatically detected by `NewProducer`. The schema string (embedded from an `.avsc` file at compile time) is registered on startup and validated against the struct's fields. If the struct is incompatible with the registered schema, the producer fails immediately at startup — not on the first message.

**4. Startup validation.**

After schema registration, the producer serializes a zero value of `T` against the registered schema. A type mismatch (e.g. `Amount float64` in Go vs `"type": "string"` in Avro) is caught at `NewProducer` time, before any real messages are sent. This makes schema drift a deploy-time failure rather than a runtime surprise.

**5. Three-layer config merge.**

Producer and consumer configuration is built by merging three layers in order:

```
Layer 1 — opinionated defaults        (e.g. enable.idempotence=true, acks=all, enable.auto.commit=false)
Layer 2 — flowgate option translations (e.g. RetryConfig → message.send.max.retries)
Layer 3 — WithRawConfig               (developer always wins)
```

This means safe defaults are active out of the box, flowgate options are first-class citizens that translate cleanly into Confluent properties, and teams can always escape to raw config for anything not exposed by the library API.

---

## Schema Versioning Strategy

### The problem

When a producer changes a message schema — adding a field, changing a type — consumers break unless the change is coordinated. In practice, coordinating producer and consumer deployments across teams is expensive and error-prone.

### The approach

flowgate adopts a **versioned package pattern**. Each schema version lives in its own Go package directory. The Avro schema file is embedded into the package at compile time so the struct and schema are always in sync.

```
payments-service/
└── order/
    ├── v1_0_0/
    │   ├── order.avsc    ← Avro schema for this version
    │   └── order.go      ← Go struct + embedded schema + Version constant
    ├── v2_0_0/
    │   ├── order.avsc
    │   └── order.go
    └── v3_0_0/
        ├── order.avsc
        └── order.go
```

Each package embeds its own schema and exposes an `AvroSchema()` method:

```go
// order/v1_0_0/order.go
package order

import _ "embed"

//go:embed order.avsc
var avroSchema string

const Version = "1.0.0"

type Order struct {
    OrderID    string  `avro:"order_id"`
    CustomerID string  `avro:"customer_id"`
    Amount     float64 `avro:"amount"`
    Currency   string  `avro:"currency"`
    Status     string  `avro:"status"`
    CreatedAt  string  `avro:"created_at"`
}

func (Order) AvroSchema() string { return avroSchema }
```

When a team creates `producer.NewProducer[orderv1.Order](...)`, flowgate detects that `orderv1.Order` implements `schema.Provider`, reads the embedded schema string, and registers it in Schema Registry. No `WithSchemaFile` option is needed.

### Minor versions — backward compatible (same topic)

A minor version adds an optional field. Avro requires a `"default"` value on the new field so that consumers reading messages without that field get a sensible zero value.

```json
// order/v2_0_0/order.avsc — adds discount_amount with default 0.0
{
  "type": "record",
  "name": "Order",
  "namespace": "com.example.payments",
  "fields": [
    {"name": "order_id",        "type": "string"},
    {"name": "customer_id",     "type": "string"},
    {"name": "amount",          "type": "double"},
    {"name": "currency",        "type": "string"},
    {"name": "status",          "type": "string"},
    {"name": "created_at",      "type": "string"},
    {"name": "discount_amount", "type": "double", "default": 0.0}
  ]
}
```

Both directions work without coordination:

| Producer | Consumer | Outcome |
|---|---|---|
| v2 (has `discount_amount`) | v1 (no `DiscountAmount` field) | Extra field silently ignored. No error. |
| v1 (no `discount_amount`) | v2 (has `DiscountAmount float64`) | Missing field defaults to `0.0`. No error. |

Teams can upgrade producers and consumers independently at their own pace.

### Major versions — breaking change (new topic required)

A major version makes an incompatible change: a field type change, field removal, or rename. These changes cannot be read by old consumers. The correct strategy is:

1. Create a new topic (e.g. `payments.order.v3`)
2. Create a new versioned package (`v3_0_0/`) with the new schema
3. Register with `CompatibilityNone` on the new subject (first registration always succeeds)
4. Migrate consumers to the new topic; decommission the old topic after cut-over

```go
producer.NewProducer[orderv3.Order](
    producer.WithBrokers("localhost:9092"),
    producer.WithSchemaRegistry("http://localhost:8081"),
    producer.WithTopic("payments.order.v3"),
    producer.WithSchemaCompatibility(schema.CompatibilityNone),
)
```

If a v2 consumer accidentally reads from a v3 topic (type mismatch on `amount`: `double` → `string`), flowgate catches the deserialization error and routes the raw bytes to the `FailureHandler`. The consumer handler is never called. No panic, no data corruption, no silent wrong value.

### Why this pattern works

A team consuming an event only needs to import the version they want to deserialize into. The schema ID on each Kafka message tells flowgate which Avro schema was used to write it. flowgate fetches that writer schema from the registry (cached after first fetch) and uses it to decode the bytes, then maps the result into the consumer's target struct. Producer and consumer can be on different package versions as long as the schemas are compatible.

Upgrading is a one-line change:

```go
// Before
import orderv1 "payments-service/order/v1_0_0"
producer.NewProducer[orderv1.Order](...)

// After — schema auto-registered, no other change required
import orderv2 "payments-service/order/v2_0_0"
producer.NewProducer[orderv2.Order](...)
```

---

## Failure Handling

### Philosophy

The guiding rule is: **no message should be silently dropped unless the team has explicitly asked for that behaviour.**

Every failure in the pipeline — serialization error, delivery timeout after retries, handler returning an error, deserialization type mismatch — is passed to a `FailureHandler`. The team decides what to do with it. The default logs to a structured file so messages can be inspected and replayed.

### Failure points

flowgate routes failures to the handler at four points in the pipeline:

| Stage | When it fires |
|---|---|
| `serialization` | Producer: struct cannot be encoded against the Avro schema |
| `delivery` | Producer: librdkafka exhausted retries and could not deliver to broker |
| `deserialization` | Consumer: schema type mismatch or corrupt wire bytes |
| `handler` | Consumer: the team's handler function returned an error |

For `handler` failures, the offset is **not committed** — the message will be redelivered on restart, preserving at-least-once semantics. For `deserialization` failures, the offset is committed to avoid infinite reprocessing of a permanently unreadable message (the raw bytes are preserved in the failure handler for inspection).

### The interface

```go
type FailureHandler interface {
    OnFailure(ctx context.Context, record failure.RawRecord, err error)
}
```

`RawRecord` carries the original bytes, topic, partition, offset, key, and headers — everything needed to replay or audit the failed message. `err` is a `*failure.FailureError` which includes the pipeline stage where the failure occurred.

Any struct with an `OnFailure` method satisfies the interface with no imports beyond the `failure` package. Teams do not need to embed or inherit anything.

### Built-in handlers

| Handler | Behaviour |
|---|---|
| `LoggingFailureHandler` (default) | Writes one NDJSON line per failure to a local file. Each line is self-contained and can be replayed. |
| `KafkaDLQFailureHandler` | Produces the raw bytes to a dead-letter Kafka topic with `flowgate-failure-reason` and `flowgate-original-topic` headers. The DLQ producer uses `acks=all` and idempotent delivery to ensure DLQ messages are not themselves lost. |
| `NoOpFailureHandler` | Discards silently. Use only when loss is explicitly acceptable. |

### Custom handler

Teams can plug in any implementation. Examples: write to SQS, send a PagerDuty alert, persist to a database for a replay UI.

```go
type SQSHandler struct {
    client   *sqs.Client
    queueURL string
}

func (h *SQSHandler) OnFailure(ctx context.Context, r failure.RawRecord, err error) {
    h.client.SendMessage(ctx, &sqs.SendMessageInput{
        QueueUrl:    &h.queueURL,
        MessageBody: aws.String(base64.StdEncoding.EncodeToString(r.Payload)),
        MessageAttributes: map[string]types.MessageAttributeValue{
            "FailureReason": {DataType: aws.String("String"), StringValue: aws.String(err.Error())},
            "OriginalTopic": {DataType: aws.String("String"), StringValue: aws.String(r.Topic)},
        },
    })
}
```

---

## Example Usage by an Application Team

This section walks through the full journey of an application team adopting flowgate for a payments service that produces and consumes `Order` events.

### Step 1 — Define the schema package

The team creates a versioned package for their event. The schema lives next to the struct.

```
payments-service/
└── order/
    └── v1_0_0/
        ├── order.avsc
        └── order.go
```

**`order/v1_0_0/order.avsc`**

```json
{
  "type": "record",
  "name": "Order",
  "namespace": "com.example.payments",
  "doc": "Payment order event, schema version 1.0.0.",
  "fields": [
    {"name": "order_id",    "type": "string", "doc": "Unique order identifier."},
    {"name": "customer_id", "type": "string", "doc": "Customer who placed the order."},
    {"name": "amount",      "type": "double", "doc": "Total order value."},
    {"name": "currency",    "type": "string", "doc": "ISO 4217 currency code."},
    {"name": "status",      "type": "string", "doc": "PENDING | CONFIRMED | SHIPPED | CANCELLED."},
    {"name": "created_at",  "type": "string", "doc": "RFC3339 timestamp."}
  ]
}
```

**`order/v1_0_0/order.go`**

```go
package order

import _ "embed"

//go:embed order.avsc
var avroSchema string

const Version = "1.0.0"

type Order struct {
    OrderID    string  `avro:"order_id"`
    CustomerID string  `avro:"customer_id"`
    Amount     float64 `avro:"amount"`
    Currency   string  `avro:"currency"`
    Status     string  `avro:"status"`
    CreatedAt  string  `avro:"created_at"`
}

// AvroSchema satisfies schema.Provider.
// flowgate reads this at startup — teams never call it directly.
func (Order) AvroSchema() string { return avroSchema }
```

### Step 2 — Producer service

The producer team imports their versioned schema package and calls `NewProducer`. flowgate registers the schema automatically.

```go
package main

import (
    "context"
    "log"
    "time"

    orderv1 "payments-service/order/v1_0_0"
    "github.com/aypandey/flowgate/pkg/producer"
    "github.com/aypandey/flowgate/pkg/record"
)

func main() {
    ctx := context.Background()

    p, err := producer.NewProducer[orderv1.Order](
        producer.WithBrokers("localhost:9092"),
        producer.WithSchemaRegistry("http://localhost:8081"),
        producer.WithTopic("payments.order"),
        // orderv1.Order implements schema.Provider — no WithSchemaFile needed.
        // Schema is registered and validated against the struct at startup.
    )
    if err != nil {
        // Startup fails here if the struct is incompatible with the registered schema.
        log.Fatalf("producer init: %v", err)
    }
    defer p.Close()

    order := orderv1.Order{
        OrderID:    "ord-001",
        CustomerID: "cust-abc",
        Amount:     149.99,
        Currency:   "USD",
        Status:     "PENDING",
        CreatedAt:  time.Now().UTC().Format(time.RFC3339),
    }

    err = p.Send(ctx, record.RecordOf(order).
        WithKey(order.OrderID).
        WithHeader("source-service", "payments-api"),
    )
    if err != nil {
        log.Printf("send failed: %v", err)
    }

    // Flush ensures all enqueued messages are delivered before the process exits.
    if err := p.Flush(ctx); err != nil {
        log.Printf("flush: %v", err)
    }
}
```

### Step 3 — Consumer service

A separate team consumes the same topic. They import the same versioned package and get a typed handler — no schema file, no deserialization code.

```go
package main

import (
    "context"
    "log"
    "os"
    "os/signal"
    "syscall"

    orderv1 "payments-service/order/v1_0_0"
    "github.com/aypandey/flowgate/pkg/consumer"
    "github.com/aypandey/flowgate/pkg/record"
)

func main() {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    go func() {
        sig := make(chan os.Signal, 1)
        signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
        <-sig
        cancel()
    }()

    c, err := consumer.NewConsumer[orderv1.Order](
        consumer.WithBrokers("localhost:9092"),
        consumer.WithSchemaRegistry("http://localhost:8081"),
        consumer.WithTopic("payments.order"),
        consumer.WithGroupID("order-fulfilment-service"),
        // The consumer reads the schema ID from the Confluent wire header of each message.
        // It fetches the writer's schema from the registry and deserializes into orderv1.Order.
        // No WithSchemaFile is needed.
    )
    if err != nil {
        log.Fatalf("consumer init: %v", err)
    }
    defer c.Close()

    // Subscribe blocks until ctx is cancelled or an unrecoverable error occurs.
    // Offset is committed only after the handler returns nil — at-least-once delivery.
    // If the handler returns an error, the message goes to FailureHandler and is not committed.
    err = c.Subscribe(ctx, func(r *record.ConsumerRecord[orderv1.Order]) error {
        order := r.Payload
        log.Printf("processing order %s amount=%.2f %s status=%s",
            order.OrderID, order.Amount, order.Currency, order.Status)

        return fulfil(order)
    })
    if err != nil && err != context.Canceled {
        log.Fatalf("consumer: %v", err)
    }
}

func fulfil(o orderv1.Order) error {
    // business logic here
    return nil
}
```

### Step 4 — Producer upgrades to v2 (minor, backward compatible)

The producer team adds a `discount_amount` field to their schema. They create a new `v2_0_0` package. No consumer changes are needed.

**`order/v2_0_0/order.avsc`** — `discount_amount` added with `"default": 0.0`

```json
{
  "type": "record",
  "name": "Order",
  "namespace": "com.example.payments",
  "fields": [
    {"name": "order_id",        "type": "string"},
    {"name": "customer_id",     "type": "string"},
    {"name": "amount",          "type": "double"},
    {"name": "currency",        "type": "string"},
    {"name": "status",          "type": "string"},
    {"name": "created_at",      "type": "string"},
    {"name": "discount_amount", "type": "double", "default": 0.0}
  ]
}
```

**`order/v2_0_0/order.go`**

```go
const Version = "2.0.0"

type Order struct {
    OrderID        string  `avro:"order_id"`
    CustomerID     string  `avro:"customer_id"`
    Amount         float64 `avro:"amount"`
    Currency       string  `avro:"currency"`
    Status         string  `avro:"status"`
    CreatedAt      string  `avro:"created_at"`
    DiscountAmount float64 `avro:"discount_amount,omitempty"`
}

func (Order) AvroSchema() string { return avroSchema }
```

The producer changes one import. Everything else is identical:

```go
// Before
import orderv1 "payments-service/order/v1_0_0"
producer.NewProducer[orderv1.Order](...)

// After
import orderv2 "payments-service/order/v2_0_0"
producer.NewProducer[orderv2.Order](...)
```

The v1 consumer keeps running unchanged. It reads v2 messages and silently ignores `discount_amount`. The `default: 0.0` in the schema ensures this round-trip is lossless and error-free.

### Step 5 — Wiring in a production failure handler

The default failure handler logs to a file. For production, the team swaps in a Kafka dead-letter queue:

```go
import "github.com/aypandey/flowgate/pkg/failure"

dlq, err := failure.NewKafkaDLQFailureHandler(
    "localhost:9092",
    "payments.order.dlq",
)
if err != nil {
    log.Fatal(err)
}
defer dlq.Close()

p, err := producer.NewProducer[orderv2.Order](
    producer.WithBrokers("localhost:9092"),
    producer.WithSchemaRegistry("http://localhost:8081"),
    producer.WithTopic("payments.order"),
    producer.WithFailureHandler(dlq),  // undeliverable messages go here
)
```

Failed messages arrive in `payments.order.dlq` with headers `flowgate-failure-reason` and `flowgate-original-topic`. A separate replay consumer can re-process them once the root cause is fixed.

---

## Running the Examples

The `example/` directory contains a working producer, consumer, and a compatibility test.

### Prerequisites

- Go 1.21+
- Docker and Docker Compose

### 1. Start local infrastructure

```bash
docker compose up -d
```

Starts Kafka on `localhost:9092`, Schema Registry on `http://localhost:8081`, and Kafka UI on `http://localhost:8080`.

### 2. Create topics

```bash
bash example/scripts/setup-topics.sh
```

### 3. Run the producer

```bash
go run example/producer/main.go
```

Expected output:

```
flowgate/producer: ready — topic=payments.order schemaID=3
queued order ord-001 (amount=149.99 USD)
queued order ord-002 (amount=299.00 EUR)
queued order ord-003 (amount=59.50 USD)
all orders sent successfully
```

### 4. Run the consumer

```bash
go run example/consumer/main.go
```

Press `Ctrl+C` to shut down gracefully. Expected output:

```
flowgate/consumer: ready — topic=payments.order groupID=order-indexing-service
received order ord-001 (status=PENDING amount=149.99 USD partition=0 offset=0)
received order ord-002 (status=CONFIRMED amount=299.00 EUR partition=0 offset=1)
received order ord-003 (status=SHIPPED amount=59.50 USD partition=0 offset=2)
```

### 5. Run the compatibility test

```bash
go run example/compat/main.go
```

Tests all three versioning scenarios across isolated topics:

```
── Scenario A — v2 producer → v1 consumer (minor, extra field ignored) ──
  PASS — v1 consumer read v2 messages; discount_amount silently ignored

── Scenario B — v1 producer → v2 consumer (minor, missing field defaults to zero) ──
  PASS — v2 consumer read v1 messages; missing discount_amount defaulted to 0.0

── Scenario C — v3 producer → v2 consumer (major, type mismatch) ──
  PASS — FailureHandler caught 2 deserialization errors; handler never called
        → v3 messages require a new topic; v1/v2 consumers must not read from it
```

---

## Configuration Reference

### Producer

| Option | Default | Description |
|---|---|---|
| `WithBrokers(string)` | — | **Required.** Bootstrap brokers |
| `WithTopic(string)` | — | **Required.** Target topic |
| `WithSchemaRegistry(string)` | — | **Required.** Schema Registry URL |
| `WithBufferConfig(BufferConfig)` | Size=100, Interval=500ms | Internal queue tuning (maps to librdkafka queue properties) |
| `WithRetryConfig(RetryConfig)` | 5 attempts, 100ms–2s backoff | Schema registration retry policy |
| `WithShutdownTimeout(Duration)` | 10s | How long `Close()` waits to flush |
| `WithFailureHandler(FailureHandler)` | `LoggingFailureHandler` | Receives undeliverable messages |
| `WithSchemaCompatibility(Mode)` | *(not set)* | Sets SR subject compatibility before registration |
| `WithRawConfig(map[string]interface{})` | — | Direct pass-through to confluent-kafka-go ConfigMap |

### Consumer

| Option | Default | Description |
|---|---|---|
| `WithBrokers(string)` | — | **Required.** Bootstrap brokers |
| `WithTopic(string)` | — | **Required.** Topic to consume |
| `WithGroupID(string)` | — | **Required.** Consumer group ID |
| `WithSchemaRegistry(string)` | — | **Required.** Schema Registry URL |
| `WithBatchSize(int)` | 100 | Max records per batch (`SubscribeBatch` only) |
| `WithBatchFlushInterval(Duration)` | 500ms | Max wait before flushing a partial batch |
| `WithShutdownTimeout(Duration)` | 10s | How long `Close()` waits for in-flight processing |
| `WithFailureHandler(FailureHandler)` | `LoggingFailureHandler` | Receives failed messages |
| `WithRawConfig(map[string]interface{})` | — | Direct pass-through to confluent-kafka-go ConfigMap |
