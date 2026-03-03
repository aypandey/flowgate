# flowgate

A production-ready Go library for building **schema-aware, resilient Kafka producers and consumers**.

flowgate wraps [confluent-kafka-go v2](https://github.com/confluentinc/confluent-kafka-go) and integrates directly with Confluent Schema Registry to give you Avro serialization, schema evolution, double-buffered publishing, exponential-backoff retries, and structured failure handling — all behind a clean generic API that keeps your domain structs free of Kafka concerns.

```
go get github.com/aypandey/flowgate
```

---

## Features

| Feature | Detail |
|---|---|
| **Schema-first or code-first** | Supply a `.avsc` file _or_ let flowgate generate Avro from struct tags |
| **Schema evolution** | Full reader/writer resolution via Confluent Schema Registry |
| **Buffered producer** | Double-buffer (ping-pong) for non-blocking throughput |
| **Idempotent by default** | `enable.idempotence=true`, `acks=all`, exactly-once-safe |
| **Exponential backoff retries** | Configurable max attempts, initial/max backoff, multiplier |
| **At-least-once consumer** | Manual offset commits only after handler success |
| **Failure handler** | Pluggable `FailureHandler` — ships with NDJSON logging and no-op |
| **Generic API** | `Producer[T]` / `Consumer[T]` — zero casting in your handlers |
| **Raw config passthrough** | Any Kafka property via `WithRawConfig` |

---

## Quickstart

### 1. Start the local stack

```bash
docker compose up -d
```

This starts Kafka on `localhost:9092`, Schema Registry on `http://localhost:8081`, and a Kafka UI at `http://localhost:8080`.

### 2. Create topics

```bash
bash example/scripts/setup-topics.sh
```

### 3. Register schemas and validate compatibility

```bash
bash example/scripts/validate-schema.sh
```

### 4. Run the producer example

```bash
go run example/producer/main.go
```

### 5. Run the consumer example (in a separate terminal)

```bash
go run example/consumer/main.go
```

---

## Schema evolution

flowgate ships two example schemas for the `Order` event under `example/schemas/order/`:

```
example/schemas/order/
├── v1/
│   └── order.avsc   # 6 fields: order_id, customer_id, amount, currency, status, created_at
└── v2/
    └── order.avsc   # + discount_amount (double, default 0.0)
```

**v2 is backward compatible with v1** — v1 producers can publish to a v2-schema topic because `discount_amount` has a default of `0.0`. Confluent Schema Registry enforces this at registration time via `validate-schema.sh`.

### Avro struct tags

Map Go fields to Avro field names exactly like `encoding/json`:

```go
type Order struct {
    OrderID        string  `avro:"order_id"`
    CustomerID     string  `avro:"customer_id"`
    Amount         float64 `avro:"amount"`
    Currency       string  `avro:"currency"`
    Status         string  `avro:"status"`
    CreatedAt      string  `avro:"created_at"`
    DiscountAmount float64 `avro:"discount_amount,omitempty"` // omitted when zero
}
```

`omitempty` fields are excluded from the serialized map when they hold the zero value. The Avro schema must declare a `default` for those fields so they survive round-trips with older producers.

---

## Producer

```go
p, err := producer.NewProducer[model.Order](
    producer.WithBrokers("localhost:9092"),
    producer.WithSchemaRegistry("http://localhost:8081"),
    producer.WithTopic("payments.order"),
    producer.WithSchemaFile("example/schemas/order/v2/order.avsc"),
    producer.WithBufferConfig(producer.BufferConfig{
        Size:          50,             // flush after 50 records
        FlushInterval: 500 * time.Millisecond,
    }),
    producer.WithRetryConfig(producer.RetryConfig{
        MaxAttempts:       3,
        InitialBackoff:    100 * time.Millisecond,
        MaxBackoff:        1 * time.Second,
        BackoffMultiplier: 2.0,
    }),
    producer.WithShutdownTimeout(10 * time.Second),
)
if err != nil {
    log.Fatal(err)
}
defer p.Close() // flushes buffer, then closes

r := record.RecordOf(order).
    WithKey(order.OrderID).
    WithHeader("source-service", "payments-api")

if err := p.Send(ctx, r); err != nil {
    log.Printf("send failed: %v", err)
}

// Force-flush before process exit
p.Flush(ctx)
```

### Producer options

| Option | Default | Description |
|---|---|---|
| `WithBrokers(brokers)` | — | Comma-separated `host:port` list |
| `WithTopic(topic)` | — | Target topic |
| `WithSchemaRegistry(url)` | — | Schema Registry base URL |
| `WithSchemaFile(path)` | — | Path to `.avsc` file (schema-first) |
| `WithSchemaStruct(v)` | — | Generate schema from struct (code-first) |
| `WithBufferConfig(bc)` | size=100, interval=500ms | Flush thresholds |
| `WithRetryConfig(rc)` | 5 attempts, 100ms→2s exp backoff | Retry policy |
| `WithShutdownTimeout(d)` | 10s | Max time for graceful close |
| `WithFailureHandler(fh)` | `failure.NoOp` | Called on terminal failure |
| `WithRawConfig(m)` | — | Raw Kafka client config properties |

### Async send

```go
resultCh := p.SendAsync(ctx, r)
go func() {
    res := <-resultCh
    if res.Err != nil {
        log.Printf("async send failed: %v", res.Err)
    }
}()
```

---

## Consumer

```go
c, err := consumer.NewConsumer[model.Order](
    consumer.WithBrokers("localhost:9092"),
    consumer.WithSchemaRegistry("http://localhost:8081"),
    consumer.WithTopic("payments.order"),
    consumer.WithGroupID("order-indexing-service"),
    consumer.WithSchemaFile("example/schemas/order/v2/order.avsc"),
    consumer.WithShutdownTimeout(10 * time.Second),
)
if err != nil {
    log.Fatal(err)
}
defer c.Close()

// Single-message handler
err = c.Subscribe(ctx, func(r *record.ConsumerRecord[model.Order]) error {
    order := r.Payload
    log.Printf("order %s partition=%d offset=%d", order.OrderID, r.Partition, r.Offset)
    return index(order) // offset committed only on nil return
})
```

### Batch handler

```go
err = c.SubscribeBatch(ctx, func(records []*record.ConsumerRecord[model.Order]) error {
    // bulk-write to DB, then return nil to commit all offsets
    return bulkInsert(records)
})
```

### Consumer options

| Option | Default | Description |
|---|---|---|
| `WithBrokers(brokers)` | — | Comma-separated `host:port` list |
| `WithTopic(topic)` | — | Topic to subscribe to |
| `WithGroupID(id)` | — | Consumer group ID |
| `WithSchemaRegistry(url)` | — | Schema Registry base URL |
| `WithSchemaFile(path)` | — | Path to `.avsc` file (schema-first) |
| `WithSchemaStruct(v)` | — | Generate schema from struct (code-first) |
| `WithBatchSize(n)` | 100 | Records per batch (SubscribeBatch) |
| `WithBatchFlushInterval(d)` | 500ms | Max wait before partial batch flush |
| `WithShutdownTimeout(d)` | 10s | Max time for graceful close |
| `WithFailureHandler(fh)` | `failure.NoOp` | Called on handler/deserialization error |
| `WithRawConfig(m)` | — | Raw Kafka client config properties |

### Offset commit semantics

| Event | Offset committed? |
|---|---|
| Handler returns `nil` | Yes |
| Handler returns error | **No** — message will be redelivered (at-least-once) |
| Deserialization error | Yes — poison pill skipped, `FailureHandler` called |

---

## ConsumerRecord fields

```go
type ConsumerRecord[T any] struct {
    Payload   T
    Topic     string
    Partition int32
    Offset    int64
    Key       string
    Headers   map[string]string
    SchemaID  int
}
```

---

## Failure handling

Every terminal failure (all retries exhausted on produce, deserialization error, handler error) is routed to a `FailureHandler`.

### Logging handler — writes NDJSON for replay

```go
fh, err := failure.NewLoggingFailureHandler("/var/log/flowgate-failures.ndjson")
if err != nil {
    log.Fatal(err)
}
defer fh.Close()

p, _ := producer.NewProducer[model.Order](
    // ...
    producer.WithFailureHandler(fh),
)
```

Each line written is a complete JSON object:

```json
{"timestamp":"2026-03-03T10:00:00Z","topic":"payments.order","partition":0,"offset":42,"key":"ord-001","headers":{"source-service":"payments-api"},"payload":[...],"error":"context deadline exceeded","stage":"produce"}
```

### No-op handler — explicit message loss opt-in

```go
producer.WithFailureHandler(failure.NoOp)
```

### Custom handler

```go
type MyHandler struct{}

func (h *MyHandler) OnFailure(r failure.RawRecord, err error) {
    // publish to a dead-letter topic, emit a metric, alert, etc.
}
```

---

## Code-first schema generation

If you don't want to maintain `.avsc` files, flowgate can generate an Avro schema directly from your struct:

```go
producer.WithSchemaStruct(model.Order{})
```

The generated schema uses `avro` struct tags to determine field names. Fields tagged `omitempty` become `["null", <type>]` unions with a `null` default.

This is convenient for greenfield services, but schema-first (`.avsc`) is recommended for services that participate in schema evolution across teams.

---

## Project layout

```
flowgate/
├── pkg/
│   ├── record/          # ProducerRecord[T] and ConsumerRecord[T] — Kafka envelope types
│   ├── producer/        # Producer[T] — buffered, retrying, schema-aware Kafka producer
│   ├── consumer/        # Consumer[T] — at-least-once, schema-aware Kafka consumer
│   ├── schema/          # Avro serialization, Schema Registry client, code-first generator
│   └── failure/         # FailureHandler interface, LoggingFailureHandler, NoOpFailureHandler
└── example/
    ├── model/           # Order domain struct with avro tags
    ├── producer/        # Runnable producer example
    ├── consumer/        # Runnable consumer example
    ├── schemas/
    │   └── order/
    │       ├── v1/order.avsc
    │       └── v2/order.avsc
    └── scripts/
        ├── setup-topics.sh      # Create Kafka topics via docker compose
        └── validate-schema.sh   # Register schemas and check backward compatibility
```

---

## Stack

- [confluent-kafka-go v2](https://github.com/confluentinc/confluent-kafka-go) — Kafka client (librdkafka-based)
- [goavro v2](https://github.com/linkedin/goavro) — Avro codec
- [srclient](https://github.com/riferrei/srclient) — Confluent Schema Registry client

---

## License

MIT
