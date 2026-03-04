// Compatibility test — demonstrates flowgate's schema versioning contract:
//
//   Minor/patch versions (e.g. v1→v2): backward compatible.
//     A v1 consumer can read v2 messages — the new field is silently ignored.
//     A v2 consumer can read v1 messages — the missing field defaults to zero.
//
//   Major versions (e.g. v2→v3): breaking.
//     A v2 consumer cannot read v3 messages when a field type changes.
//     flowgate routes the deserialization error to the FailureHandler.
//     Breaking changes require a new topic and a coordinated rollout.
//
// Run: go run example/compat/main.go
package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	orderv1 "github.com/aypandey/flowgate/example/order/v1_0_0"
	orderv2 "github.com/aypandey/flowgate/example/order/v2_0_0"
	orderv3 "github.com/aypandey/flowgate/example/order/v3_0_0"
	"github.com/aypandey/flowgate/pkg/consumer"
	"github.com/aypandey/flowgate/pkg/failure"
	"github.com/aypandey/flowgate/pkg/producer"
	"github.com/aypandey/flowgate/pkg/record"
	confluent "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const (
	brokers     = "localhost:9092"
	registryURL = "http://localhost:8081"
)

func main() {
	ensureTopic("compat.a") // Scenario A: v2 → v1
	ensureTopic("compat.b") // Scenario B: v1 → v2
	ensureTopic("compat.c") // Scenario C: v3 → v2 (breaking)

	banner("Scenario A — Minor: v2 producer → v1 consumer (extra field ignored)")
	scenarioA()

	banner("Scenario B — Minor: v1 producer → v2 consumer (missing field defaults to zero)")
	scenarioB()

	banner("Scenario C — Major (breaking): v3 producer → v2 consumer (type mismatch)")
	scenarioC()
}

// scenarioA: producer writes v2 messages (with discount_amount).
// Consumer uses v1.Order which has no DiscountAmount field.
// Expected: deserialization succeeds — extra field silently ignored.
func scenarioA() {
	ctx := context.Background()

	p, err := producer.NewProducer[orderv2.Order](
		producer.WithBrokers(brokers),
		producer.WithSchemaRegistry(registryURL),
		producer.WithTopic("compat.a"),
	)
	if err != nil {
		log.Fatalf("scenarioA producer: %v", err)
	}
	defer p.Close()

	outgoing := []orderv2.Order{
		{OrderID: "A-001", CustomerID: "c1", Amount: 100.0, Currency: "USD",
			Status: "PENDING", CreatedAt: now(), DiscountAmount: 10.0},
		{OrderID: "A-002", CustomerID: "c2", Amount: 200.0, Currency: "EUR",
			Status: "CONFIRMED", CreatedAt: now()},
	}
	for _, o := range outgoing {
		mustSend(p, ctx, o, o.OrderID)
	}
	mustFlush(p, ctx)
	log.Printf("  produced %d v2 messages (A-001 discount=10.00, A-002 discount=0)", len(outgoing))

	cCtx, cCancel := context.WithTimeout(ctx, 10*time.Second)
	defer cCancel()

	received := 0
	c := mustConsumer[orderv1.Order]("compat.a", cCtx)
	defer c.Close()

	_ = c.Subscribe(cCtx, func(r *record.ConsumerRecord[orderv1.Order]) error {
		received++
		log.Printf("  [v1 consumer] order=%s amount=%.2f %s  ← no DiscountAmount field in v1 struct",
			r.Payload.OrderID, r.Payload.Amount, r.Payload.Currency)
		if received >= len(outgoing) {
			cCancel()
		}
		return nil
	})

	if received == len(outgoing) {
		log.Println("  PASS — v1 consumer read v2 messages; discount_amount silently ignored")
	} else {
		log.Printf("  FAIL — expected %d messages, got %d", len(outgoing), received)
	}
}

// scenarioB: producer writes v1 messages (no discount_amount).
// Consumer uses v2.Order which has DiscountAmount float64.
// Expected: deserialization succeeds — missing field left as zero (0.0).
func scenarioB() {
	ctx := context.Background()

	p, err := producer.NewProducer[orderv1.Order](
		producer.WithBrokers(brokers),
		producer.WithSchemaRegistry(registryURL),
		producer.WithTopic("compat.b"),
	)
	if err != nil {
		log.Fatalf("scenarioB producer: %v", err)
	}
	defer p.Close()

	outgoing := []orderv1.Order{
		{OrderID: "B-001", CustomerID: "c3", Amount: 50.0, Currency: "USD",
			Status: "SHIPPED", CreatedAt: now()},
		{OrderID: "B-002", CustomerID: "c4", Amount: 75.0, Currency: "GBP",
			Status: "PENDING", CreatedAt: now()},
	}
	for _, o := range outgoing {
		mustSend(p, ctx, o, o.OrderID)
	}
	mustFlush(p, ctx)
	log.Printf("  produced %d v1 messages (no discount_amount field)", len(outgoing))

	cCtx, cCancel := context.WithTimeout(ctx, 10*time.Second)
	defer cCancel()

	received := 0
	allZeroDiscount := true
	c := mustConsumer[orderv2.Order]("compat.b", cCtx)
	defer c.Close()

	_ = c.Subscribe(cCtx, func(r *record.ConsumerRecord[orderv2.Order]) error {
		received++
		if r.Payload.DiscountAmount != 0 {
			allZeroDiscount = false
		}
		log.Printf("  [v2 consumer] order=%s amount=%.2f %s discount=%.2f  ← defaults to zero",
			r.Payload.OrderID, r.Payload.Amount, r.Payload.Currency, r.Payload.DiscountAmount)
		if received >= len(outgoing) {
			cCancel()
		}
		return nil
	})

	if received == len(outgoing) && allZeroDiscount {
		log.Println("  PASS — v2 consumer read v1 messages; missing discount_amount defaulted to 0.0")
	} else {
		log.Printf("  FAIL — received=%d allZeroDiscount=%v", received, allZeroDiscount)
	}
}

// scenarioC: producer writes v3 messages where amount is a STRING.
// Consumer uses v2.Order where Amount is float64.
// Expected: type mismatch on deserialization → FailureHandler fires, handler never called.
func scenarioC() {
	ctx := context.Background()

	p, err := producer.NewProducer[orderv3.Order](
		producer.WithBrokers(brokers),
		producer.WithSchemaRegistry(registryURL),
		producer.WithTopic("compat.c"),
	)
	if err != nil {
		log.Fatalf("scenarioC producer: %v", err)
	}
	defer p.Close()

	outgoing := []orderv3.Order{
		{OrderID: "C-001", CustomerID: "c5", Amount: "149.99", Currency: "USD",
			Status: "PENDING", CreatedAt: now()},
		{OrderID: "C-002", CustomerID: "c6", Amount: "299.00", Currency: "EUR",
			Status: "CONFIRMED", CreatedAt: now()},
	}
	for _, o := range outgoing {
		mustSend(p, ctx, o, o.OrderID)
	}
	mustFlush(p, ctx)
	log.Printf("  produced %d v3 messages (amount is STRING — breaking change)", len(outgoing))

	cCtx, cCancel := context.WithTimeout(ctx, 10*time.Second)
	defer cCancel()

	fh := &captureHandler{limit: len(outgoing), cancel: cCancel}
	handlerCalls := 0

	c, err := consumer.NewConsumer[orderv2.Order](
		consumer.WithBrokers(brokers),
		consumer.WithSchemaRegistry(registryURL),
		consumer.WithTopic("compat.c"),
		consumer.WithGroupID(fmt.Sprintf("compat-c-%d", time.Now().UnixNano())),
		consumer.WithFailureHandler(fh),
	)
	if err != nil {
		log.Fatalf("scenarioC consumer: %v", err)
	}
	defer c.Close()

	_ = c.Subscribe(cCtx, func(r *record.ConsumerRecord[orderv2.Order]) error {
		handlerCalls++ // should never reach here
		return nil
	})

	fh.mu.Lock()
	failures := len(fh.errs)
	fh.mu.Unlock()

	if failures == len(outgoing) && handlerCalls == 0 {
		log.Printf("  PASS — FailureHandler caught %d deserialization errors; handler never called", failures)
		log.Println("        → v3 messages require a new topic; v1/v2 consumers must not read from it")
	} else {
		log.Printf("  FAIL — failures=%d handlerCalls=%d (expected failures=%d handlerCalls=0)",
			failures, handlerCalls, len(outgoing))
	}
}

// captureHandler logs failures and cancels the consumer context after limit fires.
type captureHandler struct {
	mu     sync.Mutex
	errs   []string
	limit  int
	cancel context.CancelFunc
}

func (h *captureHandler) OnFailure(_ context.Context, r failure.RawRecord, err error) {
	h.mu.Lock()
	h.errs = append(h.errs, err.Error())
	count := len(h.errs)
	h.mu.Unlock()
	log.Printf("  [FailureHandler] %v", err)
	if count >= h.limit {
		h.cancel()
	}
}

// helpers

func now() string { return time.Now().UTC().Format(time.RFC3339) }

func mustSend[T any](p *producer.Producer[T], ctx context.Context, v T, key string) {
	if err := p.Send(ctx, record.RecordOf(v).WithKey(key)); err != nil {
		log.Fatalf("send: %v", err)
	}
}

func mustFlush[T any](p *producer.Producer[T], ctx context.Context) {
	if err := p.Flush(ctx); err != nil {
		log.Fatalf("flush: %v", err)
	}
}

func mustConsumer[T any](topic string, ctx context.Context) *consumer.Consumer[T] {
	c, err := consumer.NewConsumer[T](
		consumer.WithBrokers(brokers),
		consumer.WithSchemaRegistry(registryURL),
		consumer.WithTopic(topic),
		consumer.WithGroupID(fmt.Sprintf("compat-%s-%d", topic, time.Now().UnixNano())),
	)
	if err != nil {
		log.Fatalf("create consumer: %v", err)
	}
	return c
}

func ensureTopic(name string) {
	a, err := confluent.NewAdminClient(&confluent.ConfigMap{"bootstrap.servers": brokers})
	if err != nil {
		log.Fatalf("admin client: %v", err)
	}
	defer a.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	results, err := a.CreateTopics(ctx, []confluent.TopicSpecification{
		{Topic: name, NumPartitions: 1, ReplicationFactor: 1},
	})
	if err != nil {
		log.Fatalf("create topic %s: %v", name, err)
	}
	for _, r := range results {
		switch r.Error.Code() {
		case confluent.ErrNoError:
			log.Printf("topic created: %s", name)
		case confluent.ErrTopicAlreadyExists:
			log.Printf("topic exists:  %s", name)
		default:
			log.Fatalf("create topic %s: %v", name, r.Error)
		}
	}
}

func banner(msg string) {
	log.Println("──────────────────────────────────────────────────────────")
	log.Println(msg)
	log.Println("──────────────────────────────────────────────────────────")
}
