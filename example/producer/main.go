// Example producer — demonstrates how a team uses flowgate to send Order events to Kafka.
// Run: go run example/producer/main.go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/aypandey/flowgate/example/model"
	"github.com/aypandey/flowgate/pkg/producer"
	"github.com/aypandey/flowgate/pkg/record"
)

func main() {
	ctx := context.Background()

	// --- 1. Initialise the producer ---
	p, err := producer.NewProducer[model.Order](
		producer.WithBrokers("localhost:9092"),
		producer.WithSchemaRegistry("http://localhost:8081"),
		producer.WithTopic("payments.order"),
		producer.WithSchemaFile("example/schemas/order/v2/order.avsc"),
		producer.WithBufferConfig(producer.BufferConfig{
			Size:          50,
			FlushInterval: 500 * time.Millisecond,
		}),
		producer.WithRetryConfig(producer.RetryConfig{
			MaxAttempts:       3,
			InitialBackoff:    100 * time.Millisecond,
			MaxBackoff:        1 * time.Second,
			BackoffMultiplier: 2.0,
		}),
		producer.WithShutdownTimeout(10*time.Second),
	)
	if err != nil {
		log.Fatalf("failed to create producer: %v", err)
	}
	defer p.Close() // flushes buffer before closing

	// --- 2. Send messages ---
	orders := []model.Order{
		{
			OrderID:    "ord-001",
			CustomerID: "cust-abc",
			Amount:     149.99,
			Currency:   "USD",
			Status:     "PENDING",
			CreatedAt:  time.Now().UTC().Format(time.RFC3339),
		},
		{
			OrderID:        "ord-002",
			CustomerID:     "cust-xyz",
			Amount:         299.00,
			Currency:       "EUR",
			Status:         "CONFIRMED",
			CreatedAt:      time.Now().UTC().Format(time.RFC3339),
			DiscountAmount: 29.90, // v2 field
		},
		{
			OrderID:    "ord-003",
			CustomerID: "cust-def",
			Amount:     59.50,
			Currency:   "USD",
			Status:     "SHIPPED",
			CreatedAt:  time.Now().UTC().Format(time.RFC3339),
		},
	}

	for _, order := range orders {
		r := record.RecordOf(order).
			WithKey(order.OrderID). // route by order ID for partition ordering
			WithHeader("source-service", "payments-api").
			WithHeader("correlation-id", fmt.Sprintf("corr-%s", order.OrderID))

		if err := p.Send(ctx, r); err != nil {
			log.Printf("failed to send order %s: %v", order.OrderID, err)
			continue
		}
		log.Printf("queued order %s (amount=%.2f %s)", order.OrderID, order.Amount, order.Currency)
	}

	// --- 3. Force flush to ensure all messages are delivered before exit ---
	if err := p.Flush(ctx); err != nil {
		log.Printf("flush error: %v", err)
	}

	log.Println("all orders sent successfully")
}
