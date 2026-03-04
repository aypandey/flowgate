// Example producer — demonstrates how a team uses flowgate to send Order events to Kafka.
//
// The producer team imports the versioned order package at the schema version they want
// to publish. No WithSchemaFile is needed — the schema is embedded in the package.
//
// Run: go run example/producer/main.go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	orderv2 "github.com/aypandey/flowgate/example/order/v2_0_0"
	"github.com/aypandey/flowgate/pkg/producer"
	"github.com/aypandey/flowgate/pkg/record"
)

func main() {
	ctx := context.Background()

	// --- 1. Initialise the producer ---
	// Import orderv2 and flowgate auto-registers its embedded schema.
	// Bump to orderv3 when a new schema version ships — nothing else changes.
	p, err := producer.NewProducer[orderv2.Order](
		producer.WithBrokers("localhost:9092"),
		producer.WithSchemaRegistry("http://localhost:8081"),
		producer.WithTopic("payments.order"),
		// no WithSchemaFile — orderv2.Order implements schema.Provider
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
	defer p.Close()

	// --- 2. Send messages ---
	orders := []orderv2.Order{
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
			DiscountAmount: 29.90,
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
			WithKey(order.OrderID).
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
