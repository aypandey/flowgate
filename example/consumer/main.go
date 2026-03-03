// Example consumer — demonstrates how a team uses flowgate to consume Order events
// and forward them to an external destination (simulated Elasticsearch here).
// Run: go run example/consumer/main.go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aypandey/flowgate/example/model"
	"github.com/aypandey/flowgate/pkg/consumer"
	"github.com/aypandey/flowgate/pkg/record"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// graceful shutdown on SIGINT / SIGTERM
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Printf("received signal %s — shutting down", sig)
		cancel()
	}()

	// --- 1. Initialise the consumer ---
	c, err := consumer.NewConsumer[model.Order](
		consumer.WithBrokers("localhost:9092"),
		consumer.WithSchemaRegistry("http://localhost:8081"),
		consumer.WithTopic("payments.order"),
		consumer.WithGroupID("order-indexing-service"),
		consumer.WithSchemaFile("example/schemas/order/v2/order.avsc"),
		consumer.WithShutdownTimeout(10*time.Second),
		// example of raw config passthrough for power users
		consumer.WithRawConfig(map[string]interface{}{
			"max.poll.interval.ms": 300000,
			"session.timeout.ms":   45000,
		}),
	)
	if err != nil {
		log.Fatalf("failed to create consumer: %v", err)
	}
	defer c.Close()

	esClient := newElasticSearchClient("http://localhost:9200")

	// --- 2a. Single message subscription ---
	log.Println("starting single-message consumer...")
	if err := c.Subscribe(ctx, func(r *record.ConsumerRecord[model.Order]) error {
		order := r.Payload
		log.Printf("received order %s (status=%s amount=%.2f %s partition=%d offset=%d)",
			order.OrderID, order.Status, order.Amount, order.Currency,
			r.Partition, r.Offset,
		)
		// forward to Elasticsearch
		return esClient.Index("orders", order.OrderID, order)
	}); err != nil {
		if err != context.Canceled {
			log.Fatalf("consumer error: %v", err)
		}
	}
}

// --- Simulated Elasticsearch client ---
// In a real service this would be the official Elasticsearch Go client.

type elasticSearchClient struct {
	baseURL    string
	httpClient *http.Client
}

func newElasticSearchClient(baseURL string) *elasticSearchClient {
	return &elasticSearchClient{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

func (es *elasticSearchClient) Index(index, id string, doc interface{}) error {
	body, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("elasticsearch: failed to marshal document: %w", err)
	}

	url := fmt.Sprintf("%s/%s/_doc/%s", es.baseURL, index, id)
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("elasticsearch: failed to build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := es.httpClient.Do(req)
	if err != nil {
		// In production this would return an error and trigger FailureHandler.
		// Here we log and continue so the example works without a real ES instance.
		log.Printf("elasticsearch: (simulated) indexed document id=%s index=%s", id, index)
		return nil
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("elasticsearch: index failed with status %d", resp.StatusCode)
	}

	log.Printf("elasticsearch: indexed document id=%s index=%s", id, index)
	return nil
}
