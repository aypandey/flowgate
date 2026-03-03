// Package model contains the shared domain structs used by the example app.
// In a real organisation, each team would define their own structs in their own service.
package model

// Order represents a payment order event.
// Avro struct tags map Go field names to Avro schema field names,
// following the same pattern as encoding/json tags.
//
// Schema version: v2 (includes discount_amount)
type Order struct {
	OrderID        string  `avro:"order_id"`
	CustomerID     string  `avro:"customer_id"`
	Amount         float64 `avro:"amount"`
	Currency       string  `avro:"currency"`
	Status         string  `avro:"status"`
	CreatedAt      string  `avro:"created_at"`
	DiscountAmount float64 `avro:"discount_amount,omitempty"`
}
