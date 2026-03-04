// Package order contains the Order event schema for version 3.0.0.
//
// v3.0.0 is a MAJOR (breaking) version. The Amount field type changed from
// float64 (Avro double) to string. A v1 or v2 consumer cannot deserialize
// v3 messages — the type mismatch causes a runtime deserialization error
// that flowgate routes to the FailureHandler.
//
// Breaking changes require a new topic and a coordinated producer/consumer
// rollout. Teams still on v1/v2 must not consume from the v3 topic.
package order

import _ "embed"

//go:embed order.avsc
var avroSchema string

// Order represents a payment order event at schema version 3.0.0.
// Amount is now a pre-formatted string — INCOMPATIBLE with v1/v2 consumers.
type Order struct {
	OrderID    string `avro:"order_id"`
	CustomerID string `avro:"customer_id"`
	Amount     string `avro:"amount"` // BREAKING: was float64, now string
	Currency   string `avro:"currency"`
	Status     string `avro:"status"`
	CreatedAt  string `avro:"created_at"`
}

// Version is the semantic version of this schema package.
const Version = "3.0.0"

// AvroSchema returns the Avro schema embedded in this package.
func (Order) AvroSchema() string { return avroSchema }
