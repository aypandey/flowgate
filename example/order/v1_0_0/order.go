// Package order contains the Order event schema for version 1.0.0.
//
// Teams consume this package by importing it at the desired version:
//
//	import orderv1 "github.com/aypandey/flowgate/example/order/v1_0_0"
//
//	c, _ := consumer.NewConsumer[orderv1.Order](
//	    consumer.WithBrokers("localhost:9092"),
//	    consumer.WithSchemaRegistry("http://localhost:8081"),
//	    consumer.WithTopic("payments.order"),
//	    consumer.WithGroupID("my-service"),
//	    // no WithSchemaFile — schema is embedded in this package
//	)
package order

import _ "embed"

//go:embed order.avsc
var avroSchema string

// Order represents a payment order event at schema version 1.0.0.
// It contains the six core fields present since the initial release.
type Order struct {
	OrderID    string  `avro:"order_id"`
	CustomerID string  `avro:"customer_id"`
	Amount     float64 `avro:"amount"`
	Currency   string  `avro:"currency"`
	Status     string  `avro:"status"`
	CreatedAt  string  `avro:"created_at"`
}

// Version is the semantic version of this schema package.
const Version = "1.0.0"

// AvroSchema returns the Avro schema embedded in this package.
// flowgate uses this automatically — teams do not call this method directly.
func (Order) AvroSchema() string { return avroSchema }
