// Package order contains the Order event schema for version 2.0.0.
//
// v2.0.0 adds the optional discount_amount field (Avro default: 0.0),
// which makes it fully backward compatible — a v1 consumer reading a v2
// message simply ignores the extra field.
//
// Teams consume this package by importing it at the desired version:
//
//	import orderv2 "github.com/aypandey/flowgate/example/order/v2_0_0"
//
//	p, _ := producer.NewProducer[orderv2.Order](
//	    producer.WithBrokers("localhost:9092"),
//	    producer.WithSchemaRegistry("http://localhost:8081"),
//	    producer.WithTopic("payments.order"),
//	    // no WithSchemaFile — schema is embedded in this package
//	)
package order

import _ "embed"

//go:embed order.avsc
var avroSchema string

// Order represents a payment order event at schema version 2.0.0.
// It extends v1.0.0 with the optional DiscountAmount field.
type Order struct {
	OrderID        string  `avro:"order_id"`
	CustomerID     string  `avro:"customer_id"`
	Amount         float64 `avro:"amount"`
	Currency       string  `avro:"currency"`
	Status         string  `avro:"status"`
	CreatedAt      string  `avro:"created_at"`
	DiscountAmount float64 `avro:"discount_amount,omitempty"`
}

// Version is the semantic version of this schema package.
const Version = "2.0.0"

// AvroSchema returns the Avro schema embedded in this package.
// flowgate uses this automatically — teams do not call this method directly.
func (Order) AvroSchema() string { return avroSchema }
