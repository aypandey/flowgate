package schema

// Provider is implemented by any type that embeds its own Avro schema.
//
// When the type parameter T of NewProducer or NewConsumer implements Provider,
// flowgate uses the embedded schema automatically — no WithSchemaFile or
// WithSchemaStruct option is needed.
//
// The pattern is: each schema version lives in its own Go package, with the
// .avsc file embedded via go:embed and exposed through this interface.
//
// Example (generated once per schema version):
//
//	//go:embed order.avsc
//	var avroSchema string
//
//	type Order struct {
//	    OrderID string `avro:"order_id"`
//	    ...
//	}
//
//	func (Order) AvroSchema() string { return avroSchema }
type Provider interface {
	AvroSchema() string
}
