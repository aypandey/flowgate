package failure

// NoOpFailureHandler is a FailureHandler that silently discards failed messages.
// Use this only when message loss is explicitly acceptable — for example,
// high-frequency metrics or telemetry where occasional loss is tolerable.
//
// This is an intentional, explicit opt-in to allow loss.
// flowgate's default is always LoggingFailureHandler.
//
// Example:
//
//	producer, err := producer.NewProducer(
//	    producer.WithFailureHandler(failure.NoOp),
//	)
type noOpFailureHandler struct{}

// NoOp is the singleton NoOpFailureHandler instance.
// Use failure.NoOp directly rather than constructing a new instance.
var NoOp FailureHandler = &noOpFailureHandler{}

// OnFailure does nothing. The record is silently discarded.
func (h *noOpFailureHandler) OnFailure(_ RawRecord, _ error) {}
