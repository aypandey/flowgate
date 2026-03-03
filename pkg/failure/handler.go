// Package failure defines the FailureHandler interface and built-in implementations.
//
// flowgate's philosophy on failures: no event should be lost unless explicitly told so.
// Every failed message — whether due to schema validation, network error, or handler panic —
// is passed to a FailureHandler. The default handler logs to a structured file for restreaming.
// Teams can plug in their own implementation or opt into NoOpFailureHandler to allow loss.
package failure

import "fmt"

// RawRecord holds the raw bytes of a failed message along with its metadata.
// Used when deserialization itself fails and we cannot produce a typed record.
type RawRecord struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       string
	Headers   map[string]string
	Payload   []byte // raw bytes, not deserialized
}

// FailureHandler is the interface teams implement to handle failed messages.
// flowgate calls OnFailure when:
//   - Schema validation fails on producer side
//   - All producer retries are exhausted
//   - Consumer handler returns an error
//   - Deserialization fails on consumer side
//
// Any struct with an OnFailure method satisfies this interface — no imports needed.
//
// Example custom implementation:
//
//	type MyDLQHandler struct { client *sqs.Client }
//	func (h *MyDLQHandler) OnFailure(r failure.RawRecord, err error) {
//	    h.client.SendMessage(r.Payload)
//	}
type FailureHandler interface {
	OnFailure(record RawRecord, err error)
}

// FailureError wraps the original error with additional context
// about where in the pipeline the failure occurred.
type FailureError struct {
	Stage   string // "validation", "serialization", "produce", "consume", "handler"
	Cause   error
}

func (e *FailureError) Error() string {
	return fmt.Sprintf("flowgate [%s] failure: %v", e.Stage, e.Cause)
}

func (e *FailureError) Unwrap() error {
	return e.Cause
}

// NewFailureError creates a FailureError with the given stage and cause.
func NewFailureError(stage string, cause error) *FailureError {
	return &FailureError{Stage: stage, Cause: cause}
}
