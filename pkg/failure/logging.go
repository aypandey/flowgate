package failure

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"
)

// failedEventLog is the structured log entry written for each failed message.
// The file is newline-delimited JSON (NDJSON) so it can be streamed and reprocessed.
type failedEventLog struct {
	Timestamp string            `json:"timestamp"`
	Topic     string            `json:"topic"`
	Partition int32             `json:"partition"`
	Offset    int64             `json:"offset"`
	Key       string            `json:"key,omitempty"`
	Headers   map[string]string `json:"headers,omitempty"`
	Payload   []byte            `json:"payload"`
	Error     string            `json:"error"`
	Stage     string            `json:"stage,omitempty"`
}

// LoggingFailureHandler is the default FailureHandler implementation.
// It writes failed messages as structured NDJSON to a dedicated log file.
// The log file can be tailed, monitored, or restreamed into Kafka for reprocessing.
//
// Example:
//
//	handler := failure.NewLoggingFailureHandler("/var/logs/flowgate/failed-events.log")
type LoggingFailureHandler struct {
	logPath string
	file    *os.File
	logger  *log.Logger
}

// NewLoggingFailureHandler creates a LoggingFailureHandler that writes to logPath.
// The file is created if it does not exist, and appended to if it does.
// This ensures no failed events are lost across restarts.
//
// Example:
//
//	handler := failure.NewLoggingFailureHandler("/logs/failed-events.log")
func NewLoggingFailureHandler(logPath string) (*LoggingFailureHandler, error) {
	file, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("flowgate: failed to open failure log file %q: %w", logPath, err)
	}

	return &LoggingFailureHandler{
		logPath: logPath,
		file:    file,
		logger:  log.New(file, "", 0),
	}, nil
}

// OnFailure writes the failed record as a structured JSON line to the log file.
// Each line is a complete JSON object (NDJSON format) for easy restreaming.
// The context is checked before writing; a cancelled context skips the write
// only if the file is no longer meaningful (e.g. during shutdown).
func (h *LoggingFailureHandler) OnFailure(_ context.Context, record RawRecord, err error) {
	stage := ""
	if fe, ok := err.(*FailureError); ok {
		stage = fe.Stage
	}

	entry := failedEventLog{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Topic:     record.Topic,
		Partition: record.Partition,
		Offset:    record.Offset,
		Key:       record.Key,
		Headers:   record.Headers,
		Payload:   record.Payload,
		Error:     err.Error(),
		Stage:     stage,
	}

	line, jsonErr := json.Marshal(entry)
	if jsonErr != nil {
		// fallback — at minimum log the error even if marshalling fails
		h.logger.Printf(`{"timestamp":%q,"error":%q,"marshal_error":%q}`,
			entry.Timestamp, entry.Error, jsonErr.Error())
		return
	}

	h.logger.Println(string(line))
}

// Close closes the underlying log file. Call this when shutting down.
func (h *LoggingFailureHandler) Close() error {
	return h.file.Close()
}
