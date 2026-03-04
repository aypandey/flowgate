// Package schema handles all Schema Registry interactions for flowgate.
// It is responsible for registering schemas, fetching schema IDs,
// validating compatibility, and providing Avro codecs for serialization.
package schema

import (
	"fmt"
	"os"
	"sync"

	"github.com/linkedin/goavro/v2"
	"github.com/riferrei/srclient"
)

// CompatibilityMode is the Schema Registry subject-level compatibility setting.
// Re-exported from srclient so callers do not need to import srclient directly.
type CompatibilityMode = srclient.CompatibilityLevel

// Compatibility mode constants for use with WithSchemaCompatibility.
// Minor/patch upgrades (add optional field with default) → CompatibilityBackward.
// Major upgrades (breaking type change, field removal) → new topic required.
const (
	CompatibilityNone               CompatibilityMode = srclient.None
	CompatibilityBackward           CompatibilityMode = srclient.Backward
	CompatibilityBackwardTransitive CompatibilityMode = srclient.BackwardTransitive
	CompatibilityForward            CompatibilityMode = srclient.Forward
	CompatibilityForwardTransitive  CompatibilityMode = srclient.ForwardTransitive
	CompatibilityFull               CompatibilityMode = srclient.Full
	CompatibilityFullTransitive     CompatibilityMode = srclient.FullTransitive
)

// Registry wraps the Confluent Schema Registry client.
// It caches schema IDs and codecs to avoid redundant network calls
// on every message send/receive.
type Registry struct {
	client    *srclient.SchemaRegistryClient
	mu        sync.RWMutex
	codecCache map[int]*goavro.Codec   // schemaID -> codec
	idCache    map[string]int          // subject -> schemaID
}

// NewRegistry creates a new Registry connected to the given Schema Registry URL.
//
// Example:
//
//	registry, err := schema.NewRegistry("http://localhost:8081")
func NewRegistry(registryURL string) (*Registry, error) {
	client := srclient.CreateSchemaRegistryClient(registryURL)
	return &Registry{
		client:     client,
		codecCache: make(map[int]*goavro.Codec),
		idCache:    make(map[string]int),
	}, nil
}

// RegisterFromFile reads an Avro schema from a .avsc file and registers it
// under the given subject in the Schema Registry.
// If the schema already exists and is compatible, the existing ID is returned.
// If the schema is incompatible with the previous version, an error is returned.
//
// Subject naming convention: <topic>-value (e.g., "payments.order-value")
func (r *Registry) RegisterFromFile(subject, avscPath string) (int, error) {
	schemaBytes, err := os.ReadFile(avscPath)
	if err != nil {
		return 0, fmt.Errorf("flowgate/schema: failed to read schema file %q: %w", avscPath, err)
	}
	return r.RegisterFromString(subject, string(schemaBytes))
}

// RegisterFromString registers an Avro schema string under the given subject.
func (r *Registry) RegisterFromString(subject, schemaStr string) (int, error) {
	// validate the schema is valid Avro before attempting registration
	codec, err := goavro.NewCodec(schemaStr)
	if err != nil {
		return 0, fmt.Errorf("flowgate/schema: invalid Avro schema: %w", err)
	}

	registeredSchema, err := r.client.CreateSchema(subject, schemaStr, srclient.Avro)
	if err != nil {
		return 0, fmt.Errorf("flowgate/schema: failed to register schema for subject %q: %w", subject, err)
	}

	schemaID := registeredSchema.ID()

	// cache the codec and ID
	r.mu.Lock()
	r.codecCache[schemaID] = codec
	r.idCache[subject] = schemaID
	r.mu.Unlock()

	return schemaID, nil
}

// GetCodecByID fetches and caches the Avro codec for a given schema ID.
// Used by the consumer to deserialize messages using the writer's schema.
func (r *Registry) GetCodecByID(schemaID int) (*goavro.Codec, error) {
	// check cache first
	r.mu.RLock()
	if codec, ok := r.codecCache[schemaID]; ok {
		r.mu.RUnlock()
		return codec, nil
	}
	r.mu.RUnlock()

	// fetch from registry
	schema, err := r.client.GetSchema(schemaID)
	if err != nil {
		return nil, fmt.Errorf("flowgate/schema: failed to fetch schema ID %d: %w", schemaID, err)
	}

	codec, err := goavro.NewCodec(schema.Schema())
	if err != nil {
		return nil, fmt.Errorf("flowgate/schema: invalid Avro schema for ID %d: %w", schemaID, err)
	}

	r.mu.Lock()
	r.codecCache[schemaID] = codec
	r.mu.Unlock()

	return codec, nil
}

// GetSchemaID returns the cached schema ID for a subject.
// Returns an error if the subject has not been registered yet.
func (r *Registry) GetSchemaID(subject string) (int, error) {
	r.mu.RLock()
	id, ok := r.idCache[subject]
	r.mu.RUnlock()

	if !ok {
		return 0, fmt.Errorf("flowgate/schema: schema not registered for subject %q — call Register first", subject)
	}
	return id, nil
}

// SetCompatibility sets the subject-level compatibility mode in the Schema Registry.
// Call this (via WithSchemaCompatibility) before registering a new schema version
// when the subject's global default is not appropriate.
//
// Minor versions (add optional field with default): CompatibilityBackward
// Major versions (breaking change): requires a new topic — use CompatibilityNone
// on the new subject only.
func (r *Registry) SetCompatibility(subject string, mode CompatibilityMode) error {
	if _, err := r.client.ChangeSubjectCompatibilityLevel(subject, mode); err != nil {
		return fmt.Errorf("flowgate/schema: failed to set compatibility %q for subject %q: %w",
			mode, subject, err)
	}
	return nil
}

// SubjectName returns the Schema Registry subject name for a given topic.
// Follows the Confluent naming convention: <topic>-value
func SubjectName(topic string) string {
	return topic + "-value"
}
