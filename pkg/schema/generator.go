package schema

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

// avroSchemaField represents a field in an Avro schema JSON.
type avroSchemaField struct {
	Name    string      `json:"name"`
	Type    interface{} `json:"type"`
	Default interface{} `json:"default,omitempty"`
	Doc     string      `json:"doc,omitempty"`
}

// avroSchema represents the top-level Avro schema JSON object.
type avroSchema struct {
	Type      string            `json:"type"`
	Name      string            `json:"name"`
	Namespace string            `json:"namespace,omitempty"`
	Doc       string            `json:"doc,omitempty"`
	Fields    []avroSchemaField `json:"fields"`
}

// GenerateAvroSchema generates an Avro schema JSON string from a Go struct
// using `avro` struct tags. This powers the code-first schema approach.
//
// Supported Go types and their Avro mappings:
//   - string    → "string"
//   - bool      → "boolean"
//   - int, int32 → "int"
//   - int64     → "long"
//   - float32   → "float"
//   - float64   → "double"
//   - []byte    → "bytes"
//
// Fields tagged with `avro:"name,omitempty"` are wrapped as ["null", type]
// with a null default, making them optional in Avro.
//
// Example:
//
//	type Order struct {
//	    OrderID string  `avro:"order_id"`
//	    Amount  float64 `avro:"amount"`
//	    Note    string  `avro:"note,omitempty"`
//	}
func GenerateAvroSchema(v interface{}) (string, error) {
	t := reflect.TypeOf(v)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return "", fmt.Errorf("flowgate/schema: GenerateAvroSchema requires a struct, got %s", t.Kind())
	}

	fields, err := extractAvroFields(t)
	if err != nil {
		return "", err
	}

	s := avroSchema{
		Type:   "record",
		Name:   t.Name(),
		Fields: fields,
	}

	b, err := json.Marshal(s)
	if err != nil {
		return "", fmt.Errorf("flowgate/schema: failed to marshal generated schema: %w", err)
	}

	return string(b), nil
}

// extractAvroFields iterates struct fields and builds Avro field definitions.
func extractAvroFields(t reflect.Type) ([]avroSchemaField, error) {
	fields := make([]avroSchemaField, 0, t.NumField())

	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}

		tag := f.Tag.Get("avro")
		if tag == "-" {
			continue
		}

		name, opts := parseTag(tag)
		if name == "" {
			name = strings.ToLower(f.Name)
		}

		avroType, err := goTypeToAvro(f.Type)
		if err != nil {
			return nil, fmt.Errorf("flowgate/schema: unsupported type for field %s: %w", f.Name, err)
		}

		field := avroSchemaField{Name: name}

		if containsOpt(opts, "omitempty") {
			// optional field — union with null
			field.Type = []interface{}{"null", avroType}
			field.Default = nil
		} else {
			field.Type = avroType
		}

		fields = append(fields, field)
	}

	return fields, nil
}

// goTypeToAvro maps a Go reflect.Type to its Avro type string.
func goTypeToAvro(t reflect.Type) (string, error) {
	// dereference pointer
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	switch t.Kind() {
	case reflect.String:
		return "string", nil
	case reflect.Bool:
		return "boolean", nil
	case reflect.Int, reflect.Int32:
		return "int", nil
	case reflect.Int64:
		return "long", nil
	case reflect.Float32:
		return "float", nil
	case reflect.Float64:
		return "double", nil
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			return "bytes", nil
		}
		return "", fmt.Errorf("unsupported slice type %s (only []byte supported)", t)
	default:
		return "", fmt.Errorf("unsupported Go type %s", t.Kind())
	}
}
