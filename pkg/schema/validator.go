package schema

import (
	"fmt"
	"reflect"
	"strings"
)

// StructToNativeMap converts a Go struct with `avro` struct tags into a
// native map[string]interface{} suitable for Avro serialization.
//
// Struct tags follow the same pattern as encoding/json:
//
//	type Order struct {
//	    OrderID string  `avro:"order_id"`
//	    Amount  float64 `avro:"amount"`
//	    Status  string  `avro:"status,omitempty"`
//	}
//
// Supported tag options:
//   - `avro:"field_name"` — maps struct field to Avro field name
//   - `avro:"field_name,omitempty"` — omits field if zero value
//   - `avro:"-"` — skips this field entirely
func StructToNativeMap(v interface{}) (map[string]interface{}, error) {
	val := reflect.ValueOf(v)

	// dereference pointer if needed
	if val.Kind() == reflect.Ptr {
		if val.IsNil() {
			return nil, fmt.Errorf("flowgate/schema: cannot convert nil pointer to native map")
		}
		val = val.Elem()
	}

	if val.Kind() != reflect.Struct {
		return nil, fmt.Errorf("flowgate/schema: expected struct, got %s", val.Kind())
	}

	result := make(map[string]interface{})
	t := val.Type()

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		fieldVal := val.Field(i)

		// skip unexported fields
		if !field.IsExported() {
			continue
		}

		tag := field.Tag.Get("avro")

		// skip explicitly ignored fields
		if tag == "-" {
			continue
		}

		// parse tag name and options
		name, opts := parseTag(tag)
		if name == "" {
			// no avro tag — use lowercase field name as default
			name = strings.ToLower(field.Name)
		}

		// handle omitempty
		if containsOpt(opts, "omitempty") && isZero(fieldVal) {
			continue
		}

		result[name] = fieldVal.Interface()
	}

	return result, nil
}

// NativeMapToStruct converts a native map[string]interface{} decoded from Avro
// back into the team's domain struct using `avro` struct tags.
func NativeMapToStruct(native map[string]interface{}, target interface{}) error {
	val := reflect.ValueOf(target)

	if val.Kind() != reflect.Ptr || val.IsNil() {
		return fmt.Errorf("flowgate/schema: target must be a non-nil pointer to a struct")
	}

	val = val.Elem()
	if val.Kind() != reflect.Struct {
		return fmt.Errorf("flowgate/schema: target must point to a struct")
	}

	t := val.Type()

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		fieldVal := val.Field(i)

		if !field.IsExported() {
			continue
		}

		tag := field.Tag.Get("avro")
		if tag == "-" {
			continue
		}

		name, _ := parseTag(tag)
		if name == "" {
			name = strings.ToLower(field.Name)
		}

		mapVal, ok := native[name]
		if !ok {
			continue // field not in map — leave as zero value
		}

		if mapVal == nil {
			continue
		}

		srcVal := reflect.ValueOf(mapVal)

		// handle type conversions common in Avro decoding
		if srcVal.Type().ConvertibleTo(fieldVal.Type()) {
			fieldVal.Set(srcVal.Convert(fieldVal.Type()))
		} else {
			return fmt.Errorf(
				"flowgate/schema: cannot assign %T to field %s (%s)",
				mapVal, field.Name, fieldVal.Type(),
			)
		}
	}

	return nil
}

// parseTag splits an avro struct tag into name and options.
// e.g. "order_id,omitempty" -> ("order_id", ["omitempty"])
func parseTag(tag string) (string, []string) {
	parts := strings.Split(tag, ",")
	if len(parts) == 0 {
		return "", nil
	}
	return parts[0], parts[1:]
}

// containsOpt checks if a specific option is present in the tag options list.
func containsOpt(opts []string, opt string) bool {
	for _, o := range opts {
		if o == opt {
			return true
		}
	}
	return false
}

// isZero reports whether a value is the zero value for its type.
func isZero(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.String:
		return v.Len() == 0
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Ptr, reflect.Interface, reflect.Slice, reflect.Map, reflect.Chan, reflect.Func:
		return v.IsNil()
	default:
		return false
	}
}
