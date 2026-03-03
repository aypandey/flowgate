package schema

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/linkedin/goavro/v2"
)

const (
	// magicByte is the Confluent wire format magic byte (always 0x0)
	magicByte = byte(0)
	// wireFormatHeaderSize = 1 (magic) + 4 (schema ID) = 5 bytes
	wireFormatHeaderSize = 5
)

// Serialize encodes a native Go map into Confluent wire format Avro bytes.
// Wire format: [0x0 magic][4-byte schema ID][avro binary payload]
//
// The native map must match the Avro schema structure.
// flowgate's producer builds this map from the team's domain struct.
func Serialize(codec *goavro.Codec, schemaID int, native map[string]interface{}) ([]byte, error) {
	// encode to Avro binary
	avroBytes, err := codec.BinaryFromNative(nil, native)
	if err != nil {
		return nil, fmt.Errorf("flowgate/schema: avro serialization failed: %w", err)
	}

	// prepend confluent wire format header
	var buf bytes.Buffer
	buf.WriteByte(magicByte)

	schemaIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(schemaIDBytes, uint32(schemaID))
	buf.Write(schemaIDBytes)
	buf.Write(avroBytes)

	return buf.Bytes(), nil
}

// Deserialize decodes Confluent wire format Avro bytes into a native Go map.
// Returns the schema ID extracted from the wire header and the decoded native map.
//
// The schema ID is used by the consumer to fetch the writer's schema from the registry,
// enabling reader/writer schema resolution for safe rolling upgrades.
func Deserialize(data []byte) (schemaID int, native map[string]interface{}, err error) {
	if len(data) < wireFormatHeaderSize {
		return 0, nil, fmt.Errorf(
			"flowgate/schema: message too short (%d bytes) — not confluent wire format", len(data),
		)
	}

	if data[0] != magicByte {
		return 0, nil, fmt.Errorf(
			"flowgate/schema: invalid magic byte 0x%X — expected 0x0 (confluent wire format)", data[0],
		)
	}

	schemaID = int(binary.BigEndian.Uint32(data[1:5]))
	return schemaID, nil, nil // raw payload decoded separately via DecodeWithCodec
}

// DecodeWithCodec decodes the Avro payload (without wire header) using the given codec.
// Used after Deserialize extracts the schema ID and the caller fetches the correct codec.
func DecodeWithCodec(codec *goavro.Codec, data []byte) (map[string]interface{}, error) {
	if len(data) < wireFormatHeaderSize {
		return nil, fmt.Errorf("flowgate/schema: data too short to decode")
	}

	avroPayload := data[wireFormatHeaderSize:]

	native, _, err := codec.NativeFromBinary(avroPayload)
	if err != nil {
		return nil, fmt.Errorf("flowgate/schema: avro deserialization failed: %w", err)
	}

	result, ok := native.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("flowgate/schema: decoded value is not a map")
	}

	return result, nil
}

// ExtractSchemaID reads just the schema ID from the wire format header
// without fully deserializing the payload. Useful for quick schema version checks.
func ExtractSchemaID(data []byte) (int, error) {
	if len(data) < wireFormatHeaderSize {
		return 0, fmt.Errorf("flowgate/schema: data too short to extract schema ID")
	}
	if data[0] != magicByte {
		return 0, fmt.Errorf("flowgate/schema: invalid magic byte")
	}
	return int(binary.BigEndian.Uint32(data[1:5])), nil
}
