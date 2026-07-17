// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package parser: descriptor-driven protobuf decoding (parse_protobuf step)
// Types: protoStepConfig, protoDecoder
// Ex: makeParseProtobufStep(config) → step decoding state.Data via dynamicpb
//
// Schemas arrive through one of two mutually exclusive front-ends: a
// protoc-compiled FileDescriptorSet (descriptor_file) or a raw .proto
// source compiled in-process at pipeline compile time (proto_file). Both
// converge on the same *descriptorpb.FileDescriptorSet before message
// resolution, so decode behavior is identical by construction.
//
// Strings produced here follow the memory pinning contract (see yaml.go
// header): fmt.Sprintf, direct concatenation, and simple conversions only.
package parser

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/bufbuild/protocompile"
	connectorErrors "github.com/bureau14/qdb-nats-connector/internal/errors"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// protoTimestampName identifies the well-known Timestamp type by full name.
// Detection must be by name, not descriptor identity: the descriptor in play
// is the copy embedded in the operator's .desc file (via protoc
// --include_imports), not the one from timestamppb.
const protoTimestampName protoreflect.FullName = "google.protobuf.Timestamp"

// protoStepConfig: compile-time configuration of a parse_protobuf step.
type protoStepConfig struct {
	descriptorFile string // protoc-compiled FileDescriptorSet path; "" = compile protoFile
	protoFile      string // raw .proto source path; "" = load descriptorFile
	messageType    string
	source         string                                                  // original dot-path, for error messages; "" = decode state.Data
	lookupSource   func(fields map[string]interface{}) (interface{}, bool) // nil = decode state.Data
	target         string                                                  // top-level key to nest output under; "" = merge at root
}

// protoPathOption reads an optional path option: an absent key is fine, a
// present key must hold a non-empty string.
// In: config map[string]interface{} - step config
//
//	key string - option name
//
// Out: string - path ("" if absent); bool - key present and valid
// Ex: protoPathOption(config, "proto_file") → "x.proto", true, nil
func protoPathOption(config map[string]interface{}, key string) (path string, present bool, err error) {
	raw, present := config[key]
	if !present {
		return "", false, nil
	}

	path, ok := raw.(string)
	if !ok || path == "" {
		return "", false, connectorErrors.NewInvalidConfigError("yaml_parser",
			fmt.Sprintf("parse_protobuf '%s' must be a non-empty string", key))
	}

	return path, true, nil
}

// parseProtoStepConfig validates parse_protobuf step config.
// In: config["descriptor_file"] - path to a protoc-compiled FileDescriptorSet
//
//	config["proto_file"] - path to a raw .proto source, compiled in-process;
//	exactly one of descriptor_file / proto_file is required. Relative paths
//	in file-loaded configs are pre-resolved against the config directory
//	(resolveDescriptorPaths); programmatic configs are cwd-relative
//
//	config["message_type"] - fully-qualified message name to decode
//	config["source"] - optional dot-path to a []byte/string field to decode
//	config["target"] - optional top-level key to nest decoded fields under
//
// Out: protoStepConfig - validated config
// Ex: parseProtoStepConfig({"proto_file": "x.proto", "message_type": "test.v1.Envelope"}) → cfg
func parseProtoStepConfig(config map[string]interface{}) (protoStepConfig, error) {
	descriptorFile, hasDescriptor, err := protoPathOption(config, "descriptor_file")
	if err != nil {
		return protoStepConfig{}, err
	}

	protoFile, hasProto, err := protoPathOption(config, "proto_file")
	if err != nil {
		return protoStepConfig{}, err
	}

	if hasDescriptor == hasProto {
		return protoStepConfig{}, connectorErrors.NewInvalidConfigError("yaml_parser",
			"parse_protobuf step requires exactly one of 'descriptor_file' or 'proto_file'")
	}

	messageType, ok := config["message_type"].(string)
	if !ok || messageType == "" {
		return protoStepConfig{}, connectorErrors.NewInvalidConfigError("yaml_parser",
			"parse_protobuf step requires a 'message_type' string option")
	}

	cfg := protoStepConfig{descriptorFile: descriptorFile, protoFile: protoFile, messageType: messageType}

	if raw, present := config["source"]; present {
		source, ok := raw.(string)
		if !ok || source == "" {
			return protoStepConfig{}, connectorErrors.NewInvalidConfigError("yaml_parser",
				"parse_protobuf 'source' must be a non-empty string")
		}
		lookupSource, err := compileFieldPath(source)
		if err != nil {
			return protoStepConfig{}, connectorErrors.NewInvalidConfigError("yaml_parser",
				fmt.Sprintf("parse_protobuf 'source' is not a valid field path: %v", err))
		}
		cfg.source = source
		cfg.lookupSource = lookupSource
	}

	if raw, present := config["target"]; present {
		target, ok := raw.(string)
		if !ok || target == "" || strings.Contains(target, ".") {
			return protoStepConfig{}, connectorErrors.NewInvalidConfigError("yaml_parser",
				"parse_protobuf 'target' must be a non-empty top-level key without '.'")
		}
		cfg.target = target
	}

	return cfg, nil
}

// loadDescriptorSet reads a protoc-compiled FileDescriptorSet from disk.
// In: path string - descriptor file path
// Out: *descriptorpb.FileDescriptorSet - parsed descriptor set
// Ex: loadDescriptorSet("skf.desc") → fds
func loadDescriptorSet(path string) (*descriptorpb.FileDescriptorSet, error) {
	data, err := os.ReadFile(path) //nolint:gosec // path is a trusted operator-provided config file path
	if err != nil {
		return nil, connectorErrors.NewInvalidConfigError("yaml_parser",
			fmt.Sprintf("failed to read descriptor file: %v", err))
	}

	fds := &descriptorpb.FileDescriptorSet{}

	err = proto.Unmarshal(data, fds)
	if err != nil {
		return nil, connectorErrors.NewInvalidConfigError("yaml_parser",
			fmt.Sprintf("failed to parse descriptor file: %v", err))
	}

	return fds, nil
}

// appendFileDescriptors appends fd and its transitive imports to fds,
// dependencies first, deduplicated by path.
// In: fd protoreflect.FileDescriptor - compiled file
//
//	fds *descriptorpb.FileDescriptorSet - accumulator (mutated)
//	seen map[string]bool - paths already appended (mutated)
//
// Ex: appendFileDescriptors(fd, fds, seen) → fds.File = [timestamp.proto, x.proto]
func appendFileDescriptors(fd protoreflect.FileDescriptor, fds *descriptorpb.FileDescriptorSet, seen map[string]bool) {
	if seen[fd.Path()] {
		return
	}
	seen[fd.Path()] = true

	imports := fd.Imports()
	for i := range imports.Len() {
		appendFileDescriptors(imports.Get(i).FileDescriptor, fds, seen)
	}

	fds.File = append(fds.File, protodesc.ToFileDescriptorProto(fd))
}

// compileProtoFile compiles a raw .proto source into the same descriptor
// set shape loadDescriptorSet reads from disk, so everything downstream
// (registry build, message resolution, dynamicpb decode) is shared between
// the two front-ends. The file's own directory is the import root:
// same-directory imports resolve implicitly, and protoc's standard imports
// (google/protobuf/*.proto) come from protocompile.WithStandardImports -
// the in-process equivalent of protoc --include_imports. Runs once at
// pipeline compile (startup), never per message; context.Background() is
// deliberate - step factories have no context and this is a bounded local
// compile, not I/O to a remote service.
// In: path string - .proto source file path
// Out: *descriptorpb.FileDescriptorSet - compiled set, dependencies first
// Ex: compileProtoFile("idf_v1_min.proto") → fds
func compileProtoFile(path string) (*descriptorpb.FileDescriptorSet, error) {
	compiler := protocompile.Compiler{
		Resolver: protocompile.WithStandardImports(&protocompile.SourceResolver{
			ImportPaths: []string{filepath.Dir(path)},
		}),
	}

	files, err := compiler.Compile(context.Background(), filepath.Base(path))
	if err != nil {
		return nil, connectorErrors.NewInvalidConfigError("yaml_parser",
			fmt.Sprintf("failed to compile proto file '%s': %v", path, err))
	}

	fds := &descriptorpb.FileDescriptorSet{}
	appendFileDescriptors(files[0], fds, map[string]bool{})

	return fds, nil
}

// loadStepDescriptorSet materializes a step's descriptor set from its
// configured front-end: load a protoc-compiled file (descriptor_file) or
// compile raw source in-process (proto_file). parseProtoStepConfig
// guarantees exactly one is set.
// In: cfg protoStepConfig - validated step config
// Out: *descriptorpb.FileDescriptorSet - descriptor set
// Ex: loadStepDescriptorSet(cfg) → fds
func loadStepDescriptorSet(cfg protoStepConfig) (*descriptorpb.FileDescriptorSet, error) {
	if cfg.protoFile != "" {
		return compileProtoFile(cfg.protoFile)
	}

	return loadDescriptorSet(cfg.descriptorFile)
}

// formatProtoTimestamp renders a google.protobuf.Timestamp message as an
// RFC3339 UTC string with a fixed 9-digit nanosecond fraction. Never uses
// time.RFC3339Nano, which trims trailing zeros and would lose the fixed
// width needed for lossless downstream extract_index parsing.
// In: m protoreflect.Message - a google.protobuf.Timestamp message
// Out: string - e.g. "2026-07-02T11:22:33.000000001Z" (pinning-safe)
// Ex: formatProtoTimestamp(ts) → "2026-07-02T11:22:33.500000000Z"
func formatProtoTimestamp(m protoreflect.Message) string {
	fields := m.Descriptor().Fields()
	seconds := m.Get(fields.ByNumber(1)).Int()
	nanos := m.Get(fields.ByNumber(2)).Int()
	t := time.Unix(seconds, nanos).UTC()

	return t.Format("2006-01-02T15:04:05") + fmt.Sprintf(".%09dZ", t.Nanosecond())
}

// protoEmbeddedValue converts a nested message value: well-known Timestamps
// become RFC3339Nano strings, everything else a nested map addressable by
// extract_field dot-paths.
// In: m protoreflect.Message - nested message value
// Out: interface{} - string (Timestamp) or map[string]interface{}
// Ex: protoEmbeddedValue(inner) → {"reading": 1.5, "unit": "mm"}
func protoEmbeddedValue(m protoreflect.Message) interface{} {
	if m.Descriptor().FullName() == protoTimestampName {
		return formatProtoTimestamp(m)
	}

	return protoMessageToMap(m)
}

// protoSingularValue normalizes a scalar protobuf value to the types the
// downstream pipeline understands: string, int64, float64, bool, []byte,
// map[string]interface{}. Unsigned 64-bit values above MaxInt64 wrap
// negative in the int64 conversion; protobuf semantics for such values are
// deployment-specific and the wrap is deliberate, not hidden behind a lossy
// float.
// In: fd protoreflect.FieldDescriptor - field schema
//
//	v protoreflect.Value - decoded value
//
// Out: interface{} - normalized Go value
// Ex: protoSingularValue(uint32Field, v) → int64(42)
func protoSingularValue(fd protoreflect.FieldDescriptor, v protoreflect.Value) interface{} {
	switch fd.Kind() {
	case protoreflect.BoolKind:
		return v.Bool()
	case protoreflect.StringKind:
		return v.String()
	case protoreflect.BytesKind:
		return v.Bytes()
	case protoreflect.EnumKind:
		return int64(v.Enum())
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind,
		protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return v.Int()
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind,
		protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return int64(v.Uint()) //nolint:gosec // deliberate wrap above MaxInt64, documented in the doc comment
	case protoreflect.FloatKind, protoreflect.DoubleKind:
		return v.Float()
	case protoreflect.MessageKind, protoreflect.GroupKind:
		return protoEmbeddedValue(v.Message())
	default:
		return v.Interface()
	}
}

// protoListValue converts a repeated field to []interface{} of normalized
// element values.
// In: fd protoreflect.FieldDescriptor - repeated field schema
//
//	l protoreflect.List - decoded list
//
// Out: []interface{} - normalized elements
// Ex: protoListValue(tagsField, l) → ["a", "b"]
func protoListValue(fd protoreflect.FieldDescriptor, l protoreflect.List) []interface{} {
	values := make([]interface{}, l.Len())
	for i := range l.Len() {
		values[i] = protoSingularValue(fd, l.Get(i))
	}

	return values
}

// protoMapValue converts a protobuf map field to map[string]interface{}
// with stringified keys (map keys are data, not schema, and do not reach
// WriterTables directly).
// In: fd protoreflect.FieldDescriptor - map field schema
//
//	m protoreflect.Map - decoded map
//
// Out: map[string]interface{} - stringified keys, normalized values
// Ex: protoMapValue(blobsField, m) → {"3": []byte{...}}
func protoMapValue(fd protoreflect.FieldDescriptor, m protoreflect.Map) map[string]interface{} {
	values := make(map[string]interface{}, m.Len())
	m.Range(func(k protoreflect.MapKey, v protoreflect.Value) bool {
		values[k.String()] = protoSingularValue(fd.MapValue(), v)

		return true
	})

	return values
}

// protoFieldValue dispatches a decoded field value to the map, list, or
// singular converter.
// In: fd protoreflect.FieldDescriptor - field schema
//
//	v protoreflect.Value - decoded value
//
// Out: interface{} - normalized Go value
// Ex: protoFieldValue(fd, v) → int64(7)
func protoFieldValue(fd protoreflect.FieldDescriptor, v protoreflect.Value) interface{} {
	switch {
	case fd.IsMap():
		return protoMapValue(fd, v.Map())
	case fd.IsList():
		return protoListValue(fd, v.List())
	default:
		return protoSingularValue(fd, v)
	}
}

// protoMessageToMap converts a decoded message to map[string]interface{},
// visiting populated fields only (protoreflect Range semantics): unset
// proto3 scalars are absent from the result, mirroring how a JSON payload
// omits keys.
// In: m protoreflect.Message - decoded message
// Out: map[string]interface{} - field name → normalized value
// Ex: protoMessageToMap(msg) → {"name": "a", "count": int64(2)}
func protoMessageToMap(m protoreflect.Message) map[string]interface{} {
	fields := make(map[string]interface{}, m.Descriptor().Fields().Len())
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		fields[string(fd.Name())] = protoFieldValue(fd, v)

		return true
	})

	return fields
}

// protoDecoder: compile-time state of a parse_protobuf step - the resolved
// message descriptor decoded against per message.
type protoDecoder struct {
	desc protoreflect.MessageDescriptor
}

// newProtoDecoder materializes the descriptor set and resolves the message
// type.
// In: cfg protoStepConfig - validated step config
// Out: *protoDecoder - ready-to-use decoder
// Ex: newProtoDecoder(cfg) → decoder
func newProtoDecoder(cfg protoStepConfig) (*protoDecoder, error) {
	fds, err := loadStepDescriptorSet(cfg)
	if err != nil {
		return nil, err
	}

	// protodesc.NewFiles builds a standalone registry; it never touches
	// protoregistry.GlobalFiles, so the timestamp.proto copy embedded via
	// protoc --include_imports cannot conflict with globally registered
	// well-known types.
	files, err := protodesc.NewFiles(fds)
	if err != nil {
		return nil, connectorErrors.NewInvalidConfigError("yaml_parser",
			fmt.Sprintf("failed to build descriptor registry: %v", err))
	}

	desc, err := files.FindDescriptorByName(protoreflect.FullName(cfg.messageType))
	if err != nil {
		return nil, connectorErrors.NewInvalidConfigError("yaml_parser",
			fmt.Sprintf("message type '%s' not found in descriptor file: %v", cfg.messageType, err))
	}

	msgDesc, ok := desc.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, connectorErrors.NewInvalidConfigError("yaml_parser",
			fmt.Sprintf("descriptor '%s' is not a message type", cfg.messageType))
	}

	return &protoDecoder{desc: msgDesc}, nil
}

// decode unmarshals a protobuf payload and converts it to normalized fields.
// In: data []byte - raw protobuf bytes
// Out: map[string]interface{} - populated fields, normalized values
// Ex: decode(payload) → {"name": "a", "created_at": "2026-...Z"}
func (d *protoDecoder) decode(data []byte) (map[string]interface{}, error) {
	msg := dynamicpb.NewMessage(d.desc)

	err := proto.Unmarshal(data, msg)
	if err != nil {
		return nil, err
	}

	return protoMessageToMap(msg), nil
}

// resolveProtoSource selects the bytes a parse_protobuf step decodes: the
// raw payload by default, or a []byte/string field lifted from an earlier
// step (the nested re-decode pattern). Callers reject empty results: an
// all-default inner message marshals to zero bytes and is then
// indistinguishable from a missing payload - a deliberate trade-off.
// In: state *ParseState - pipeline state
//
//	cfg protoStepConfig - nil lookupSource = state.Data
//
// Out: []byte - bytes to decode
// Ex: resolveProtoSource(state, cfg) → serialized inner message
func resolveProtoSource(state *ParseState, cfg protoStepConfig) ([]byte, error) {
	if cfg.lookupSource == nil {
		return state.Data, nil
	}

	value, ok := cfg.lookupSource(state.Fields)
	if !ok {
		return nil, fmt.Errorf("source field '%s' not found in parse_protobuf step", cfg.source)
	}

	switch v := value.(type) {
	case []byte:
		return v, nil
	case string:
		return []byte(v), nil
	default:
		return nil, fmt.Errorf("parse_protobuf source '%s' has type %T, want bytes or string", cfg.source, v)
	}
}

// makeParseProtobufStep creates a descriptor-driven protobuf decode step.
//
// parse_protobuf is STRUCTURAL: a decode error yields OutcomeUnusable →
// zero tables, nil error → the worker ACKs the message and counts it in
// messagesDropped under --parse-error-action drop. This is deliberate: it
// is how malformed payloads are skipped without NACK redelivery loops.
// Decode errors only occur on malformed wire data (truncation, invalid
// tags); a wire-type MISMATCH on a known field does NOT error - protobuf
// treats it as an unknown field, so the field is simply absent from the
// output. Unknown wire fields are silently ignored (protobuf default);
// unset proto3 scalars are absent from state.Fields (populated-only
// semantics).
//
// Consequence for nested re-decodes (source set): decoding bytes of the
// WRONG message type usually SUCCEEDS with zero populated fields, because
// mismatched wire types park as unknown fields. A second-pass decode is
// therefore NOT a message-shape filter; use extract_map_entry
// 'allowed_keys' to filter on the map key instead.
//
// In: config["descriptor_file"] - protoc-compiled FileDescriptorSet path
//
//	config["proto_file"] - raw .proto source path, compiled in-process;
//	exactly one of descriptor_file / proto_file is required
//	(config-dir-relative when file-loaded, cwd-relative when programmatic)
//
//	config["message_type"] - fully-qualified message name
//	config["source"] - optional dot-path to a []byte/string field to decode
//	config["target"] - optional top-level key to nest decoded fields under
//
// Out: TransformationStep - decodes into state.Fields
// Ex: makeParseProtobufStep({"proto_file": "x.proto", "message_type": "test.v1.Envelope"}) → step
func makeParseProtobufStep(config map[string]interface{}) (TransformationStep, error) {
	cfg, err := parseProtoStepConfig(config)
	if err != nil {
		return nil, err
	}

	decoder, err := newProtoDecoder(cfg)
	if err != nil {
		return nil, err
	}

	return func(state *ParseState) error {
		if state == nil {
			return connectorErrors.NewParsingFailedError("yaml_parser", fmt.Errorf("nil state in parse_protobuf step"))
		}

		data, err := resolveProtoSource(state, cfg)
		if err != nil {
			return connectorErrors.NewParsingFailedError("yaml_parser", err)
		}

		if len(data) == 0 {
			return connectorErrors.NewParsingFailedError("yaml_parser", fmt.Errorf("empty data in parse_protobuf step"))
		}

		decoded, err := decoder.decode(data)
		if err != nil {
			return connectorErrors.NewParsingFailedError("yaml_parser", fmt.Errorf("failed to decode protobuf in parse_protobuf step: %w", err))
		}

		if cfg.target != "" {
			state.Fields[cfg.target] = decoded

			return nil
		}

		// Merge decoded fields - accumulates values for later steps
		for key, value := range decoded {
			state.Fields[key] = value
		}

		return nil
	}, nil
}
