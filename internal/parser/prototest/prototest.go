// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package prototest provides shared dynamicpb helpers over the synthetic
// test.v1 schema (testdata/envelope.proto) for protobuf pipeline tests.
// Importable by both internal/parser tests and test/integration; it must
// NOT import internal/parser (parser's in-package tests import this
// package, so that would be an import cycle) - descriptors are resolved
// directly via protodesc.
// Types: -
// Ex: prototest.MarshalEnvelope(t, func(m protoreflect.Message) {...})
package prototest

import (
	_ "embed"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// Fully-qualified message names in the embedded synthetic schema.
const (
	EnvelopeType = "test.v1.Envelope"
	InnerType    = "test.v1.Inner"
)

// envelopeDesc is the protoc-compiled FileDescriptorSet for
// testdata/envelope.proto. Package-level var required by go:embed;
// read-only after program start (sanctioned exception to the no-globals
// rule, like stepRegistry).
//
//go:embed testdata/envelope.desc
var envelopeDesc []byte

// WriteDescriptor writes the embedded descriptor set into dir and returns
// its path. Use it to place envelope.desc next to a generated YAML config
// so a relative descriptor_file resolves against the config directory.
func WriteDescriptor(t *testing.T, dir string) string {
	t.Helper()

	path := filepath.Join(dir, "envelope.desc")
	err := os.WriteFile(path, envelopeDesc, 0o600)
	if err != nil {
		t.Fatalf("prototest: write descriptor: %v", err)
	}

	return path
}

// newMessage builds an empty dynamic message for a type resolved from the
// embedded descriptor set.
func newMessage(t *testing.T, messageType string) *dynamicpb.Message {
	t.Helper()

	var set descriptorpb.FileDescriptorSet
	err := proto.Unmarshal(envelopeDesc, &set)
	if err != nil {
		t.Fatalf("prototest: unmarshal descriptor set: %v", err)
	}

	files, err := protodesc.NewFiles(&set)
	if err != nil {
		t.Fatalf("prototest: build descriptor files: %v", err)
	}

	desc, err := files.FindDescriptorByName(protoreflect.FullName(messageType))
	if err != nil {
		t.Fatalf("prototest: find %s: %v", messageType, err)
	}

	md, ok := desc.(protoreflect.MessageDescriptor)
	if !ok {
		t.Fatalf("prototest: %s is not a message", messageType)
	}

	return dynamicpb.NewMessage(md)
}

// NewEnvelope returns an empty dynamic test.v1.Envelope message.
func NewEnvelope(t *testing.T) *dynamicpb.Message {
	t.Helper()

	return newMessage(t, EnvelopeType)
}

// MarshalEnvelope builds a test payload via dynamicpb round-trip: set
// populates the message through protoreflect, then it is marshalled.
func MarshalEnvelope(t *testing.T, set func(m protoreflect.Message)) []byte {
	t.Helper()

	m := NewEnvelope(t)
	set(m)

	data, err := proto.Marshal(m)
	if err != nil {
		t.Fatalf("prototest: marshal envelope: %v", err)
	}

	return data
}

// MarshalInner builds a serialized test.v1.Inner payload - the synthetic
// stand-in for a nested attribute message carried as map value bytes.
func MarshalInner(t *testing.T, reading float64, unit string) []byte {
	t.Helper()

	m := newMessage(t, InnerType)
	fields := m.Descriptor().Fields()
	m.Set(fields.ByName("reading"), protoreflect.ValueOfFloat64(reading))
	m.Set(fields.ByName("unit"), protoreflect.ValueOfString(unit))

	data, err := proto.Marshal(m)
	if err != nil {
		t.Fatalf("prototest: marshal inner: %v", err)
	}

	return data
}

// SetStringField sets a string field by name.
func SetStringField(m protoreflect.Message, name, value string) {
	m.Set(m.Descriptor().Fields().ByName(protoreflect.Name(name)), protoreflect.ValueOfString(value))
}

// SetTimestampField populates a google.protobuf.Timestamp field by name.
func SetTimestampField(m protoreflect.Message, name string, seconds int64, nanos int32) {
	fd := m.Descriptor().Fields().ByName(protoreflect.Name(name))
	ts := m.Mutable(fd).Message()
	tsFields := ts.Descriptor().Fields()
	ts.Set(tsFields.ByNumber(1), protoreflect.ValueOfInt64(seconds))
	ts.Set(tsFields.ByNumber(2), protoreflect.ValueOfInt32(nanos))
}

// SetBlobsEntry stores raw bytes under an int32 key in Envelope.blobs.
func SetBlobsEntry(m protoreflect.Message, key int32, value []byte) {
	fd := m.Descriptor().Fields().ByName("blobs")
	m.Mutable(fd).Map().Set(protoreflect.ValueOfInt32(key).MapKey(), protoreflect.ValueOfBytes(value))
}
