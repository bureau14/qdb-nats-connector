// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package util: testing & development utilities
// Types: none
// Ex: util.ValidJsonString() → "{\"key\": \"value\"}"
package util

import (
	"encoding/json"
	"fmt"
	"math"

	"github.com/nats-io/nats.go"
	"pgregory.net/rapid"
)

// generateRandomJsonFields creates a map with random JSON-compatible fields.
// Returns a map[string]interface{} with 1-5 randomly generated fields.
func generateRandomJsonFields(t *rapid.T) map[string]interface{} {
	numFields := rapid.IntRange(1, 5).Draw(t, "numFields")
	jsonData := make(map[string]interface{})

	for i := range numFields {
		key := rapid.StringMatching(`[a-zA-Z][a-zA-Z0-9_]*`).Draw(t, fmt.Sprintf("key_%d", i))

		// Choose random valid type
		typeChoice := rapid.IntRange(0, 2).Draw(t, fmt.Sprintf("type_%d", i))
		switch typeChoice {
		case 0: // string
			jsonData[key] = rapid.String().Draw(t, fmt.Sprintf("str_val_%d", i))
		case 1: // number
			value := rapid.Float64Range(-1e10, 1e10).Draw(t, fmt.Sprintf("num_val_%d", i))
			if !math.IsNaN(value) && !math.IsInf(value, 0) {
				jsonData[key] = value
			} else {
				jsonData[key] = 42.0 // fallback for special values
			}
		case 2: // boolean
			jsonData[key] = rapid.Bool().Draw(t, fmt.Sprintf("bool_val_%d", i))
		}
	}

	return jsonData
}

// ValidJsonString generates a valid JSON string with random fields.
// Returns a JSON string with 1-5 fields containing various data types.
func ValidJsonString(t *rapid.T) string {
	jsonData := generateRandomJsonFields(t)
	jsonBytes, _ := json.Marshal(jsonData)

	return string(jsonBytes)
}

// ValidJsonMap generates a valid JSON map with random fields.
// Returns a map[string]interface{} suitable for JSON marshaling.
func ValidJsonMap(t *rapid.T) map[string]interface{} {
	return generateRandomJsonFields(t)
}

// InvalidJsonString generates invalid JSON strings for testing error handling.
func InvalidJsonString(t *rapid.T) string {
	invalidPatterns := []string{
		`{"key": invalid}`,
		`{"key": "value"`,
		`{"key": "value}`,
		`{"key": "val\x00ue"}`,
		`{"key": "value",}`,
		`{key: "value"}`,
		`{'key': 'value'}`,
		`{"key": "value", "key2":}`,
	}

	idx := rapid.IntRange(0, len(invalidPatterns)-1).Draw(t, "invalid_pattern")

	return invalidPatterns[idx]
}

// NatsMessage generates a NATS message with random subject and data.
func NatsMessage(t *rapid.T) *nats.Msg {
	return &nats.Msg{
		Subject: rapid.StringMatching(`[a-zA-Z][a-zA-Z0-9_]*`).Draw(t, "topic"),
		Data:    []byte(rapid.String().Draw(t, "data")),
	}
}

// NatsMessageWithJson generates a NATS message containing valid JSON data.
func NatsMessageWithJson(t *rapid.T) *nats.Msg {
	return &nats.Msg{
		Subject: rapid.StringMatching(`[a-zA-Z][a-zA-Z0-9_]*`).Draw(t, "topic"),
		Data:    []byte(ValidJsonString(t)),
	}
}

// NatsMessageWithInvalidJson generates a NATS message containing invalid JSON.
func NatsMessageWithInvalidJson(t *rapid.T) *nats.Msg {
	return &nats.Msg{
		Subject: rapid.StringMatching(`[a-zA-Z][a-zA-Z0-9_]*`).Draw(t, "topic"),
		Data:    []byte(InvalidJsonString(t)),
	}
}

// RandomEndpoint generates random NATS endpoints for testing.
func RandomEndpoint(t *rapid.T) string {
	endpoints := []string{
		"nats://localhost:4222",
		"nats://127.0.0.1:4222",
		"nats://test.example.com:4222",
	}

	return rapid.SampledFrom(endpoints).Draw(t, "endpoint")
}

// ComponentName generates a random component name for error testing.
func ComponentName(t *rapid.T) string {
	components := []string{
		"source", "sink", "parser", "connector",
		"json_parser", "noop_parser", "writer",
	}

	return rapid.SampledFrom(components).Draw(t, "component")
}

// UnicodeName generates a string with unicode characters for testing.
func UnicodeName(t *rapid.T) string {
	unicodeStrings := []string{
		"🚀💻🔥",
		"你好世界",
		"مرحبا بالعالم",
		"こんにちは世界",
		"Здравствуй мир",
		"Γεια σου κόσμε",
	}

	return rapid.SampledFrom(unicodeStrings).Draw(t, "unicode")
}

// SpecialCharsString generates strings with special characters for testing.
func SpecialCharsString(t *rapid.T) string {
	specialStrings := []string{
		`He said "Hello"`,
		`C:\Windows\Path`,
		"line1\nline2\nline3",
		"col1\tcol2\tcol3",
		"",
		" ",
		"\x00\x01\x02",
	}

	return rapid.SampledFrom(specialStrings).Draw(t, "special_chars")
}
