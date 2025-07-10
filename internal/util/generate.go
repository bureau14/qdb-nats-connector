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

// generateRandomJsonFields creates 1-5 random JSON fields.
// In: t *rapid.T - test randomizer
// Out: map[string]any - string/float64/bool fields
// Ex: generateRandomJsonFields(t) → {"key1":"val","key2":42.0}
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

// ValidJsonString generates valid JSON with 1-5 random fields.
// In: t *rapid.T - test randomizer
// Out: string - marshaled JSON object
// Ex: ValidJsonString(t) → "{\"key\":\"value\"}"
func ValidJsonString(t *rapid.T) string {
	jsonData := generateRandomJsonFields(t)
	jsonBytes, _ := json.Marshal(jsonData)

	return string(jsonBytes)
}

// ValidJsonMap generates random JSON-compatible map.
// In: t *rapid.T - test randomizer
// Out: map[string]any - 1-5 fields
// Ex: ValidJsonMap(t) → {"field1":true,"field2":123}
func ValidJsonMap(t *rapid.T) map[string]interface{} {
	return generateRandomJsonFields(t)
}

// InvalidJsonString picks malformed JSON for error tests.
// In: t *rapid.T - test randomizer
// Out: string - syntax error JSON
// Ex: InvalidJsonString(t) → "{\"key\": invalid}"
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

// NatsMessage generates random NATS message.
// In: t *rapid.T - test randomizer
// Out: *nats.Msg - random subject & data
// Ex: NatsMessage(t) → &Msg{Subject:"topic1",Data:[]byte("data")}
func NatsMessage(t *rapid.T) *nats.Msg {
	return &nats.Msg{
		Subject: rapid.StringMatching(`[a-zA-Z][a-zA-Z0-9_]*`).Draw(t, "topic"),
		Data:    []byte(rapid.String().Draw(t, "data")),
	}
}

// NatsMessageWithJson creates NATS message with valid JSON.
// In: t *rapid.T - test randomizer
// Out: *nats.Msg - random subject, JSON data
// Ex: NatsMessageWithJson(t) → &Msg{Data:[]byte("{\"k\":\"v\"}")}
func NatsMessageWithJson(t *rapid.T) *nats.Msg {
	return &nats.Msg{
		Subject: rapid.StringMatching(`[a-zA-Z][a-zA-Z0-9_]*`).Draw(t, "topic"),
		Data:    []byte(ValidJsonString(t)),
	}
}

// NatsMessageWithInvalidJson creates NATS message with bad JSON.
// In: t *rapid.T - test randomizer
// Out: *nats.Msg - random subject, invalid JSON
// Ex: NatsMessageWithInvalidJson(t) → &Msg{Data:[]byte("{key:}")}
func NatsMessageWithInvalidJson(t *rapid.T) *nats.Msg {
	return &nats.Msg{
		Subject: rapid.StringMatching(`[a-zA-Z][a-zA-Z0-9_]*`).Draw(t, "topic"),
		Data:    []byte(InvalidJsonString(t)),
	}
}

// RandomEndpoint picks test NATS endpoint.
// In: t *rapid.T - test randomizer
// Out: string - nats://host:4222 format
// Ex: RandomEndpoint(t) → "nats://localhost:4222"
func RandomEndpoint(t *rapid.T) string {
	endpoints := []string{
		"nats://localhost:4222",
		"nats://127.0.0.1:4222",
		"nats://test.example.com:4222",
	}

	return rapid.SampledFrom(endpoints).Draw(t, "endpoint")
}

// ComponentName picks random component for error tests.
// In: t *rapid.T - test randomizer
// Out: string - source/sink/parser/etc
// Ex: ComponentName(t) → "json_parser"
func ComponentName(t *rapid.T) string {
	components := []string{
		"source", "sink", "parser", "connector",
		"json_parser", "noop_parser", "writer",
	}

	return rapid.SampledFrom(components).Draw(t, "component")
}

// UnicodeName picks unicode test string.
// In: t *rapid.T - test randomizer
// Out: string - emoji/CJK/Arabic/etc
// Ex: UnicodeName(t) → "🚀💻🔥"
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

// SpecialCharsString picks string with special chars.
// In: t *rapid.T - test randomizer
// Out: string - quotes/newlines/nulls/etc
// Ex: SpecialCharsString(t) → "line1\nline2"
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
