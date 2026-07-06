// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package parser: extract_subject step tests
// Types: -
// Ex: go test -run TestExtractSubject ./internal/parser
package parser

import (
	"errors"
	"testing"

	connectorErrors "github.com/bureau14/qdb-nats-connector/internal/errors"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// runSubjectStep compiles the step and runs it against a message with the
// given subject.
func runSubjectStep(t *testing.T, config map[string]interface{}, subject string) (*ParseState, error) {
	t.Helper()

	step, err := makeExtractSubjectStep(config)
	require.NoError(t, err)

	state := &ParseState{
		Message: &nats.Msg{Subject: subject},
		Fields:  map[string]interface{}{},
	}

	return state, step(state)
}

// TestExtractSubjectFactoryErrors validates fail-fast config handling
func TestExtractSubjectFactoryErrors(t *testing.T) {
	cases := []struct {
		name   string
		config map[string]interface{}
	}{
		{"missing target", map[string]interface{}{}},
		{"empty target", map[string]interface{}{"target": ""}},
		{"segment not an integer", map[string]interface{}{"target": "t", "segment": "x"}},
		{"segment float", map[string]interface{}{"target": "t", "segment": 1.5}},
		{"trim not a string", map[string]interface{}{"target": "t", "trim": 7}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			step, err := makeExtractSubjectStep(tc.config)
			assert.Nil(t, step)
			require.Error(t, err)

			var connErr *connectorErrors.ConnectorError
			require.True(t, errors.As(err, &connErr))
			assert.Equal(t, "yaml_parser", connErr.Component)
			assert.Equal(t, connectorErrors.ErrCodeInvalidConfig, connErr.Code)
		})
	}
}

// TestExtractSubjectWholeSubject copies the full subject when segment is
// absent
func TestExtractSubjectWholeSubject(t *testing.T) {
	state, err := runSubjectStep(t, map[string]interface{}{"target": "subject"}, "a.b.c")
	require.NoError(t, err)
	assert.Equal(t, "a.b.c", state.Fields["subject"])
}

// TestExtractSubjectSegments covers positive and negative indexing
func TestExtractSubjectSegments(t *testing.T) {
	cases := []struct {
		name    string
		segment int
		want    string
	}{
		{"first", 0, "a"},
		{"last positive", 3, "d"},
		{"last negative", -1, "d"},
		{"first negative", -4, "a"},
		{"second to last", -2, "c"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			state, err := runSubjectStep(t, map[string]interface{}{
				"target":  "token",
				"segment": tc.segment,
			}, "a.b.c.d")
			require.NoError(t, err)
			assert.Equal(t, tc.want, state.Fields["token"])
		})
	}
}

// TestExtractSubjectSegmentOutOfRange errors at the step and yields
// OutcomePartial through a pipeline (non-structural)
func TestExtractSubjectSegmentOutOfRange(t *testing.T) {
	for _, segment := range []int{4, -5} {
		_, err := runSubjectStep(t, map[string]interface{}{
			"target":  "token",
			"segment": segment,
		}, "a.b.c.d")
		requireParsingFailedError(t, err)
	}

	t.Run("partial through pipeline", func(t *testing.T) {
		config := YAMLConfig{
			Output: OutputSchema{
				Columns: []ColumnSchema{
					{Name: "token", Type: "string"},
				},
			},
			Transformations: []TransformSpec{
				{Step: "parse_json", Config: map[string]interface{}{}},
				{Step: "extract_subject", Config: map[string]interface{}{
					"target":  "token",
					"segment": 10,
				}},
				{Step: "extract_table", Config: map[string]interface{}{"value": "events"}},
			},
		}

		parser, err := NewYAMLParserFromConfig(config)
		require.NoError(t, err)

		res, err := parser.Parse(&nats.Msg{Subject: "a.b", Data: []byte(`{"x": 1}`)})
		require.NoError(t, err)
		assert.Equal(t, OutcomePartial, res.Outcome)
		require.NotEmpty(t, res.Errors)
		assert.True(t, errorsContain(res.Errors, "out of range"))
	})
}

// TestExtractSubjectTrim covers cutset trimming, including the all-padding
// edge where the result is a valid empty string
func TestExtractSubjectTrim(t *testing.T) {
	t.Run("trims padding from segment", func(t *testing.T) {
		state, err := runSubjectStep(t, map[string]interface{}{
			"target":  "token",
			"segment": -2,
			"trim":    "=",
		}, "skf.123.0.ion.streams.XWEVNPQ=.value")
		require.NoError(t, err)
		assert.Equal(t, "XWEVNPQ", state.Fields["token"])
	})

	t.Run("all-padding trims to empty string", func(t *testing.T) {
		state, err := runSubjectStep(t, map[string]interface{}{
			"target":  "token",
			"segment": 1,
			"trim":    "=",
		}, "a.===.c")
		require.NoError(t, err)
		assert.Equal(t, "", state.Fields["token"])
	})

	t.Run("trims whole subject without segment", func(t *testing.T) {
		state, err := runSubjectStep(t, map[string]interface{}{
			"target": "subject",
			"trim":   "x",
		}, "xxa.bxx")
		require.NoError(t, err)
		assert.Equal(t, "a.b", state.Fields["subject"])
	})
}

// TestExtractSubjectNilMessage pins the defensive nil-message error
func TestExtractSubjectNilMessage(t *testing.T) {
	step, err := makeExtractSubjectStep(map[string]interface{}{"target": "t"})
	require.NoError(t, err)

	state := &ParseState{Fields: map[string]interface{}{}}
	requireParsingFailedError(t, step(state))
}
