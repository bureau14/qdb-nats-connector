// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package parser: NATS subject extraction (extract_subject step)
// Types: subjectStepConfig
// Ex: makeExtractSubjectStep(config) → step copying subject data to a field
//
// Strings produced here follow the memory pinning contract (see yaml.go
// header): strings.Split/Trim substrings are contiguous and pinning-safe.
package parser

import (
	"fmt"
	"strings"

	connectorErrors "github.com/bureau14/qdb-nats-connector/internal/errors"
)

// subjectStepConfig: compile-time configuration of an extract_subject step.
type subjectStepConfig struct {
	target     string
	segment    int  // dot-separated index; negative = from the end
	hasSegment bool // absent segment = whole subject
	trim       string
}

// parseSubjectConfig validates extract_subject step config.
// In: config["target"] - output field name
//
//	config["segment"] - optional int index (negative = from end)
//	config["trim"] - optional cutset trimmed from both ends
//
// Out: subjectStepConfig - validated config
// Ex: parseSubjectConfig({"target": "stream_token", "segment": -2}) → cfg
func parseSubjectConfig(config map[string]interface{}) (subjectStepConfig, error) {
	target, ok := config["target"].(string)
	if !ok || target == "" {
		return subjectStepConfig{}, connectorErrors.NewInvalidConfigError("yaml_parser",
			"extract_subject step requires a 'target' string option")
	}

	cfg := subjectStepConfig{target: target}

	segment, present, err := parseIntOption(config, "segment")
	if err != nil {
		return subjectStepConfig{}, connectorErrors.NewInvalidConfigError("yaml_parser",
			fmt.Sprintf("extract_subject %v", err))
	}

	cfg.segment = segment
	cfg.hasSegment = present

	if raw, present := config["trim"]; present {
		trim, ok := raw.(string)
		if !ok {
			return subjectStepConfig{}, connectorErrors.NewInvalidConfigError("yaml_parser",
				"extract_subject 'trim' must be a string")
		}

		cfg.trim = trim
	}

	return cfg, nil
}

// subjectSegment returns the dot-separated subject segment at index
// (negative = from the end).
// In: subject string - full NATS subject, index int
// Out: string - selected segment
// Ex: subjectSegment("a.b.c", -2) → "b"
func subjectSegment(subject string, index int) (string, error) {
	parts := strings.Split(subject, ".")

	idx := index
	if idx < 0 {
		idx += len(parts)
	}

	if idx < 0 || idx >= len(parts) {
		return "", fmt.Errorf("segment %d out of range for subject with %d segments in extract_subject step", index, len(parts))
	}

	return parts[idx], nil
}

// makeExtractSubjectStep creates a step copying routing data from the NATS
// subject into state.Fields; no other step reads the subject. Trimming to
// an empty string is a valid result, not an error.
//
// extract_subject is NON-structural: failures (out-of-range segment, nil
// message) follow the normal per-step error flow → OutcomePartial.
//
// In: config - see parseSubjectConfig
// Out: TransformationStep - writes the (segmented, trimmed) subject
// Ex: makeExtractSubjectStep({"target": "stream_token", "segment": -2, "trim": "="}) → step
func makeExtractSubjectStep(config map[string]interface{}) (TransformationStep, error) {
	cfg, err := parseSubjectConfig(config)
	if err != nil {
		return nil, err
	}

	return func(state *ParseState) error {
		if state.Message == nil {
			return connectorErrors.NewParsingFailedError("yaml_parser",
				fmt.Errorf("nil message in extract_subject step"))
		}

		value := state.Message.Subject

		if cfg.hasSegment {
			segment, err := subjectSegment(value, cfg.segment)
			if err != nil {
				return connectorErrors.NewParsingFailedError("yaml_parser", err)
			}

			value = segment
		}

		if cfg.trim != "" {
			value = strings.Trim(value, cfg.trim)
		}

		state.Fields[cfg.target] = value

		return nil
	}, nil
}
