// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package parser: single map-entry lifting (extract_map_entry step)
// Types: mapEntryStepConfig
// Ex: makeExtractMapEntryStep(config) → step lifting one map entry to named fields
//
// Strings produced here follow the memory pinning contract (see yaml.go
// header): fmt.Sprintf, direct concatenation, and simple conversions only.
package parser

import (
	"fmt"
	"log/slog"
	"sort"
	"strconv"

	connectorErrors "github.com/bureau14/qdb-nats-connector/internal/errors"
)

// mapEntryStepConfig: compile-time configuration of an extract_map_entry step.
type mapEntryStepConfig struct {
	source      string          // dot-path to a map[string]interface{}
	keyTarget   string          // field receiving the (coerced) entry key
	valueTarget string          // field receiving the entry value
	onMultiple  string          // "first" | "fail"; omitted = "fail"
	allowedKeys map[string]bool // nil = no filtering
}

// parseAllowedKeys builds the allowed-key set from a config list. YAML
// integer literals arrive as int (yaml.v3), quoted keys as string; both
// stringify to match the stringified protobuf map keys.
// In: raw interface{} - config["allowed_keys"] value
// Out: map[string]bool - stringified key set
// Ex: parseAllowedKeys([]interface{}{3, "7"}) → {"3": true, "7": true}
func parseAllowedKeys(raw interface{}) (map[string]bool, error) {
	list, ok := raw.([]interface{})
	if !ok {
		return nil, fmt.Errorf("'allowed_keys' must be a list")
	}

	if len(list) == 0 {
		return nil, fmt.Errorf("'allowed_keys' must not be empty: an empty list would drop every message")
	}

	keys := make(map[string]bool, len(list))

	for _, element := range list {
		switch v := element.(type) {
		case string:
			keys[v] = true
		case int:
			keys[strconv.Itoa(v)] = true
		case int64:
			keys[strconv.FormatInt(v, 10)] = true
		case uint64:
			keys[strconv.FormatUint(v, 10)] = true
		default:
			return nil, fmt.Errorf("'allowed_keys' element has unsupported type %T", element)
		}
	}

	return keys, nil
}

// parseMapEntryOptions validates the optional on_multiple/allowed_keys keys.
// In: config map - raw step config
//
//	cfg *mapEntryStepConfig - config being built (defaults pre-set)
//
// Out: error - InvalidConfig on malformed options
// Ex: parseMapEntryOptions({"on_multiple": "first"}, &cfg) → nil
func parseMapEntryOptions(config map[string]interface{}, cfg *mapEntryStepConfig) error {
	if raw, present := config["on_multiple"]; present {
		mode, ok := raw.(string)
		if !ok || (mode != "first" && mode != "fail") {
			return connectorErrors.NewInvalidConfigError("yaml_parser",
				"extract_map_entry 'on_multiple' must be \"first\" or \"fail\"")
		}

		cfg.onMultiple = mode
	}

	if raw, present := config["allowed_keys"]; present {
		keys, err := parseAllowedKeys(raw)
		if err != nil {
			return connectorErrors.NewInvalidConfigError("yaml_parser",
				fmt.Sprintf("extract_map_entry %v", err))
		}

		cfg.allowedKeys = keys
	}

	return nil
}

// parseMapEntryConfig validates extract_map_entry step config.
// In: config["source"] - dot-path to a map field
//
//	config["key_target"], config["value_target"] - output field names
//	config["on_multiple"] - optional "first"|"fail" (default "fail")
//	config["allowed_keys"] - optional key whitelist (see parseAllowedKeys)
//
// Out: mapEntryStepConfig - validated config
// Ex: parseMapEntryConfig({"source": "attributes", ...}) → cfg
func parseMapEntryConfig(config map[string]interface{}) (mapEntryStepConfig, error) {
	cfg := mapEntryStepConfig{onMultiple: "fail"}

	for key, dst := range map[string]*string{
		"source":       &cfg.source,
		"key_target":   &cfg.keyTarget,
		"value_target": &cfg.valueTarget,
	} {
		value, ok := config[key].(string)
		if !ok || value == "" {
			return mapEntryStepConfig{}, connectorErrors.NewInvalidConfigError("yaml_parser",
				fmt.Sprintf("extract_map_entry step requires a '%s' string option", key))
		}

		*dst = value
	}

	err := validateFieldPath(cfg.source)
	if err != nil {
		return mapEntryStepConfig{}, connectorErrors.NewInvalidConfigError("yaml_parser",
			fmt.Sprintf("extract_map_entry 'source' is not a valid field path: %v", err))
	}

	err = parseMapEntryOptions(config, &cfg)
	if err != nil {
		return mapEntryStepConfig{}, err
	}

	return cfg, nil
}

// selectMapEntry picks the single entry key to lift. Multiple entries under
// "first" take the lexicographically smallest STRINGIFIED key ("10" < "3"):
// the contract is determinism, not numeric order.
// In: m map - the decoded map field
//
//	onMultiple string - "first" | "fail"
//	source string - dot-path, for error/log context
//
// Out: string - selected key
// Ex: selectMapEntry({"3": v}, "fail", "attributes") → "3"
func selectMapEntry(m map[string]interface{}, onMultiple, source string) (string, error) {
	if len(m) == 0 {
		return "", fmt.Errorf("map '%s' is empty in extract_map_entry step", source)
	}

	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	if len(keys) > 1 {
		if onMultiple == "fail" {
			return "", fmt.Errorf("map '%s' has %d entries in extract_map_entry step, want exactly 1", source, len(keys))
		}

		slog.Warn("extract_map_entry taking first of multiple entries",
			"source", source, "entries", len(keys), "key", keys[0])
	}

	return keys[0], nil
}

// coerceMapKey converts a protobuf-stringified integer map key back to
// int64; non-integer keys stay strings. Both forms are contiguous heap
// strings, pinning-safe.
// In: key string - stringified map key
// Out: interface{} - int64 or string
// Ex: coerceMapKey("3") → int64(3)
func coerceMapKey(key string) interface{} {
	n, err := strconv.ParseInt(key, 10, 64)
	if err != nil {
		return key
	}

	return n
}

// makeExtractMapEntryStep creates a step lifting a decoded map field's
// single entry to named fields. Needed because a dot-path like
// "attributes.3.value" would bake the map KEY - which is data, not schema -
// into config.
//
// extract_map_entry is STRUCTURAL: every failure (missing/non-map source,
// empty map, multiple entries under on_multiple "fail", key not in
// allowed_keys) means "this message's shape is not what the pipeline
// consumes" → OutcomeUnusable → zero tables, nil error → the worker ACKs
// and counts messagesDropped under --parse-error-action drop. With
// allowed_keys set, this step IS the message-shape filter for map-keyed
// payloads (a nested parse_protobuf re-decode cannot filter: wrong-type
// bytes decode to zero fields without error - see makeParseProtobufStep).
//
// In: config - see parseMapEntryConfig
// Out: TransformationStep - lifts key/value into state.Fields
// Ex: makeExtractMapEntryStep({"source": "attributes", "key_target": "capability_key", "value_target": "attribute_raw"}) → step
func makeExtractMapEntryStep(config map[string]interface{}) (TransformationStep, error) {
	cfg, err := parseMapEntryConfig(config)
	if err != nil {
		return nil, err
	}

	return func(state *ParseState) error {
		value, err := extractFieldByPath(state.Fields, cfg.source)
		if err != nil {
			return connectorErrors.NewParsingFailedError("yaml_parser",
				fmt.Errorf("failed to resolve extract_map_entry source '%s': %w", cfg.source, err))
		}

		m, ok := value.(map[string]interface{})
		if !ok {
			return connectorErrors.NewParsingFailedError("yaml_parser",
				fmt.Errorf("extract_map_entry source '%s' has type %T, want a map", cfg.source, value))
		}

		key, err := selectMapEntry(m, cfg.onMultiple, cfg.source)
		if err != nil {
			return connectorErrors.NewParsingFailedError("yaml_parser", err)
		}

		if cfg.allowedKeys != nil && !cfg.allowedKeys[key] {
			slog.Warn("extract_map_entry dropping disallowed key", "source", cfg.source, "key", key)

			return connectorErrors.NewParsingFailedError("yaml_parser",
				fmt.Errorf("map key '%s' not in allowed_keys in extract_map_entry step", key))
		}

		state.Fields[cfg.keyTarget] = coerceMapKey(key)
		state.Fields[cfg.valueTarget] = m[key]

		return nil
	}, nil
}
