// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.

package generators

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"

	"github.com/bureau14/qdb-nats-connector/tools/generator/internal"
	"github.com/bureau14/qdb-nats-connector/tools/generator/internal/generator"
)

// patternCompositeGenerator creates high-cardinality IDs from patterns
// Memory-efficient generation of millions of unique IDs
// In: cardinality, patterns with weights and components
// Ex: sensor IDs like "ZONE01/UNIT-A100.TEMP001"
type patternCompositeGenerator struct {
	cardinality  int64        // Total unique IDs to generate
	patterns     []patternDef // Pattern definitions with weights
	currentIndex int64        // Current position in sequence
	totalCombos  []int64      // Total combinations per pattern
	weightedSums []int64      // Cumulative weighted sums
	mu           sync.Mutex   // Thread safety
}

// patternDef defines a single pattern with weight
type patternDef struct {
	weight     int                     // Relative weight
	template   string                  // Template string with placeholders
	components map[string]componentDef // Component definitions
}

// componentDef defines how to generate a component
type componentDef struct {
	values    []string         // Static values to choose from
	generator patternGenerator // Dynamic pattern generator
}

// patternGenerator generates patterned values
type patternGenerator interface {
	generate(index int64) string
	count() int64
}

// NewPatternCompositeGenerator creates high-cardinality ID generator
// Config:
//   - cardinality: total unique IDs (required)
//   - patterns: array of pattern definitions with weights
//
// Ex: {"cardinality": 2000000, "patterns": [...]}
func NewPatternCompositeGenerator(config map[string]interface{}) (*patternCompositeGenerator, error) {
	gen := &patternCompositeGenerator{}

	// Parse cardinality
	if card, ok := getFloat64(config, "cardinality"); ok && card > 0 {
		gen.cardinality = int64(card)
	} else {
		return nil, fmt.Errorf("pattern_composite requires positive 'cardinality'")
	}

	// Parse patterns
	patternsRaw, ok := config["patterns"].([]interface{})
	if !ok || len(patternsRaw) == 0 {
		return nil, fmt.Errorf("pattern_composite requires 'patterns' array")
	}

	// Process each pattern
	for i, patRaw := range patternsRaw {
		patMap, ok := patRaw.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("pattern %d is not a map", i)
		}

		pattern, err := parsePattern(patMap)
		if err != nil {
			return nil, fmt.Errorf("pattern %d: %w", i, err)
		}

		gen.patterns = append(gen.patterns, pattern)
	}

	// Calculate total combinations and weighted distribution
	err := gen.calculateDistribution()
	if err != nil {
		return nil, err
	}

	return gen, nil
}

// parsePattern parses a single pattern definition
func parsePattern(patMap map[string]interface{}) (patternDef, error) {
	pattern := patternDef{
		weight:     1, // Default weight
		components: make(map[string]componentDef),
	}

	// Parse weight
	if weight, ok := getFloat64(patMap, "weight"); ok {
		pattern.weight = int(weight)
	}

	// Parse template
	template, ok := patMap["template"].(string)
	if !ok {
		return pattern, fmt.Errorf("pattern requires 'template' string")
	}
	pattern.template = template

	// Parse components
	componentsMap, ok := patMap["components"].(map[string]interface{})
	if !ok {
		return pattern, fmt.Errorf("pattern requires 'components' map")
	}

	// Extract placeholders from template
	placeholders := extractPlaceholders(template)

	// Parse each component
	for name, compRaw := range componentsMap {
		if !contains(placeholders, name) {
			continue // Skip unused components
		}

		comp, err := parseComponent(compRaw)
		if err != nil {
			return pattern, fmt.Errorf("component %s: %w", name, err)
		}
		pattern.components[name] = comp
	}

	// Verify all placeholders have components
	for _, ph := range placeholders {
		if _, ok := pattern.components[ph]; !ok {
			return pattern, fmt.Errorf("template placeholder {%s} has no component", ph)
		}
	}

	return pattern, nil
}

// parseComponent parses a component definition
func parseComponent(compRaw interface{}) (componentDef, error) {
	comp := componentDef{}

	// Simple array of values
	if values, ok := compRaw.([]interface{}); ok {
		for _, v := range values {
			if str, ok := v.(string); ok {
				comp.values = append(comp.values, str)
			} else {
				return comp, fmt.Errorf("component value must be string")
			}
		}

		return comp, nil
	}

	// Complex component with type
	if compMap, ok := compRaw.(map[string]interface{}); ok {
		compType, _ := compMap["type"].(string)

		switch compType {
		case "pattern":
			gen, err := createPatternGenerator(compMap)
			if err != nil {
				return comp, err
			}
			comp.generator = gen

		case "sequence":
			gen, err := createSequenceGenerator(compMap)
			if err != nil {
				return comp, err
			}
			comp.generator = gen

		default:
			return comp, fmt.Errorf("unknown component type: %s", compType)
		}

		return comp, nil
	}

	return comp, fmt.Errorf("invalid component definition")
}

// Generate produces next ID in sequence
func (g *patternCompositeGenerator) Generate(ctx context.Context) (interface{}, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	// Cycle through cardinality
	index := g.currentIndex % g.cardinality
	g.currentIndex++

	// Determine which pattern to use based on weighted distribution
	patternIdx := g.selectPattern(index)
	pattern := g.patterns[patternIdx]

	// Calculate sub-index within this pattern
	startIdx := int64(0)
	if patternIdx > 0 {
		// Calculate how many IDs previous patterns should generate
		for i := range patternIdx {
			weight := float64(g.patterns[i].weight) / float64(g.weightedSums[len(g.patterns)-1])
			startIdx += int64(weight * float64(g.cardinality))
		}
	}

	subIndex := index - startIdx

	// Generate ID from pattern
	id := g.generateFromPattern(pattern, subIndex)

	return id, nil
}

// calculateDistribution calculates weighted distribution
func (g *patternCompositeGenerator) calculateDistribution() error {
	g.totalCombos = make([]int64, len(g.patterns))
	g.weightedSums = make([]int64, len(g.patterns))

	var totalWeight int64

	// Calculate combinations per pattern
	for i, pattern := range g.patterns {
		combos := int64(1)

		// Calculate total combinations for this pattern
		for _, comp := range pattern.components {
			if len(comp.values) > 0 {
				combos *= int64(len(comp.values))
			} else if comp.generator != nil {
				combos *= comp.generator.count()
			}
		}

		g.totalCombos[i] = combos
		totalWeight += int64(pattern.weight)

		if i > 0 {
			g.weightedSums[i] = g.weightedSums[i-1] + int64(pattern.weight)
		} else {
			g.weightedSums[i] = int64(pattern.weight)
		}
	}

	// Verify sufficient combinations
	totalPossible := int64(0)
	for i, combos := range g.totalCombos {
		weightedCombos := combos * int64(g.patterns[i].weight) / totalWeight
		totalPossible += weightedCombos
	}

	if totalPossible < g.cardinality {
		return fmt.Errorf("insufficient combinations: %d possible < %d requested",
			totalPossible, g.cardinality)
	}

	return nil
}

// selectPattern chooses pattern based on index
func (g *patternCompositeGenerator) selectPattern(index int64) int {
	// Distribute indices across patterns based on weights
	totalWeight := g.weightedSums[len(g.weightedSums)-1]
	scaledIndex := index * totalWeight / g.cardinality

	for i, sum := range g.weightedSums {
		if scaledIndex < sum {
			return i
		}
	}

	return len(g.patterns) - 1
}

// generateFromPattern creates ID from pattern and index
func (g *patternCompositeGenerator) generateFromPattern(pattern patternDef, index int64) string {
	result := pattern.template

	// Replace each placeholder
	for name, comp := range pattern.components {
		value := g.getComponentValue(comp, index)
		placeholder := "{" + name + "}"
		result = strings.ReplaceAll(result, placeholder, value)

		// Update index for next component
		if len(comp.values) > 0 {
			index /= int64(len(comp.values))
		} else if comp.generator != nil {
			index /= comp.generator.count()
		}
	}

	return result
}

// getComponentValue gets value for component at index
func (g *patternCompositeGenerator) getComponentValue(comp componentDef, index int64) string {
	if len(comp.values) > 0 {
		// Static values
		idx := index % int64(len(comp.values))

		return comp.values[idx]
	} else if comp.generator != nil {
		// Dynamic generator
		return comp.generator.generate(index)
	}

	return ""
}

// Helper functions
func parseDigitsFromString(gen *simplePatternGenerator, digitsStr string) {
	var minVal, maxVal int
	_, err := fmt.Sscanf(digitsStr, "%d-%d", &minVal, &maxVal)
	if err == nil {
		gen.minDig = minVal
		gen.maxDig = maxVal

		return
	}

	_, err = fmt.Sscanf(digitsStr, "%d", &minVal)
	if err == nil {
		gen.minDig = minVal
		gen.maxDig = minVal
	}
}

func extractPlaceholders(template string) []string {
	var placeholders []string
	start := -1

	for i, ch := range template {
		if ch == '{' {
			start = i
		} else if ch == '}' && start >= 0 {
			placeholder := template[start+1 : i]
			placeholders = append(placeholders, placeholder)
			start = -1
		}
	}

	return placeholders
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}

	return false
}

// Pattern generators for components

// simplePatternGenerator generates pattern-based values
type simplePatternGenerator struct {
	prefix string
	base   string
	minDig int
	maxDig int
}

func createPatternGenerator(config map[string]interface{}) (*simplePatternGenerator, error) {
	gen := &simplePatternGenerator{}

	// Parse prefix array
	if prefixes, ok := config["prefix"].([]interface{}); ok {
		// For simplicity, just use first prefix
		if len(prefixes) > 0 {
			if p, ok := prefixes[0].(string); ok {
				gen.prefix = p
			}
		}
	}

	// Parse base array
	if bases, ok := config["base"].([]interface{}); ok {
		if len(bases) > 0 {
			if b, ok := bases[0].(string); ok {
				gen.base = b
			}
		}
	}

	// Parse digits range
	if digitsStr, ok := config["digits"].(string); ok {
		parseDigitsFromString(gen, digitsStr)
	} else if digits, ok := getFloat64(config, "digits"); ok {
		gen.minDig = int(digits)
		gen.maxDig = int(digits)
	}

	return gen, nil
}

func (g *simplePatternGenerator) generate(index int64) string {
	// Generate number with appropriate digits
	numDigits := g.minDig
	if g.maxDig > g.minDig {
		numDigits = g.minDig + int(index%int64(g.maxDig-g.minDig+1))
	}

	format := fmt.Sprintf("%%s%%s%%0%dd", numDigits)
	maxNum := int64(math.Pow10(numDigits))
	num := index % maxNum

	return fmt.Sprintf(format, g.prefix, g.base, num)
}

func (g *simplePatternGenerator) count() int64 {
	// Simplified: assume max digits
	return int64(math.Pow10(g.maxDig))
}

// sequencePatternGenerator generates sequential values
type sequencePatternGenerator struct {
	start int64
	end   int64
}

func createSequenceGenerator(config map[string]interface{}) (*sequencePatternGenerator, error) {
	gen := &sequencePatternGenerator{
		start: 1,
		end:   9999,
	}

	if start, ok := getFloat64(config, "start"); ok {
		gen.start = int64(start)
	}

	if end, ok := getFloat64(config, "end"); ok {
		gen.end = int64(end)
	}

	return gen, nil
}

func (g *sequencePatternGenerator) generate(index int64) string {
	value := g.start + (index % (g.end - g.start + 1))

	return fmt.Sprintf("%d", value)
}

func (g *sequencePatternGenerator) count() int64 {
	return g.end - g.start + 1
}

// Register the generator
func init() {
	generator.RegisterGenerator("pattern_composite", func(config map[string]interface{}) (internal.FieldGenerator, error) {
		return NewPatternCompositeGenerator(config)
	})
}
