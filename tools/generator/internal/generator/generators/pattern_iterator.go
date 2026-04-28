// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.

package generators

import (
	"fmt"
	"math"
)

// PatternIterator provides memory-efficient iteration over pattern space
// Enables O(1) memory generation of high-cardinality patterns
// In: pattern components, total space size
// Ex: iterator for 2M sensor IDs without storing them
type PatternIterator struct {
	components []IteratorComponent // Component iterators
	sizes      []int64             // Size of each component
	indices    []int64             // Current indices
	total      int64               // Total iteration space
	current    int64               // Current position
}

// IteratorComponent defines iteration over a component space
type IteratorComponent interface {
	// ValueAt returns value at given index
	ValueAt(index int64) string
	// Size returns total number of values
	Size() int64
}

// NewPatternIterator creates iterator over pattern space
// In: components defining iteration space
// Out: initialized iterator
// Ex: NewPatternIterator(comps) → iterator for pattern combinations
func NewPatternIterator(components []IteratorComponent) *PatternIterator {
	it := &PatternIterator{
		components: components,
		sizes:      make([]int64, len(components)),
		indices:    make([]int64, len(components)),
		total:      1,
	}

	// Calculate sizes and total space
	for i, comp := range components {
		size := comp.Size()
		it.sizes[i] = size
		it.total *= size
	}

	return it
}

// Next advances to next combination
// Out: true if more values, false if done
// Ex: for it.Next() { values := it.Values() }
func (it *PatternIterator) Next() bool {
	if it.current >= it.total {
		return false
	}

	// Calculate indices from current position
	remaining := it.current
	for i := len(it.components) - 1; i >= 0; i-- {
		it.indices[i] = remaining % it.sizes[i]
		remaining /= it.sizes[i]
	}

	it.current++

	return true
}

// Values returns current component values
// Out: array of current values
// Ex: values := it.Values() → ["ZONE01", "UNIT", "TEMP001"]
func (it *PatternIterator) Values() []string {
	values := make([]string, len(it.components))
	for i, comp := range it.components {
		values[i] = comp.ValueAt(it.indices[i])
	}

	return values
}

// Reset returns iterator to start
// Ex: it.Reset() → back to first combination
func (it *PatternIterator) Reset() {
	it.current = 0
	for i := range it.indices {
		it.indices[i] = 0
	}
}

// Progress returns iteration progress
// Out: current position, total space
// Ex: curr, total := it.Progress() → 1000, 2000000
func (it *PatternIterator) Progress() (current, total int64) {
	return it.current, it.total
}

// Component implementations

// ArrayIteratorComponent iterates over static array
type ArrayIteratorComponent struct {
	values []string
}

// NewArrayIterator creates iterator over array values
// In: array of values
// Out: component iterator
// Ex: NewArrayIterator([]string{"A", "B", "C"})
func NewArrayIterator(values []string) *ArrayIteratorComponent {
	return &ArrayIteratorComponent{values: values}
}

func (c *ArrayIteratorComponent) ValueAt(index int64) string {
	if index < 0 || index >= int64(len(c.values)) {
		return ""
	}

	return c.values[index]
}

func (c *ArrayIteratorComponent) Size() int64 {
	return int64(len(c.values))
}

// RangeIteratorComponent iterates over numeric range
type RangeIteratorComponent struct {
	start  int64
	end    int64
	format string
}

// NewRangeIterator creates iterator over numeric range
// In: start, end values, format string
// Out: component iterator
// Ex: NewRangeIterator(1, 1000, "%03d")
func NewRangeIterator(start, end int64, format string) *RangeIteratorComponent {
	return &RangeIteratorComponent{
		start:  start,
		end:    end,
		format: format,
	}
}

func (c *RangeIteratorComponent) ValueAt(index int64) string {
	value := c.start + index
	if value > c.end {
		value = c.end
	}

	return fmt.Sprintf(c.format, value)
}

func (c *RangeIteratorComponent) Size() int64 {
	return c.end - c.start + 1
}

// PatternIteratorComponent iterates over patterned values
type PatternIteratorComponent struct {
	prefix   string
	base     string
	digits   int
	maxValue int64
}

// NewPatternIterator creates iterator for pattern values
// In: prefix, base, number of digits
// Out: component iterator
// Ex: NewPatternIterator("TEMP", "", 3) → TEMP001, TEMP002...
func NewPatternIteratorComponent(prefix, base string, digits int) *PatternIteratorComponent {
	return &PatternIteratorComponent{
		prefix:   prefix,
		base:     base,
		digits:   digits,
		maxValue: int64(math.Pow10(digits)),
	}
}

func (c *PatternIteratorComponent) ValueAt(index int64) string {
	format := fmt.Sprintf("%s%s%%0%dd", c.prefix, c.base, c.digits)
	value := index % c.maxValue

	return fmt.Sprintf(format, value)
}

func (c *PatternIteratorComponent) Size() int64 {
	return c.maxValue
}

// CycleIteratorComponent cycles through values repeatedly
type CycleIteratorComponent struct {
	values []string
	cycles int64
}

// NewCycleIterator creates iterator that cycles values
// In: values to cycle, number of cycles
// Out: component iterator
// Ex: NewCycleIterator([]string{"A", "B"}, 1000) → A,B,A,B...
func NewCycleIterator(values []string, cycles int64) *CycleIteratorComponent {
	return &CycleIteratorComponent{
		values: values,
		cycles: cycles,
	}
}

func (c *CycleIteratorComponent) ValueAt(index int64) string {
	if len(c.values) == 0 {
		return ""
	}

	return c.values[index%int64(len(c.values))]
}

func (c *CycleIteratorComponent) Size() int64 {
	return int64(len(c.values)) * c.cycles
}

// CartesianProduct calculates total combinations
// In: component sizes
// Out: total combination count
// Ex: CartesianProduct([]int64{10, 20, 30}) → 6000
func CartesianProduct(sizes []int64) int64 {
	if len(sizes) == 0 {
		return 0
	}

	total := int64(1)
	for _, size := range sizes {
		total *= size
	}

	return total
}

// IndexToIndices converts flat index to component indices
// In: flat index, component sizes
// Out: array of component indices
// Ex: IndexToIndices(123, []int64{10, 10, 10}) → [1, 2, 3]
func IndexToIndices(index int64, sizes []int64) []int64 {
	indices := make([]int64, len(sizes))

	for i := len(sizes) - 1; i >= 0; i-- {
		indices[i] = index % sizes[i]
		index /= sizes[i]
	}

	return indices
}

// IndicesToIndex converts component indices to flat index
// In: component indices, component sizes
// Out: flat index
// Ex: IndicesToIndex([]int64{1, 2, 3}, []int64{10, 10, 10}) → 123
func IndicesToIndex(indices, sizes []int64) int64 {
	if len(indices) != len(sizes) {
		return -1
	}

	index := int64(0)
	multiplier := int64(1)

	for i := len(indices) - 1; i >= 0; i-- {
		index += indices[i] * multiplier
		multiplier *= sizes[i]
	}

	return index
}
