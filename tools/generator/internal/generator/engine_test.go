// Package generator_test verifies parallel generation semantics.
package generator_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/bureau14/qdb-nats-connector/tools/generator/internal/generator"
	// Register the built-in generators (sequence, etc.) via their init().
	_ "github.com/bureau14/qdb-nats-connector/tools/generator/internal/generator/generators"
)

// twoSequenceTemplate defines two independent sequence fields that advance in
// lockstep. A correct engine keeps them equal within every record regardless of
// worker count; a racy engine lets their counters drift apart.
const twoSequenceTemplate = `
name: alignment
table: t
fields:
  - name: a
    type: sequence
    config:
      start: 1
      step: 1
  - name: b
    type: sequence
    config:
      start: 1
      step: 1
`

// TestGenerateRecordsParallelKeepsSequencesAligned guards against the data race
// where concurrent workers share stateful generators, drifting sequence counters
// out of lockstep (the cause of cross-field routing to non-existent tables).
func TestGenerateRecordsParallelKeepsSequencesAligned(t *testing.T) {
	t.Parallel()

	path := writeTemplate(t, twoSequenceTemplate)

	engine, err := generator.NewEngine(path)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}

	const (
		count   = 5000
		workers = 16
	)

	var buf bytes.Buffer
	err = engine.GenerateRecordsParallel(context.Background(), count, workers, &buf)
	if err != nil {
		t.Fatalf("GenerateRecordsParallel: %v", err)
	}

	lines := 0
	scanner := bufio.NewScanner(&buf)
	for scanner.Scan() {
		lines++

		var record struct {
			A float64 `json:"a"`
			B float64 `json:"b"`
		}
		err := json.Unmarshal(scanner.Bytes(), &record)
		if err != nil {
			t.Fatalf("unmarshal record %d: %v", lines, err)
		}

		if record.A != record.B {
			t.Fatalf("record %d misaligned: a=%v b=%v", lines, record.A, record.B)
		}
	}

	err = scanner.Err()
	if err != nil {
		t.Fatalf("scan: %v", err)
	}

	if lines != count {
		t.Fatalf("expected %d records, got %d", count, lines)
	}
}

// writeTemplate writes template content to a temp file and returns its path.
func writeTemplate(t *testing.T, content string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "template.yaml")
	err := os.WriteFile(path, []byte(content), 0o600)
	if err != nil {
		t.Fatalf("write template: %v", err)
	}

	return path
}
