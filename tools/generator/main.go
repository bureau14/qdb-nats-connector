// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.

// Package main: qdb-data-gen CLI - test data generator for QuasarDB NATS connector.
// Types: none
// Ex: qdb-data-gen template.yaml --count 10000
package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"

	"github.com/spf13/pflag"

	"github.com/bureau14/qdb-nats-connector/tools/generator/internal/generator"
	_ "github.com/bureau14/qdb-nats-connector/tools/generator/internal/generator/generators"
)

// usageStr: CLI help text with all generator options
var usageStr = `
Usage: qdb-data-gen <template-file> [options]

Arguments:
    <template-file>                  Path to data generation template file

Options:
    --count <n>                      Number of records to generate (default: 1000)
    --mode <mode>                    Generation mode (default: batch)
    -h, --help                       Show this message

Examples:
    qdb-data-gen finance-template.yaml --count 10000
    qdb-data-gen sensor-template.yaml --count 1000 --mode batch
`

// usage prints CLI help & exits
// Out: exit(0)
// Ex: usage() → help text printed
func usage() {
	fmt.Println(usageStr)
	os.Exit(0)
}

// Generate creates JSON records according to template specifications
// In: templateFile path, count of records, writer for output
// Out: error if generation fails
// Ex: Generate("template.yaml", 1000, writer) → generates 1000 JSON records
func Generate(templateFile string, count int, writer io.Writer) error {
	// Create a new generation engine using the template file
	engine, err := generator.NewEngine(templateFile)
	if err != nil {
		return fmt.Errorf("failed to create generation engine from template %s: %w", templateFile, err)
	}

	// Generate the specified number of records
	err = engine.GenerateRecords(context.Background(), count, writer)
	if err != nil {
		return fmt.Errorf("failed to generate %d records using template %s: %w", count, templateFile, err)
	}

	return nil
}

// main starts the data generator process.
// In: command-line args (positional template file + flags)
// Out: generates test data according to template
// Ex: main() → generates data and outputs to stdout or file
func main() {
	exitCode := runMain()
	if exitCode != 0 {
		os.Exit(exitCode)
	}
}

// runMain executes generator with error handling
// In: none (reads os.Args)
// Out: exit code (0=success, 1=error)
// Ex: runMain() → 0 on successful generation
func runMain() int {
	fs := pflag.NewFlagSet("qdb-data-gen", pflag.ExitOnError)

	var showHelp bool
	var count int
	var mode string

	// Define CLI flags
	fs.BoolVarP(&showHelp, "help", "h", false, "Show this message")
	fs.IntVar(&count, "count", 1000, "Number of records to generate")
	fs.StringVar(&mode, "mode", "batch", "Generation mode")

	err := fs.Parse(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing flags: %v\n", err)

		return 1
	}

	if showHelp {
		usage()

		return 0
	}

	// Get positional arguments (non-flag arguments)
	args := fs.Args()
	if len(args) < 1 {
		fmt.Fprintf(os.Stderr, "Error: template file path is required\n")
		usage()

		return 1
	}

	templateFile := args[0]

	// Create buffered writer for efficient stdout writing
	writer := bufio.NewWriter(os.Stdout)

	// Generate data using the extracted Generate function
	err = Generate(templateFile, count, writer)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error generating data: %v\n", err)

		return 1
	}

	// Flush buffered writer before returning
	err = writer.Flush()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error flushing stdout: %v\n", err)

		return 1
	}

	return 0
}
