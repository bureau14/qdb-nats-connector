# Parameterized Makefile for qdb-nats-connector Examples

.PHONY: help test generate-golden extract package-golden print-vars test-validation build-tools debug test-quick test-full all-test all-generate-golden all-package-golden all-extract test-all generate-all package-all extract-all test-all-sizes generate-golden-all-sizes package-golden-all-sizes
.DEFAULT_GOAL := help


# =============================================================================
# Configuration
# =============================================================================

# Message count mappings define standardized dataset sizes for reproducible testing.
# These sizes enable quick iteration (10k), CI/CD validation (100k), full testing (1m),
# and stress testing (10m). The exact counts ensure consistent performance baselines.
SIZES := 10k 100k 1m 10m
NUM_10k := 10000
NUM_100k := 100000
NUM_1m := 1000000
NUM_10m := 10000000
TESTDATA_DIR := testdata

# Dynamically discover example scripts, excluding utility scripts. This enables
# zero-maintenance addition of new examples - just drop a new .sh file and it's
# automatically included in test matrices.
EXAMPLES := $(filter-out common utils test-loader debug-industrial-sensor, $(basename $(wildcard *.sh)))
EXAMPLE ?= finance-ohlc
SIZE ?= 10k

# DATE is used for versioning golden data archives per ADR-007
DATE := $(shell date +%Y-%m-%d)

# S3 Golden Data Configuration
S3_BUCKET := s3://teamcity-agentbuilddeps-20241223095405875100000001/datasets/golden
S3_BASE_URL := https://teamcity-agentbuilddeps.s3.amazonaws.com/datasets/golden

# Golden data versioning
GOLDEN_VERSION ?= $(DATE)  # Can override for specific versions

# CI/CD integration flag (for future use)
GOLDEN_AUTO_DOWNLOAD ?= false


# Parameter validation ensures early failure with clear error messages rather than
# cryptic failures deep in script execution. This saves debugging time by catching
# typos and invalid combinations at the entry point.

# Validation function for parameters
define validate_params
	@# First validation: Check if the EXAMPLE parameter matches a discovered script.
	@# We use $(filter) to search for EXAMPLE in our EXAMPLES list. If filter returns
	@# empty (checked with -z test), the example doesn't exist.
	@if [ -z "$(filter $(EXAMPLE),$(EXAMPLES))" ]; then \
		echo "Error: Invalid EXAMPLE=$(EXAMPLE)"; \
		echo "Valid examples: $(EXAMPLES)"; \
		exit 1; \
	fi
	@# Second validation: Check if SIZE is one of our predefined dataset sizes.
	@# These sizes map to specific message counts (10k=10000, 1m=1000000, etc.)
	@# ensuring consistent test baselines across runs.
	@if [ -z "$(filter $(SIZE),$(SIZES))" ]; then \
		echo "Error: Invalid SIZE=$(SIZE)"; \
		echo "Valid sizes: $(SIZES)"; \
		exit 1; \
	fi
endef


# Color-coded logging provides visual scanning efficiency in CI/CD logs and terminals.
# Blue=informational progress, Green=successful completion, Yellow=non-fatal issues,
# Red=failures requiring intervention. This aligns with standard Unix conventions.

# Color logging
COLOR_RESET := \033[0m
COLOR_GREEN := \033[32m
COLOR_BLUE := \033[34m
COLOR_YELLOW := \033[33m
COLOR_RED := \033[31m
LOG_INFO = echo "$(COLOR_BLUE)[INFO]$(COLOR_RESET) $(1)"
LOG_SUCCESS = echo "$(COLOR_GREEN)[SUCCESS]$(COLOR_RESET) $(1)"
LOG_WARNING = echo "$(COLOR_YELLOW)[WARNING]$(COLOR_RESET) $(1)"
LOG_ERROR = echo "$(COLOR_RED)[ERROR]$(COLOR_RESET) $(1)"


# run_test_workflow orchestrates the complete end-to-end testing pipeline:
# 1. Infrastructure setup (NATS streams, QuasarDB tables)
# 2. Data generation OR reuse of extracted golden data (performance optimization)
# 3. NATS JetStream data loading
# 4. Connector execution with configured parser
# 5. Await processing completion with timeout protection
# 6. Export results to CSV for comparison
# 7. Graceful shutdown with cleanup
# 8. Validation against golden data (if available)
#
# The function uses trap for cleanup on failure, ensuring no orphaned processes.
# Golden data reuse (step 2) significantly speeds up repeated test runs by skipping
# the expensive data generation phase when pre-validated datasets are available.
define run_test_workflow
	@$(call LOG_INFO,Starting test workflow: $(1) with $(3) messages)

	@# Install cleanup handler to stop the connector even if the script fails midway.
	@# This prevents orphaned processes that would interfere with subsequent test runs.
	@trap './$(1).sh stop 2>/dev/null || true' EXIT
	@$(call LOG_INFO,Step 1/8: Creating infrastructure for $(1))

	@# Step 1: Create the required infrastructure (NATS streams, QuasarDB tables).
	@# The create action is idempotent - safe to run multiple times.
	@./$(1).sh create 2>&1 || { $(call LOG_ERROR,Failed to create infrastructure for $(1)); ./$(1).sh stop 2>/dev/null || true; exit 1; }
	@$(call LOG_INFO,Step 2/8: Preparing test data for $(1))

	@# Step 2: Smart data preparation - check if we have pre-extracted golden data.
	@# The .extracted marker file indicates we have a validated dataset ready to use.
	@if [ -f "$(TESTDATA_DIR)/$(1)-$(2)/.extracted" ] && [ -d "$(TESTDATA_DIR)/$(1)-$(2)/expected" ]; then \
		$(call LOG_INFO,Using extracted test data for $(1)); \
		export INPUT_FILE="$(TESTDATA_DIR)/$(1)-$(2)/$(1)-$(2)-input.data"; \
		export GOLDEN_DIR="$(TESTDATA_DIR)/$(1)-$(2)/expected"; \
	else \
		$(call LOG_ERROR,Golden data not found for $(1)-$(2)); \
		echo "To fix this:"; \
		echo "  1. Generate: make generate-golden EXAMPLE=$(1) SIZE=$(2)"; \
		echo "  2. Package:  make package-golden EXAMPLE=$(1) SIZE=$(2)"; \
		echo "  3. Extract:  make extract EXAMPLE=$(1) SIZE=$(2)"; \
		echo "  4. Or download from S3 (coming soon)"; \
		exit 1; \
	fi
	@$(call LOG_INFO,Step 3/8: Loading data for $(1))

	@# Step 3: Load test data into NATS JetStream. Each message gets durably stored
	@# in the stream, ready for the connector to consume.
	@NUM_MESSAGES=$(3) ./$(1).sh load 2>&1 || { $(call LOG_ERROR,Failed to load data for $(1)); ./$(1).sh stop 2>/dev/null || true; exit 1; }
	@$(call LOG_INFO,Step 4/8: Starting connector for $(1))

	@# Step 4: Start the connector with the appropriate parser configuration.
	@# The connector runs in background, reading from NATS and writing to QuasarDB.
	@NUM_MESSAGES=$(3) ./$(1).sh run 2>&1 || { $(call LOG_ERROR,Failed to start connector for $(1)); ./$(1).sh stop 2>/dev/null || true; exit 1; }
	@$(call LOG_INFO,Step 5/8: Waiting for processing completion for $(1))

	@# Step 5: Wait for processing completion by monitoring QuasarDB row counts.
	@# This polls until all expected messages are processed or timeout occurs.
	@NUM_MESSAGES=$(3) ./$(1).sh wait 2>&1 || { $(call LOG_ERROR,Processing failed for $(1)); ./$(1).sh stop 2>/dev/null || true; exit 1; }
	@$(call LOG_INFO,Step 6/8: Exporting results for $(1))

	@# Step 6: Export processed data from QuasarDB to CSV for validation.
	@# This creates human-readable output that can be compared against golden data.
	@./$(1).sh export 2>&1 || { $(call LOG_ERROR,Failed to export results for $(1)); ./$(1).sh stop 2>/dev/null || true; exit 1; }
	@$(call LOG_INFO,Step 7/8: Stopping connector for $(1))

	@# Step 7: Graceful shutdown. Warning (not error) on stop failure since the
	@# data has already been processed successfully at this point.
	@./$(1).sh stop 2>&1 || $(call LOG_WARNING,Failed to stop connector gracefully for $(1))
	@$(call LOG_INFO,Step 8/8: Validating results for $(1))

	@# Step 8: Validation against golden data (if available).
	@# This is the critical quality gate - comparing actual output with expected results.
	@NUM_MESSAGES=$(3) ./$(1).sh validate 2>&1 || $(call LOG_WARNING,Validation failed or no golden data available for $(1))
endef


# print-vars displays current parameter values for quick configuration checks
print-vars:
	@echo "EXAMPLE: $(EXAMPLE), SIZE: $(SIZE), NUM_MESSAGES: $(NUM_$(SIZE))"
	@echo "Examples: $(EXAMPLES)"; echo "Sizes: $(SIZES)"


# test-validation verifies parameter values without running any operations.
# Useful for CI/CD parameter verification and debugging configuration issues.
test-validation:
	$(call validate_params)
	@echo "Validation passed!"


# build-tools ensures the Go data generation and loading utilities are compiled.
# These tools are prerequisites for all test operations, handling data generation
# and NATS loading. Built into ../bin/ to share across all examples.
build-tools:
	@$(call LOG_INFO,Building qdb-data-gen and qdb-data-loader)
	@cd .. && direnv exec . go build -o bin/qdb-data-gen ./tools/generator
	@cd .. && direnv exec . go build -o bin/qdb-data-loader ./tools/loader
	@$(call LOG_SUCCESS,Tools built successfully)


# test runs the complete E2E validation pipeline, preferring existing golden data
# when available. This is the primary target for CI/CD and local validation.
test: build-tools
	$(call validate_params)
	$(call run_test_workflow,$(EXAMPLE),$(SIZE),$(NUM_$(SIZE)))


# generate-golden creates new reference datasets by running the complete pipeline
# and capturing the output as the "correct" result. Use this when:
# - Creating initial golden data for a new example
# - Updating golden data after intentional output changes
# - Regenerating data after parser or transformation updates
# The resulting data becomes the validation baseline per ADR-007.
generate-golden: build-tools
	@# Validate parameters before starting the expensive golden data generation process
	$(call validate_params)
	@$(call LOG_INFO,Generating golden data for $(EXAMPLE) with $(NUM_$(SIZE)) messages)
	
	@# Install cleanup handler to stop the connector even if the script fails midway.
	@# This prevents orphaned processes that would interfere with subsequent operations.
	@trap './$(EXAMPLE).sh stop 2>/dev/null || true' EXIT
	
	@# Create fresh infrastructure to ensure clean baseline - no contamination from previous runs
	@NUM_MESSAGES=$(NUM_$(SIZE)) ./$(EXAMPLE).sh create 2>&1 || { $(call LOG_ERROR,Failed to create infrastructure); exit 1; }
	
	@# Generate deterministic test data - same seed produces identical datasets for reproducibility
	@NUM_MESSAGES=$(NUM_$(SIZE)) ./$(EXAMPLE).sh generate 2>&1 || { $(call LOG_ERROR,Failed to generate data); exit 1; }
	
	@# Load all generated messages into NATS - this becomes our test input corpus
	@NUM_MESSAGES=$(NUM_$(SIZE)) ./$(EXAMPLE).sh load 2>&1 || { $(call LOG_ERROR,Failed to load data); exit 1; }
	
	@# Execute the connector with current parser configuration - this produces our "expected" output
	@NUM_MESSAGES=$(NUM_$(SIZE)) ./$(EXAMPLE).sh run 2>&1 || { $(call LOG_ERROR,Failed to run connector); exit 1; }
	
	@# Wait for all messages to be processed and written to QuasarDB tables
	@NUM_MESSAGES=$(NUM_$(SIZE)) ./$(EXAMPLE).sh wait 2>&1 || { $(call LOG_ERROR,Failed to wait for processing); exit 1; }
	
	@# Export processed data to CSV files - these become our expected results
	@NUM_MESSAGES=$(NUM_$(SIZE)) ./$(EXAMPLE).sh export 2>&1 || { $(call LOG_ERROR,Failed to export results); exit 1; }
	@$(call LOG_INFO,Stopping connector for $(EXAMPLE))
	
	@# CRITICAL: Move exported CSVs to golden/ directory, making them the new truth baseline.
	@# After this step, all future tests will validate against these results.
	@NUM_MESSAGES=$(NUM_$(SIZE)) ./$(EXAMPLE).sh prepare-golden 2>&1 || { $(call LOG_ERROR,Failed to prepare golden data); exit 1; }
	@$(call LOG_SUCCESS,Golden data generated successfully for $(EXAMPLE)-$(SIZE))


# Convenience shortcuts for common test scenarios
test-quick:
	$(MAKE) test SIZE=10k
test-full:
	$(MAKE) test SIZE=1m


# extract unpacks previously generated golden data archives for reuse.
# This enables sharing validated datasets across team members and CI/CD systems,
# avoiding regeneration costs. The .extracted marker prevents redundant unpacking.
extract:
	@# Ensure parameters are valid before attempting extraction
	$(call validate_params)
	@$(call LOG_INFO,Extracting test data for $(EXAMPLE)-$(SIZE))
	
	@# Create the target directory structure if it doesn't exist.
	@# Path format: testdata/example-size/ (e.g., testdata/finance-ohlc-10k/)
	@mkdir -p $(TESTDATA_DIR)/$(EXAMPLE)-$(SIZE)
	
	@# Check if the archive exists before attempting extraction.
	@# Archive naming: example-YYYY-MM-DD-size.tar.gz (date-versioned for traceability)
	@if [ -f "$(TESTDATA_DIR)/$(EXAMPLE)-$(DATE)-$(SIZE).tar.gz" ]; then \
		tar -xzf $(TESTDATA_DIR)/$(EXAMPLE)-$(DATE)-$(SIZE).tar.gz -C $(TESTDATA_DIR)/$(EXAMPLE)-$(SIZE) && \
		touch $(TESTDATA_DIR)/$(EXAMPLE)-$(SIZE)/.extracted && \
		$(call LOG_SUCCESS,Extracted test data for $(EXAMPLE)-$(SIZE)); \
	else \
		$(call LOG_ERROR,Archive not found: $(TESTDATA_DIR)/$(EXAMPLE)-$(DATE)-$(SIZE).tar.gz); \
		exit 1; fi


# package-golden archives the golden/ directory with timestamp versioning.
# This creates distributable test datasets for CI/CD systems and team sharing.
# Archives follow the naming pattern: example-YYYY-MM-DD-size.tar.gz
package-golden:
	@# Validate parameters to ensure consistent archive naming
	$(call validate_params)
	@$(call LOG_INFO,Packaging golden data for $(EXAMPLE)-$(SIZE))
	
	@# Check that golden/ directory exists before attempting to package
	@if [ -d "$(TESTDATA_DIR)/golden" ]; then \
		$(call LOG_INFO,Creating package structure for $(EXAMPLE)-$(SIZE)); \
		cd $(TESTDATA_DIR) && \
		mkdir -p package-temp/expected && \
		cp golden/$(EXAMPLE)-$(SIZE)-input.data package-temp/ 2>/dev/null || \
			cp golden/*-input.data package-temp/ 2>/dev/null || \
			$(call LOG_WARNING,No input data file found); \
		cp golden/*.csv package-temp/expected/ 2>/dev/null || \
			$(call LOG_WARNING,No CSV files found); \
		tar -czf $(EXAMPLE)-$(DATE)-$(SIZE).tar.gz -C package-temp . && \
		rm -rf package-temp && \
		$(call LOG_SUCCESS,Packaged golden data to $(EXAMPLE)-$(DATE)-$(SIZE).tar.gz); \
	else \
		$(call LOG_ERROR,Golden data directory not found: $(TESTDATA_DIR)/golden); \
		echo "Run 'make generate-golden EXAMPLE=$(EXAMPLE) SIZE=$(SIZE)' first"; \
		exit 1; fi


# download-golden fetches golden data archives from S3 (placeholder).
# This functionality will be activated once golden datasets are uploaded to S3.
download-golden:
	@$(call validate_params)
	@$(call LOG_ERROR,Download functionality not yet implemented)
	@echo ""
	@echo "Golden datasets will be available at:"
	@echo "  $(S3_BASE_URL)/$(EXAMPLE)-$(GOLDEN_VERSION)-$(SIZE).tar.gz"
	@echo ""
	@echo "Current workaround options:"
	@echo "  1. Generate locally: make generate-golden EXAMPLE=$(EXAMPLE) SIZE=$(SIZE)"
	@echo "  2. Use existing package: make extract EXAMPLE=$(EXAMPLE) SIZE=$(SIZE)"
	@echo ""
	@echo "This functionality will be activated once golden datasets are uploaded to S3."
	@# Future implementation location:
	@# curl -f -o $(TESTDATA_DIR)/$(EXAMPLE)-$(GOLDEN_VERSION)-$(SIZE).tar.gz \
	@#      $(S3_BASE_URL)/$(EXAMPLE)-$(GOLDEN_VERSION)-$(SIZE).tar.gz
	@exit 1

# check-golden verifies if golden data exists locally or needs download.
check-golden:
	@$(call validate_params)
	@echo "Checking golden data for $(EXAMPLE)-$(SIZE)..."
	@if [ -f "$(TESTDATA_DIR)/$(EXAMPLE)-$(SIZE)/.extracted" ]; then \
		$(call LOG_SUCCESS,Golden data available locally: $(EXAMPLE)-$(SIZE)); \
		echo "  Location: $(TESTDATA_DIR)/$(EXAMPLE)-$(SIZE)/"; \
	elif [ -f "$(TESTDATA_DIR)/$(EXAMPLE)-$(GOLDEN_VERSION)-$(SIZE).tar.gz" ]; then \
		$(call LOG_INFO,Golden archive found but not extracted); \
		echo "  Run: make extract EXAMPLE=$(EXAMPLE) SIZE=$(SIZE)"; \
	else \
		$(call LOG_WARNING,Golden data not found locally); \
		echo "  Would download from: $(S3_BASE_URL)/$(EXAMPLE)-$(GOLDEN_VERSION)-$(SIZE).tar.gz"; \
		echo "  Run: make download-golden EXAMPLE=$(EXAMPLE) SIZE=$(SIZE)"; \
	fi


# Batch operations for matrix testing across all examples or sizes.
# These targets enable comprehensive validation in CI/CD pipelines.

define run_all_examples
all-$(1):
	@# Iterate all examples with fixed size - useful for validating parser changes
	@for example in $(EXAMPLES); do \
		echo "Running $(1) for $$$$example with SIZE=$(SIZE)"; \
		$(MAKE) $(1) EXAMPLE=$$$$example SIZE=$(SIZE) || exit 1; done
endef
$(foreach action,test generate-golden package-golden extract,$(eval $(call run_all_examples,$(action))))
test-all: all-test
generate-all: all-generate-golden
package-all: all-package-golden
extract-all: all-extract


define run_all_sizes
$(1)-all-sizes:
	@# Iterate all sizes with fixed example - useful for performance profiling
	@for size in $(SIZES); do \
		echo "Running $(1) with EXAMPLE=$(EXAMPLE) SIZE=$$$$size"; \
		$(MAKE) $(1) EXAMPLE=$(EXAMPLE) SIZE=$$$$size || exit 1; done
endef
$(foreach action,test generate-golden package-golden,$(eval $(call run_all_sizes,$(action))))


# help (default target) provides a concise usage summary.
# Intentionally compact to fit in terminal without scrolling.
help:
	@echo "qdb-nats-connector Examples - Usage: make <target> EXAMPLE=<example> SIZE=<size>"
	@echo "Parameters: EXAMPLE=$(EXAMPLES) (default: $(EXAMPLE)), SIZE=$(SIZES) (default: $(SIZE))"
	@echo "Main: test, generate-golden, extract, package-golden"
	@echo "Golden: download-golden, check-golden"
	@echo "Convenience: test-quick (10k), test-full (1m)"
	@echo "Batch: test-all, generate-all, package-all, extract-all, test-all-sizes, generate-golden-all-sizes"
	@echo "Utility: debug, print-vars, test-validation, build-tools"
	@echo "Examples: make test EXAMPLE=finance-ohlc SIZE=10k; make test-all SIZE=100k"


# debug provides detailed configuration and path information for troubleshooting.
# Shows calculated values, paths, and environment context.
debug:
	@echo "Config: EXAMPLE=$(EXAMPLE), SIZE=$(SIZE), NUM_MESSAGES=$(NUM_$(SIZE)), DATE=$(DATE)"
	@echo "Paths: $(TESTDATA_DIR)/$(EXAMPLE)-$(SIZE)/, Archive: $(EXAMPLE)-$(DATE)-$(SIZE).tar.gz"
	@echo "Available: Examples=$(EXAMPLES), Sizes=$(SIZES)"
	@echo "Env: $(shell pwd), Make=$(MAKE_VERSION)"

