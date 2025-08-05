# Golden Data Testing Framework - Makefile Orchestration
# Provides automated workflows for testing qdb-nats-connector with modular actions

# =============================================================================
# Configuration Variables
# =============================================================================

# Test data repository configuration
BASE_URL := https://data.quasardb.net/test-data
DATE := 2025-01-22

# Test size configurations for different workloads
QUICK_SIZE := 10k
FULL_SIZE := 1m

# Available examples (currently only finance-ohlc is fully refactored)
EXAMPLES := finance-ohlc industrial-sensor network-metrics

# Directory configuration
TESTDATA_DIR := testdata
EXPORTS_DIR := exports
LOGS_DIR_VAR := logs

# Parallel execution support
MAKEFLAGS += --no-builtin-rules
.SUFFIXES:

# Tool binaries
GENERATOR := ../bin/qdb-data-gen
LOADER := ../bin/qdb-data-loader

# Default target
.DEFAULT_GOAL := help

# =============================================================================
# Utility Functions and Variables
# =============================================================================

# Colors for output formatting
COLOR_RESET := \033[0m
COLOR_BOLD := \033[1m
COLOR_GREEN := \033[32m
COLOR_BLUE := \033[34m
COLOR_YELLOW := \033[33m
COLOR_RED := \033[31m

# Helper function to check if command exists
define check_command
	@command -v $(1) >/dev/null 2>&1 || { \
		echo "$(COLOR_RED)Error: $(1) is required but not installed$(COLOR_RESET)"; \
		exit 1; \
	}
endef

# Helper macros for colored output (must be used with shell echo)
LOG_INFO = echo "$(COLOR_BLUE)[INFO]$(COLOR_RESET) $(1)"
LOG_SUCCESS = echo "$(COLOR_GREEN)[SUCCESS]$(COLOR_RESET) $(1)"
LOG_WARNING = echo "$(COLOR_YELLOW)[WARNING]$(COLOR_RESET) $(1)"
LOG_ERROR = echo "$(COLOR_RED)[ERROR]$(COLOR_RESET) $(1)"

# =============================================================================
# Help Target
# =============================================================================

.PHONY: help
help: ## Show this help message
	@echo "$(COLOR_BOLD)Golden Data Testing Framework$(COLOR_RESET)"
	@echo "Automated workflows for testing qdb-nats-connector"
	@echo ""
	@echo "$(COLOR_BOLD)Usage:$(COLOR_RESET)"
	@echo "  make <target>"
	@echo ""
	@echo "$(COLOR_BOLD)Test Workflows:$(COLOR_RESET)"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  $(COLOR_GREEN)%-25s$(COLOR_RESET) %s\n", $$1, $$2}' $(MAKEFILE_LIST) | grep -E "(test-|generate-)" | sort
	@echo ""
	@echo "$(COLOR_BOLD)Data Management:$(COLOR_RESET)"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  $(COLOR_GREEN)%-25s$(COLOR_RESET) %s\n", $$1, $$2}' $(MAKEFILE_LIST) | grep -E "(download-|extract-)" | sort
	@echo ""
	@echo "$(COLOR_BOLD)Utilities:$(COLOR_RESET)"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  $(COLOR_GREEN)%-25s$(COLOR_RESET) %s\n", $$1, $$2}' $(MAKEFILE_LIST) | grep -vE "(test-|generate-|download-|extract-)" | sort
	@echo ""
	@echo "$(COLOR_BOLD)Examples:$(COLOR_RESET)"
	@echo "  make test-quick                    # Run all quick tests (10k records)"
	@echo "  make test-full                     # Run all full tests (1M records)"
	@echo "  make test-quick-finance-ohlc       # Run single quick test"
	@echo "  make -j4 test-quick               # Run tests in parallel"
	@echo "  make generate-golden-finance-ohlc-10k  # Generate golden data"
	@echo ""
	@echo "$(COLOR_BOLD)Available Examples:$(COLOR_RESET) $(EXAMPLES)"

# =============================================================================
# Build Tools
# =============================================================================

# Build required Go tools
.PHONY: build-tools
build-tools: ## Build required Go tools (qdb-data-gen and qdb-data-loader)
	@$(call LOG_INFO,Building qdb-data-gen and qdb-data-loader)
	@cd .. && direnv exec . go build -o bin/qdb-data-gen ./tools/generator
	@cd .. && direnv exec . go build -o bin/qdb-data-loader ./tools/loader
	@$(call LOG_SUCCESS,Tools built successfully)

# =============================================================================
# Directory Creation
# =============================================================================

$(TESTDATA_DIR) $(EXPORTS_DIR) $(LOGS_DIR):
	@mkdir -p $@

# =============================================================================
# Download Rules for Test Data Packages
# =============================================================================

# Pattern rule for downloading test data packages
$(TESTDATA_DIR)/%-$(DATE)-$(QUICK_SIZE).tar.gz: | $(TESTDATA_DIR)
	@$(call LOG_INFO,Downloading quick test data: $(@F))
	@curl -L --progress-bar --fail --create-dirs -o $@ $(BASE_URL)/$(@F) || { \
		$(call LOG_WARNING,Test data package not available: $(@F)); \
		$(call LOG_INFO,This is expected for examples not yet published); \
		touch $@; \
	}

$(TESTDATA_DIR)/%-$(DATE)-$(FULL_SIZE).tar.gz: | $(TESTDATA_DIR)
	@$(call LOG_INFO,Downloading full test data: $(@F))
	@curl -L --progress-bar --fail --create-dirs -o $@ $(BASE_URL)/$(@F) || { \
		$(call LOG_WARNING,Test data package not available: $(@F)); \
		$(call LOG_INFO,This is expected for examples not yet published); \
		touch $@; \
	}

# =============================================================================
# Extraction Rules with Marker Files
# =============================================================================

# Pattern rule for extracting quick test data
$(TESTDATA_DIR)/%-$(QUICK_SIZE)/.extracted: $(TESTDATA_DIR)/%-$(DATE)-$(QUICK_SIZE).tar.gz
	@$(call LOG_INFO,Extracting quick test data: $*)
	@if [ -s $< ]; then \
		mkdir -p $(@D); \
		tar -xzf $< -C $(@D) --strip-components=1 2>/dev/null || { \
			$(call LOG_WARNING,Failed to extract $<, may be empty placeholder); \
		}; \
	else \
		mkdir -p $(@D); \
		$(call LOG_INFO,Using generated data instead of extracted data for $*); \
	fi
	@touch $@

# Pattern rule for extracting full test data
$(TESTDATA_DIR)/%-$(FULL_SIZE)/.extracted: $(TESTDATA_DIR)/%-$(DATE)-$(FULL_SIZE).tar.gz
	@$(call LOG_INFO,Extracting full test data: $*)
	@if [ -s $< ]; then \
		mkdir -p $(@D); \
		tar -xzf $< -C $(@D) --strip-components=1 2>/dev/null || { \
			$(call LOG_WARNING,Failed to extract $<, may be empty placeholder); \
		}; \
	else \
		mkdir -p $(@D); \
		$(call LOG_INFO,Using generated data instead of extracted data for $*); \
	fi
	@touch $@

# =============================================================================
# Test Workflow Implementation
# =============================================================================

# Define the 8-step test workflow as a reusable function
# Usage: $(call run_test_workflow,example,size,num_messages)
define run_test_workflow
	@$(call LOG_INFO,Starting test workflow: $(1) with $(3) messages)

	# Always ensure cleanup on any failure
	@trap './$(1).sh stop 2>/dev/null || true' EXIT

	# Step 1: Create infrastructure
	@$(call LOG_INFO,Step 1/8: Creating infrastructure for $(1))
	@./$(1).sh create 2>&1 || { $(call LOG_ERROR,Failed to create infrastructure for $(1)); ./$(1).sh stop 2>/dev/null || true; exit 1; }

	# Step 2: Generate test data (skip if using extracted data)
	@$(call LOG_INFO,Step 2/8: Preparing test data for $(1))
	@if [ -f "$(TESTDATA_DIR)/$(1)-$(2)/.extracted" ] && [ -d "$(TESTDATA_DIR)/$(1)-$(2)/golden" ]; then \
		$(call LOG_INFO,Using extracted test data for $(1)); \
		export TESTDATA_DIR="$(TESTDATA_DIR)/$(1)-$(2)"; \
	else \
		$(call LOG_INFO,Generating fresh test data for $(1)); \
		NUM_MESSAGES=$(3) ./$(1).sh generate 2>&1 || { $(call LOG_ERROR,Failed to generate data for $(1)); ./$(1).sh stop 2>/dev/null || true; exit 1; }; \
	fi

	# Step 3: Load data into NATS
	@$(call LOG_INFO,Step 3/8: Loading data for $(1))
	@NUM_MESSAGES=$(3) ./$(1).sh load 2>&1 || { $(call LOG_ERROR,Failed to load data for $(1)); ./$(1).sh stop 2>/dev/null || true; exit 1; }

	# Step 4: Start connector
	@$(call LOG_INFO,Step 4/8: Starting connector for $(1))
	@NUM_MESSAGES=$(3) ./$(1).sh run 2>&1 || { $(call LOG_ERROR,Failed to start connector for $(1)); ./$(1).sh stop 2>/dev/null || true; exit 1; }

	# Step 5: Wait for processing completion
	@$(call LOG_INFO,Step 5/8: Waiting for processing completion for $(1))
	@NUM_MESSAGES=$(3) ./$(1).sh wait 2>&1 || { $(call LOG_ERROR,Processing failed for $(1)); ./$(1).sh stop 2>/dev/null || true; exit 1; }

	# Step 6: Export results
	@$(call LOG_INFO,Step 6/8: Exporting results for $(1))
	@./$(1).sh export 2>&1 || { $(call LOG_ERROR,Failed to export results for $(1)); ./$(1).sh stop 2>/dev/null || true; exit 1; }

	# Step 7: Stop connector
	@$(call LOG_INFO,Step 7/8: Stopping connector for $(1))
	@./$(1).sh stop 2>&1 || $(call LOG_WARNING,Failed to stop connector gracefully for $(1))

	# Step 8: Validate results (if golden data exists)
	@$(call LOG_INFO,Step 8/8: Validating results for $(1))
	@if [ -f "$(TESTDATA_DIR)/$(1)-$(2)/.extracted" ] && [ -d "$(TESTDATA_DIR)/$(1)-$(2)/golden" ]; then \
		$(call LOG_INFO,Validating against golden data for $(1)); \
		TESTDATA_DIR="$(TESTDATA_DIR)/$(1)-$(2)" ./$(1).sh validate || { \
			$(call LOG_ERROR,Validation failed for $(1)); exit 1; \
		}; \
		$(call LOG_SUCCESS,Validation passed for $(1)); \
	else \
		$(call LOG_INFO,Skipping validation for $(1) - no golden data available); \
	fi

	@$(call LOG_SUCCESS,Test workflow completed successfully: $(1))
endef

# =============================================================================
# Individual Test Targets
# =============================================================================

# Quick test targets (10k records)
.PHONY: test-quick-finance-ohlc
test-quick-finance-ohlc: build-tools $(TESTDATA_DIR)/finance-ohlc-$(QUICK_SIZE)/.extracted ## Run finance-ohlc quick test (10k records)
	$(call run_test_workflow,finance-ohlc,$(QUICK_SIZE),10000)

.PHONY: test-quick-industrial-sensor
test-quick-industrial-sensor: build-tools $(TESTDATA_DIR)/industrial-sensor-$(QUICK_SIZE)/.extracted ## Run industrial-sensor quick test (10k records)
	$(call run_test_workflow,industrial-sensor,$(QUICK_SIZE),10000)

.PHONY: test-quick-network-metrics
test-quick-network-metrics: build-tools $(TESTDATA_DIR)/network-metrics-$(QUICK_SIZE)/.extracted ## Run network-metrics quick test (10k records)
	$(call run_test_workflow,network-metrics,$(QUICK_SIZE),10000)

# Full test targets (1M records)
.PHONY: test-full-finance-ohlc
test-full-finance-ohlc: build-tools $(TESTDATA_DIR)/finance-ohlc-$(FULL_SIZE)/.extracted ## Run finance-ohlc full test (1M records)
	$(call run_test_workflow,finance-ohlc,$(FULL_SIZE),1000000)

.PHONY: test-full-industrial-sensor
test-full-industrial-sensor: build-tools $(TESTDATA_DIR)/industrial-sensor-$(FULL_SIZE)/.extracted ## Run industrial-sensor full test (1M records)
	$(call run_test_workflow,industrial-sensor,$(FULL_SIZE),1000000)

.PHONY: test-full-network-metrics
test-full-network-metrics: build-tools $(TESTDATA_DIR)/network-metrics-$(FULL_SIZE)/.extracted ## Run network-metrics full test (1M records)
	$(call run_test_workflow,network-metrics,$(FULL_SIZE),1000000)

# =============================================================================
# Aggregate Test Targets
# =============================================================================

# Quick tests for all examples
.PHONY: test-quick
test-quick: ## Run all quick tests (10k records)
	@$(call LOG_INFO,Running all quick tests in sequence)
	@$(MAKE) test-quick-finance-ohlc
	@$(MAKE) test-quick-industrial-sensor
	@$(MAKE) test-quick-network-metrics
	@$(call LOG_SUCCESS,All quick tests completed)

# Full tests for all examples
.PHONY: test-full
test-full: ## Run all full tests (1M records)
	@$(call LOG_INFO,Running all full tests in sequence)
	@$(MAKE) test-full-finance-ohlc
	@$(MAKE) test-full-industrial-sensor
	@$(MAKE) test-full-network-metrics
	@$(call LOG_SUCCESS,All full tests completed)

# Parallel quick tests
.PHONY: test-quick-parallel
test-quick-parallel: ## Run all quick tests in parallel
	@$(call LOG_INFO,Running all quick tests in parallel)
	@$(MAKE) -j$(shell nproc 2>/dev/null || echo 4) test-quick-finance-ohlc test-quick-industrial-sensor test-quick-network-metrics
	@$(call LOG_SUCCESS,All parallel quick tests completed)

# =============================================================================
# Golden Data Generation Targets
# =============================================================================

# Generate golden data for finance-ohlc with different sizes
.PHONY: generate-golden-finance-ohlc-10k
generate-golden-finance-ohlc-10k: build-tools ## Generate golden data for finance-ohlc (10k records)
	@$(call LOG_INFO,Generating golden data: finance-ohlc 10k records)
	@NUM_MESSAGES=10000 ./finance-ohlc.sh create || { $(call LOG_ERROR,Failed at create step); exit 1; }
	@NUM_MESSAGES=10000 ./finance-ohlc.sh generate || { $(call LOG_ERROR,Failed at generate step); exit 1; }
	@NUM_MESSAGES=10000 ./finance-ohlc.sh load || { $(call LOG_ERROR,Failed at load step); exit 1; }
	@NUM_MESSAGES=10000 ./finance-ohlc.sh run || { $(call LOG_ERROR,Failed at run step); exit 1; }
	@NUM_MESSAGES=10000 ./finance-ohlc.sh wait || { $(call LOG_ERROR,Failed at wait step); exit 1; }
	@./finance-ohlc.sh export || { $(call LOG_ERROR,Failed at export step); exit 1; }
	@./finance-ohlc.sh stop || { $(call LOG_ERROR,Failed at stop step); exit 1; }
	@./finance-ohlc.sh prepare-golden || { $(call LOG_ERROR,Failed at prepare-golden step); exit 1; }
	@$(call LOG_SUCCESS,Golden data generated: finance-ohlc 10k records)

.PHONY: generate-golden-finance-ohlc-1m
generate-golden-finance-ohlc-1m: build-tools ## Generate golden data for finance-ohlc (1M records)
	@$(call LOG_INFO,Generating golden data: finance-ohlc 1M records)
	@NUM_MESSAGES=1000000 ./finance-ohlc.sh create || { $(call LOG_ERROR,Failed at create step); exit 1; }
	@NUM_MESSAGES=1000000 ./finance-ohlc.sh generate || { $(call LOG_ERROR,Failed at generate step); exit 1; }
	@NUM_MESSAGES=1000000 ./finance-ohlc.sh load || { $(call LOG_ERROR,Failed at load step); exit 1; }
	@NUM_MESSAGES=1000000 ./finance-ohlc.sh run || { $(call LOG_ERROR,Failed at run step); exit 1; }
	@NUM_MESSAGES=1000000 ./finance-ohlc.sh wait || { $(call LOG_ERROR,Failed at wait step); exit 1; }
	@./finance-ohlc.sh export || { $(call LOG_ERROR,Failed at export step); exit 1; }
	@./finance-ohlc.sh stop || { $(call LOG_ERROR,Failed at stop step); exit 1; }
	@./finance-ohlc.sh prepare-golden || { $(call LOG_ERROR,Failed at prepare-golden step); exit 1; }
	@$(call LOG_SUCCESS,Golden data generated: finance-ohlc 1M records)

# Package golden data for distribution
.PHONY: package-golden-finance-ohlc-10k
package-golden-finance-ohlc-10k: generate-golden-finance-ohlc-10k ## Package golden data for distribution (10k)
	@$(call LOG_INFO,Packaging golden data: finance-ohlc 10k)
	@cd testdata && tar -czf finance-ohlc-$(DATE)-$(QUICK_SIZE).tar.gz golden/
	@$(call LOG_SUCCESS,Golden data packaged: testdata/finance-ohlc-$(DATE)-$(QUICK_SIZE).tar.gz)

.PHONY: package-golden-finance-ohlc-1m
package-golden-finance-ohlc-1m: generate-golden-finance-ohlc-1m ## Package golden data for distribution (1M)
	@$(call LOG_INFO,Packaging golden data: finance-ohlc 1M)
	@cd testdata && tar -czf finance-ohlc-$(DATE)-$(FULL_SIZE).tar.gz golden/
	@$(call LOG_SUCCESS,Golden data packaged: testdata/finance-ohlc-$(DATE)-$(FULL_SIZE).tar.gz)

# Generate golden data for industrial-sensor with different sizes
.PHONY: generate-golden-industrial-sensor-10k
generate-golden-industrial-sensor-10k: build-tools ## Generate golden data for industrial-sensor (10k records)
	@$(call LOG_INFO,Generating golden data: industrial-sensor 10k records)
	@NUM_MESSAGES=10000 ./industrial-sensor.sh create || { $(call LOG_ERROR,Failed at create step); exit 1; }
	@NUM_MESSAGES=10000 ./industrial-sensor.sh generate || { $(call LOG_ERROR,Failed at generate step); exit 1; }
	@NUM_MESSAGES=10000 ./industrial-sensor.sh load || { $(call LOG_ERROR,Failed at load step); exit 1; }
	@NUM_MESSAGES=10000 ./industrial-sensor.sh run || { $(call LOG_ERROR,Failed at run step); exit 1; }
	@NUM_MESSAGES=10000 ./industrial-sensor.sh wait || { $(call LOG_ERROR,Failed at wait step); exit 1; }
	@./industrial-sensor.sh export || { $(call LOG_ERROR,Failed at export step); exit 1; }
	@./industrial-sensor.sh stop || { $(call LOG_ERROR,Failed at stop step); exit 1; }
	@./industrial-sensor.sh prepare-golden || { $(call LOG_ERROR,Failed at prepare-golden step); exit 1; }
	@$(call LOG_SUCCESS,Golden data generated: industrial-sensor 10k records)

.PHONY: generate-golden-industrial-sensor-1m
generate-golden-industrial-sensor-1m: build-tools ## Generate golden data for industrial-sensor (1M records)
	@$(call LOG_INFO,Generating golden data: industrial-sensor 1M records)
	@NUM_MESSAGES=1000000 ./industrial-sensor.sh create || { $(call LOG_ERROR,Failed at create step); exit 1; }
	@NUM_MESSAGES=1000000 ./industrial-sensor.sh generate || { $(call LOG_ERROR,Failed at generate step); exit 1; }
	@NUM_MESSAGES=1000000 ./industrial-sensor.sh load || { $(call LOG_ERROR,Failed at load step); exit 1; }
	@NUM_MESSAGES=1000000 ./industrial-sensor.sh run || { $(call LOG_ERROR,Failed at run step); exit 1; }
	@NUM_MESSAGES=1000000 ./industrial-sensor.sh wait || { $(call LOG_ERROR,Failed at wait step); exit 1; }
	@./industrial-sensor.sh export || { $(call LOG_ERROR,Failed at export step); exit 1; }
	@./industrial-sensor.sh stop || { $(call LOG_ERROR,Failed at stop step); exit 1; }
	@./industrial-sensor.sh prepare-golden || { $(call LOG_ERROR,Failed at prepare-golden step); exit 1; }
	@$(call LOG_SUCCESS,Golden data generated: industrial-sensor 1M records)

# Package golden data for distribution
.PHONY: package-golden-industrial-sensor-10k
package-golden-industrial-sensor-10k: generate-golden-industrial-sensor-10k ## Package golden data for distribution (10k)
	@$(call LOG_INFO,Packaging golden data: industrial-sensor 10k)
	@cd testdata && tar -czf industrial-sensor-$(DATE)-$(QUICK_SIZE).tar.gz golden/
	@$(call LOG_SUCCESS,Golden data packaged: testdata/industrial-sensor-$(DATE)-$(QUICK_SIZE).tar.gz)

.PHONY: package-golden-industrial-sensor-1m
package-golden-industrial-sensor-1m: generate-golden-industrial-sensor-1m ## Package golden data for distribution (1M)
	@$(call LOG_INFO,Packaging golden data: industrial-sensor 1M)
	@cd testdata && tar -czf industrial-sensor-$(DATE)-$(FULL_SIZE).tar.gz golden/
	@$(call LOG_SUCCESS,Golden data packaged: testdata/industrial-sensor-$(DATE)-$(FULL_SIZE).tar.gz)

# Generate golden data for network-metrics with different sizes
.PHONY: generate-golden-network-metrics-10k
generate-golden-network-metrics-10k: build-tools ## Generate golden data for network-metrics (10k records)
	@$(call LOG_INFO,Generating golden data: network-metrics 10k records)
	@NUM_MESSAGES=10000 ./network-metrics.sh create || { $(call LOG_ERROR,Failed at create step); exit 1; }
	@NUM_MESSAGES=10000 ./network-metrics.sh generate || { $(call LOG_ERROR,Failed at generate step); exit 1; }
	@NUM_MESSAGES=10000 ./network-metrics.sh load || { $(call LOG_ERROR,Failed at load step); exit 1; }
	@NUM_MESSAGES=10000 ./network-metrics.sh run || { $(call LOG_ERROR,Failed at run step); exit 1; }
	@NUM_MESSAGES=10000 ./network-metrics.sh wait || { $(call LOG_ERROR,Failed at wait step); exit 1; }
	@./network-metrics.sh export || { $(call LOG_ERROR,Failed at export step); exit 1; }
	@./network-metrics.sh stop || { $(call LOG_ERROR,Failed at stop step); exit 1; }
	@./network-metrics.sh prepare-golden || { $(call LOG_ERROR,Failed at prepare-golden step); exit 1; }
	@$(call LOG_SUCCESS,Golden data generated: network-metrics 10k records)

.PHONY: generate-golden-network-metrics-1m
generate-golden-network-metrics-1m: build-tools ## Generate golden data for network-metrics (1M records)
	@$(call LOG_INFO,Generating golden data: network-metrics 1M records)
	@NUM_MESSAGES=1000000 ./network-metrics.sh create || { $(call LOG_ERROR,Failed at create step); exit 1; }
	@NUM_MESSAGES=1000000 ./network-metrics.sh generate || { $(call LOG_ERROR,Failed at generate step); exit 1; }
	@NUM_MESSAGES=1000000 ./network-metrics.sh load || { $(call LOG_ERROR,Failed at load step); exit 1; }
	@NUM_MESSAGES=1000000 ./network-metrics.sh run || { $(call LOG_ERROR,Failed at run step); exit 1; }
	@NUM_MESSAGES=1000000 ./network-metrics.sh wait || { $(call LOG_ERROR,Failed at wait step); exit 1; }
	@./network-metrics.sh export || { $(call LOG_ERROR,Failed at export step); exit 1; }
	@./network-metrics.sh stop || { $(call LOG_ERROR,Failed at stop step); exit 1; }
	@./network-metrics.sh prepare-golden || { $(call LOG_ERROR,Failed at prepare-golden step); exit 1; }
	@$(call LOG_SUCCESS,Golden data generated: network-metrics 1M records)

# Package golden data for distribution
.PHONY: package-golden-network-metrics-10k
package-golden-network-metrics-10k: generate-golden-network-metrics-10k ## Package golden data for distribution (10k)
	@$(call LOG_INFO,Packaging golden data: network-metrics 10k)
	@cd testdata && tar -czf network-metrics-$(DATE)-$(QUICK_SIZE).tar.gz golden/
	@$(call LOG_SUCCESS,Golden data packaged: testdata/network-metrics-$(DATE)-$(QUICK_SIZE).tar.gz)

.PHONY: package-golden-network-metrics-1m
package-golden-network-metrics-1m: generate-golden-network-metrics-1m ## Package golden data for distribution (1M)
	@$(call LOG_INFO,Packaging golden data: network-metrics 1M)
	@cd testdata && tar -czf network-metrics-$(DATE)-$(FULL_SIZE).tar.gz golden/
	@$(call LOG_SUCCESS,Golden data packaged: testdata/network-metrics-$(DATE)-$(FULL_SIZE).tar.gz)

# Aggregate golden data generation targets
.PHONY: generate-golden-all-10k
generate-golden-all-10k: ## Generate golden data for all examples (10k records)
	@$(call LOG_INFO,Generating golden data for all examples: 10k records)
	@$(MAKE) generate-golden-finance-ohlc-10k
	@$(MAKE) generate-golden-industrial-sensor-10k
	@$(MAKE) generate-golden-network-metrics-10k
	@$(call LOG_SUCCESS,All golden data generated: 10k records)

.PHONY: generate-golden-all-1m
generate-golden-all-1m: ## Generate golden data for all examples (1M records)
	@$(call LOG_INFO,Generating golden data for all examples: 1M records)
	@$(MAKE) generate-golden-finance-ohlc-1m
	@$(MAKE) generate-golden-industrial-sensor-1m
	@$(MAKE) generate-golden-network-metrics-1m
	@$(call LOG_SUCCESS,All golden data generated: 1M records)

.PHONY: package-golden-all-10k
package-golden-all-10k: ## Package golden data for all examples (10k records)
	@$(call LOG_INFO,Packaging golden data for all examples: 10k records)
	@$(MAKE) package-golden-finance-ohlc-10k
	@$(MAKE) package-golden-industrial-sensor-10k
	@$(MAKE) package-golden-network-metrics-10k
	@$(call LOG_SUCCESS,All golden data packaged: 10k records)

.PHONY: package-golden-all-1m
package-golden-all-1m: ## Package golden data for all examples (1M records)
	@$(call LOG_INFO,Packaging golden data for all examples: 1M records)
	@$(MAKE) package-golden-finance-ohlc-1m
	@$(MAKE) package-golden-industrial-sensor-1m
	@$(MAKE) package-golden-network-metrics-1m
	@$(call LOG_SUCCESS,All golden data packaged: 1M records)

# =============================================================================
# Data Management Targets
# =============================================================================

# Download targets
.PHONY: download-quick
download-quick: $(addprefix $(TESTDATA_DIR)/,$(addsuffix -$(DATE)-$(QUICK_SIZE).tar.gz,$(EXAMPLES))) ## Download all quick test data packages

.PHONY: download-full
download-full: $(addprefix $(TESTDATA_DIR)/,$(addsuffix -$(DATE)-$(FULL_SIZE).tar.gz,$(EXAMPLES))) ## Download all full test data packages

.PHONY: download-all
download-all: download-quick download-full ## Download all test data packages

# Extract targets
.PHONY: extract-quick
extract-quick: $(addprefix $(TESTDATA_DIR)/,$(addsuffix -$(QUICK_SIZE)/.extracted,$(EXAMPLES))) ## Extract all quick test data

.PHONY: extract-full
extract-full: $(addprefix $(TESTDATA_DIR)/,$(addsuffix -$(FULL_SIZE)/.extracted,$(EXAMPLES))) ## Extract all full test data

.PHONY: extract-all
extract-all: extract-quick extract-full ## Extract all test data

# =============================================================================
# Cleanup Targets
# =============================================================================

.PHONY: clean
clean: ## Remove generated files but keep downloads
	@$(call LOG_INFO,Cleaning generated files...)
	@rm -rf $(EXPORTS_DIR) $(LOGS_DIR_VAR)
	@rm -f *.log *.pid *.jsonl
	@find $(TESTDATA_DIR) -name ".extracted" -delete 2>/dev/null || true
	@find . -name "exports" -type d -exec rm -rf {} + 2>/dev/null || true
	@$(call LOG_SUCCESS,Generated files cleaned)

.PHONY: clean-testdata
clean-testdata: ## Remove extracted test data but keep downloads
	@$(call LOG_INFO,Cleaning extracted test data...)
	@find $(TESTDATA_DIR) -type d -name "*-$(QUICK_SIZE)" -exec rm -rf {} + 2>/dev/null || true
	@find $(TESTDATA_DIR) -type d -name "*-$(FULL_SIZE)" -exec rm -rf {} + 2>/dev/null || true
	@$(call LOG_SUCCESS,Extracted test data cleaned)

.PHONY: clean-all
clean-all: clean clean-testdata ## Remove everything including downloads
	@$(call LOG_INFO,Cleaning all files including downloads...)
	@rm -rf $(TESTDATA_DIR)
	@$(call LOG_SUCCESS,All files cleaned)

# =============================================================================
# Development and Debugging Targets
# =============================================================================

.PHONY: check-deps
check-deps: ## Check if required dependencies are installed
	@$(call LOG_INFO,Checking dependencies...)
	$(call check_command,curl)
	$(call check_command,tar)
	$(call check_command,nats)
	$(call check_command,qdbsh)
	@command -v numdiff >/dev/null 2>&1 || $(call LOG_WARNING,numdiff not found - validation will be skipped)
	@command -v jq >/dev/null 2>&1 || $(call LOG_WARNING,jq not found - metadata generation may fail)
	@$(call LOG_SUCCESS,Dependency check completed)

.PHONY: status
status: ## Show current status of test data and processes
	@$(call LOG_INFO,Test data status:)
	@echo "Downloads:"
	@ls -la $(TESTDATA_DIR)/*.tar.gz 2>/dev/null || echo "  No downloads found"
	@echo "Extracted:"
	@find $(TESTDATA_DIR) -name ".extracted" 2>/dev/null | sed 's|^|  |' || echo "  No extracted data found"
	@echo "Exports:"
	@ls -la */exports/ 2>/dev/null | head -10 || echo "  No exports found"

.PHONY: logs
logs: ## Show recent connector logs
	@find . -name "*connector.log" -exec echo "=== {} ===" \; -exec tail -20 {} \; 2>/dev/null || echo "No connector logs found"

.PHONY: diagnose
diagnose: ## Run comprehensive diagnostics for troubleshooting
	@$(call LOG_INFO,Running diagnostics...)
	@echo ""
	@echo "=== Environment ==="
	@echo "QDB_URI: ${QDB_URI:-not set}"
	@echo "NATS_URL: ${NATS_URL:-not set}"
	@echo "NUM_MESSAGES: ${NUM_MESSAGES:-not set}"
	@echo "DEBUG: ${DEBUG:-not set}"
	@echo ""
	@echo "=== Service Connectivity ==="
	@timeout 2 nc -zv localhost 4222 2>&1 | grep -E "(succeeded|refused)" || echo "NATS: Unable to connect"
	@timeout 2 nc -zv localhost 2836 2>&1 | grep -E "(succeeded|refused)" || echo "QuasarDB: Unable to connect"
	@echo ""
	@echo "=== Running Processes ==="
	@ps aux | grep -E "(qdb-nats-connector)" | grep -v grep || echo "No connector processes found"
	@echo ""
	@echo "=== Recent Errors ==="
	@find . -name "*connector.log" -exec grep -i "error" {} \; 2>/dev/null | tail -10 || echo "No errors found in logs"
	@echo ""
	@echo "=== File Status ==="
	@echo "PID files: $(shell find . -name "*.pid" 2>/dev/null | wc -l)"
	@echo "Log files: $(shell find . -name "*.log" 2>/dev/null | wc -l)"
	@echo "Data files: $(shell find . -name "*.jsonl" 2>/dev/null | wc -l)"
	@$(call LOG_SUCCESS,Diagnostics completed)

# =============================================================================
# Pattern Rules (for advanced usage)
# =============================================================================

# Generic pattern for running any test with custom size
test-%-quick: $(TESTDATA_DIR)/%-$(QUICK_SIZE)/.extracted
	@if [ -f "$*.sh" ]; then \
		$(call run_test_workflow,$*,$(QUICK_SIZE),10000); \
	else \
		$(call LOG_ERROR,$*.sh not found or not yet refactored); \
		exit 1; \
	fi

test-%-full: $(TESTDATA_DIR)/%-$(FULL_SIZE)/.extracted
	@if [ -f "$*.sh" ]; then \
		$(call run_test_workflow,$*,$(FULL_SIZE),1000000); \
	else \
		$(call LOG_ERROR,$*.sh not found or not yet refactored); \
		exit 1; \
	fi

# =============================================================================
# Special Targets
# =============================================================================

# Prevent Make from removing intermediate files
.PRECIOUS: $(TESTDATA_DIR)/%-$(DATE)-$(QUICK_SIZE).tar.gz
.PRECIOUS: $(TESTDATA_DIR)/%-$(DATE)-$(FULL_SIZE).tar.gz
.PRECIOUS: $(TESTDATA_DIR)/%-$(QUICK_SIZE)/.extracted
.PRECIOUS: $(TESTDATA_DIR)/%-$(FULL_SIZE)/.extracted

# Mark phony targets
.PHONY: all clean clean-all clean-testdata download-all download-quick download-full
.PHONY: extract-all extract-quick extract-full help test-quick test-full
.PHONY: check-deps status logs
