# Parameterized Makefile for qdb-nats-connector Examples

.PHONY: help test generate-golden extract package-golden print-vars test-validation build-tools debug test-quick test-full all-test all-generate-golden all-package-golden all-extract test-all generate-all package-all extract-all test-all-sizes generate-golden-all-sizes package-golden-all-sizes
.DEFAULT_GOAL := help


# =============================================================================
# Configuration
# =============================================================================

# Standardized dataset sizes, expressed directly as message counts. These exact
# counts enable quick iteration (10000), CI/CD validation (100000), full testing
# (1000000), and stress testing (10000000) with consistent performance baselines.
# NUM_MESSAGES is the single token for dataset size across the CLI, on-disk dataset
# directories, and archive names -- there is no separate short-form alias.
NUM_MESSAGES_SET := 10000 100000 1000000 10000000
DATASETS_DIR := datasets

# Dynamically discover example scripts, excluding utility scripts. This enables
# zero-maintenance addition of new examples - just drop a new .sh file and it's
# automatically included in test matrices.
EXAMPLES := $(filter-out common utils debug-industrial-sensor, $(basename $(wildcard *.sh)))
EXAMPLE ?= finance-ohlc
NUM_MESSAGES ?= 10000

# DATE is used for versioning golden data archives per ADR-007
DATE := $(shell date +%Y-%m-%d)

# Committed index of golden archives. Maps each (name, num_messages) pair to the
# generation date that forms the archive filename, plus the public base_url from
# which missing archives are fetched. Lives in examples/ (committed); the datasets/
# directory it populates is gitignored.
DATASETS_INDEX := datasets.json

# Optionally wrap `go build` in `direnv exec .` so the repo .envrc CGO env loads
# on developer machines. In CI direnv is absent and the env is provided by
# scripts/cicd/00.common.sh::cicd_setup_qdb_env, so DIRENV expands to empty there.
DIRENV := $(shell command -v direnv >/dev/null 2>&1 && echo direnv exec .)


# Parameter validation ensures early failure with clear error messages rather than
# cryptic failures deep in script execution. This saves debugging time by catching
# typos and invalid combinations at the entry point.

# Validation function for parameters
define validate_params
	@# First validation: Check if the EXAMPLE parameter matches a discovered script.
	@if [ -z "$(filter $(EXAMPLE),$(EXAMPLES))" ]; then \
		echo "Error: Invalid EXAMPLE=$(EXAMPLE)"; \
		echo "Valid examples: $(EXAMPLES)"; \
		exit 1; \
	fi
	@# Second validation: Check if NUM_MESSAGES is one of the standardized dataset sizes.
	@if [ -z "$(filter $(NUM_MESSAGES),$(NUM_MESSAGES_SET))" ]; then \
		echo "Error: Invalid NUM_MESSAGES=$(NUM_MESSAGES)"; \
		echo "Valid message counts: $(NUM_MESSAGES_SET)"; \
		exit 1; \
	fi
endef


# Fail-fast connector preflight: refuse to run if any qdb-nats-connector process is
# already alive. Detection is process-based (pgrep) so it catches leaked or orphaned
# connectors. Leaf targets call it directly; batch wrappers call it once then pass
# GUARD_SKIP=1 to their sub-makes. There is intentionally no bypass flag.
GUARD_SKIP ?=

define assert_no_connector
	@[ -n "$(GUARD_SKIP)" ] || bash -c 'source common.sh && assert_no_connector_running'
endef


# Color-coded logging for visual scanning in CI/CD logs and terminals.
COLOR_RESET := \033[0m
COLOR_GREEN := \033[32m
COLOR_BLUE := \033[34m
COLOR_YELLOW := \033[33m
COLOR_RED := \033[31m
LOG_INFO = echo "$(COLOR_BLUE)[INFO]$(COLOR_RESET) $(1)"
LOG_SUCCESS = echo "$(COLOR_GREEN)[SUCCESS]$(COLOR_RESET) $(1)"
LOG_WARNING = echo "$(COLOR_YELLOW)[WARNING]$(COLOR_RESET) $(1)"
LOG_ERROR = echo "$(COLOR_RED)[ERROR]$(COLOR_RESET) $(1)"


# run_test_workflow orchestrates the end-to-end test pipeline against pre-extracted
# golden data. The dataset directory datasets/<example>-<count>/ is the single
# source of truth: scripts read input.data and expected/ from it directly, so no
# INPUT_FILE/GOLDEN_DIR env handoff is needed. Validation (Step 8) is a hard gate.
#   $(1) = example name, $(2) = message count (NUM_MESSAGES)
define run_test_workflow
	@$(call LOG_INFO,Starting test workflow: $(1) with $(2) messages)
	@$(call LOG_INFO,Step 1/8: Creating infrastructure for $(1))
	@./$(1).sh create 2>&1 || { $(call LOG_ERROR,Failed to create infrastructure for $(1)); ./$(1).sh stop 2>/dev/null || true; exit 1; }
	@$(call LOG_INFO,Step 2/8: Ensuring golden data for $(1))
	@# Fast path: already-extracted golden data is used as-is. Otherwise download
	@# the archive (if none is present locally) and extract it; extraction creates
	@# the .extracted marker that the fast path keys on for subsequent runs.
	@if [ -f "$(DATASETS_DIR)/$(1)-$(2)/.extracted" ] && [ -d "$(DATASETS_DIR)/$(1)-$(2)/expected" ]; then \
		$(call LOG_INFO,Using extracted golden data for $(1)-$(2)); \
	else \
		found=0; \
		for f in $(DATASETS_DIR)/$(1)-*-$(2).tar.gz; do [ -e "$$f" ] && found=1; done; \
		if [ "$$found" -eq 0 ]; then \
			$(call LOG_INFO,No local golden data for $(1)-$(2); downloading); \
			$(MAKE) download-golden EXAMPLE=$(1) NUM_MESSAGES=$(2) || exit 1; \
		fi; \
		$(MAKE) extract EXAMPLE=$(1) NUM_MESSAGES=$(2) || exit 1; \
	fi
	@$(call LOG_INFO,Step 3/8: Loading data for $(1))
	@NUM_MESSAGES=$(2) ./$(1).sh load 2>&1 || { $(call LOG_ERROR,Failed to load data for $(1)); ./$(1).sh stop 2>/dev/null || true; exit 1; }
	@$(call LOG_INFO,Step 4/8: Starting connector for $(1))
	@NUM_MESSAGES=$(2) ./$(1).sh run 2>&1 || { $(call LOG_ERROR,Failed to start connector for $(1)); ./$(1).sh stop 2>/dev/null || true; exit 1; }
	@$(call LOG_INFO,Step 5/8: Waiting for processing completion for $(1))
	@NUM_MESSAGES=$(2) ./$(1).sh wait 2>&1 || { $(call LOG_ERROR,Processing failed for $(1)); ./$(1).sh stop 2>/dev/null || true; exit 1; }
	@$(call LOG_INFO,Step 6/8: Exporting results for $(1))
	@NUM_MESSAGES=$(2) ./$(1).sh export 2>&1 || { $(call LOG_ERROR,Failed to export results for $(1)); ./$(1).sh stop 2>/dev/null || true; exit 1; }
	@$(call LOG_INFO,Step 7/8: Stopping connector for $(1))
	@./$(1).sh stop 2>&1 || $(call LOG_WARNING,Failed to stop connector gracefully for $(1))
	@$(call LOG_INFO,Step 8/8: Validating results for $(1))
	@# Validation is a HARD gate: a failed or missing comparison fails the test.
	@NUM_MESSAGES=$(2) ./$(1).sh validate 2>&1 || { $(call LOG_ERROR,Validation failed for $(1)); exit 1; }
endef


# print-vars displays current parameter values for quick configuration checks
print-vars:
	@echo "EXAMPLE: $(EXAMPLE), NUM_MESSAGES: $(NUM_MESSAGES)"
	@echo "Examples: $(EXAMPLES)"; echo "Message counts: $(NUM_MESSAGES_SET)"


# test-validation verifies parameter values without running any operations.
test-validation:
	$(call validate_params)
	@echo "Validation passed!"


# build-tools ensures the Go data generation and loading utilities are compiled.
build-tools:
	@$(call LOG_INFO,Building qdb-data-gen and qdb-data-loader)
	@cd .. && $(DIRENV) go build -o bin/qdb-data-gen ./tools/generator
	@cd .. && $(DIRENV) go build -o bin/qdb-data-loader ./tools/loader
	@$(call LOG_SUCCESS,Tools built successfully)


# test runs the complete E2E validation pipeline against pre-extracted golden data.
test: build-tools
	$(call validate_params)
	@# Fail fast if any connector is already running (e.g. a leaked one from a prior run)
	$(call assert_no_connector)
	$(call run_test_workflow,$(EXAMPLE),$(NUM_MESSAGES))


# generate-golden creates a new reference dataset by running the full pipeline and
# capturing the output as the golden baseline in datasets/<example>-<count>/.
generate-golden: build-tools
	@# Validate parameters before starting the expensive golden data generation process
	$(call validate_params)
	@# Fail fast if any connector is already running (e.g. a leaked one from a prior run)
	$(call assert_no_connector)
	@$(call LOG_INFO,Generating golden data for $(EXAMPLE) with $(NUM_MESSAGES) messages)
	@NUM_MESSAGES=$(NUM_MESSAGES) ./$(EXAMPLE).sh create 2>&1 || { $(call LOG_ERROR,Failed to create infrastructure); ./$(EXAMPLE).sh stop 2>/dev/null || true; exit 1; }
	@NUM_MESSAGES=$(NUM_MESSAGES) ./$(EXAMPLE).sh generate 2>&1 || { $(call LOG_ERROR,Failed to generate data); ./$(EXAMPLE).sh stop 2>/dev/null || true; exit 1; }
	@NUM_MESSAGES=$(NUM_MESSAGES) ./$(EXAMPLE).sh load 2>&1 || { $(call LOG_ERROR,Failed to load data); ./$(EXAMPLE).sh stop 2>/dev/null || true; exit 1; }
	@NUM_MESSAGES=$(NUM_MESSAGES) ./$(EXAMPLE).sh run 2>&1 || { $(call LOG_ERROR,Failed to run connector); ./$(EXAMPLE).sh stop 2>/dev/null || true; exit 1; }
	@NUM_MESSAGES=$(NUM_MESSAGES) ./$(EXAMPLE).sh wait 2>&1 || { $(call LOG_ERROR,Failed to wait for processing); ./$(EXAMPLE).sh stop 2>/dev/null || true; exit 1; }
	@NUM_MESSAGES=$(NUM_MESSAGES) ./$(EXAMPLE).sh export 2>&1 || { $(call LOG_ERROR,Failed to export results); ./$(EXAMPLE).sh stop 2>/dev/null || true; exit 1; }
	@./$(EXAMPLE).sh stop 2>&1 || $(call LOG_WARNING,Failed to stop connector gracefully for $(EXAMPLE))
	@NUM_MESSAGES=$(NUM_MESSAGES) ./$(EXAMPLE).sh prepare-golden 2>&1 || { $(call LOG_ERROR,Failed to prepare golden data); exit 1; }
	@$(call LOG_SUCCESS,Golden data generated successfully for $(EXAMPLE)-$(NUM_MESSAGES))


# Convenience shortcuts for common test scenarios
test-quick:
	$(MAKE) test NUM_MESSAGES=10000
test-full:
	$(MAKE) test NUM_MESSAGES=1000000


# extract unpacks a previously packaged golden archive into its dataset directory.
# The archive's filename carries the generation date, so we DISCOVER it by glob
# rather than reconstructing today's date; exactly one match is required.
extract:
	$(call validate_params)
	@$(call LOG_INFO,Extracting golden data for $(EXAMPLE)-$(NUM_MESSAGES))
	@mkdir -p $(DATASETS_DIR)/$(EXAMPLE)-$(NUM_MESSAGES)
	@count=0; archive=; \
	for f in $(DATASETS_DIR)/$(EXAMPLE)-*-$(NUM_MESSAGES).tar.gz; do \
		[ -e "$$f" ] || continue; \
		count=$$((count + 1)); archive="$$f"; \
	done; \
	if [ "$$count" -eq 0 ]; then \
		$(call LOG_ERROR,No archive matching $(EXAMPLE)-*-$(NUM_MESSAGES).tar.gz in $(DATASETS_DIR)); \
		exit 1; \
	elif [ "$$count" -gt 1 ]; then \
		$(call LOG_ERROR,Multiple archives match $(EXAMPLE)-*-$(NUM_MESSAGES).tar.gz; refusing to guess:); \
		for f in $(DATASETS_DIR)/$(EXAMPLE)-*-$(NUM_MESSAGES).tar.gz; do [ -e "$$f" ] && echo "  $$f"; done; \
		exit 1; \
	else \
		tar -xzf "$$archive" -C $(DATASETS_DIR)/$(EXAMPLE)-$(NUM_MESSAGES) && \
		touch $(DATASETS_DIR)/$(EXAMPLE)-$(NUM_MESSAGES)/.extracted && \
		$(call LOG_SUCCESS,Extracted $$archive); \
	fi


# package-golden archives one dataset directory with date-versioned naming.
# Only the golden members (input.data, expected/, metadata.json) are included --
# transient actual/ output and the .extracted marker are deliberately excluded.
package-golden:
	$(call validate_params)
	@$(call LOG_INFO,Packaging golden data for $(EXAMPLE)-$(NUM_MESSAGES))
	@dataset_dir=$(DATASETS_DIR)/$(EXAMPLE)-$(NUM_MESSAGES); \
	if [ ! -f "$$dataset_dir/input.data" ] || [ ! -d "$$dataset_dir/expected" ] || [ ! -f "$$dataset_dir/metadata.json" ]; then \
		$(call LOG_ERROR,Incomplete golden dataset in $$dataset_dir (need input.data, expected/, metadata.json)); \
		echo "Run 'make generate-golden EXAMPLE=$(EXAMPLE) NUM_MESSAGES=$(NUM_MESSAGES)' first"; \
		exit 1; \
	fi; \
	tar -czf $(DATASETS_DIR)/$(EXAMPLE)-$(DATE)-$(NUM_MESSAGES).tar.gz -C "$$dataset_dir" input.data expected metadata.json && \
	$(call LOG_SUCCESS,Packaged golden data to $(EXAMPLE)-$(DATE)-$(NUM_MESSAGES).tar.gz)


# download-golden fetches one golden archive from the public bucket if it is not
# already present locally. The date component of the filename is looked up from
# datasets.json by (EXAMPLE, NUM_MESSAGES); curl writes the archive into
# DATASETS_DIR under the exact glob name that extract/check-golden expect.
# Download is idempotent: an already-present archive is left untouched.
download-golden:
	$(call validate_params)
	@bash -c 'source common.sh && require_command jq && require_command curl'
	@mkdir -p $(DATASETS_DIR)
	@date=$$(jq -r --arg name "$(EXAMPLE)" --argjson num $(NUM_MESSAGES) \
		'.archives[] | select(.name == $$name and .num_messages == $$num) | .date' $(DATASETS_INDEX)); \
	if [ -z "$$date" ] || [ "$$date" = "null" ]; then \
		$(call LOG_ERROR,No $(DATASETS_INDEX) entry for $(EXAMPLE)-$(NUM_MESSAGES)); \
		exit 1; \
	fi; \
	base_url=$$(jq -r '.base_url' $(DATASETS_INDEX)); \
	archive=$(EXAMPLE)-$$date-$(NUM_MESSAGES).tar.gz; \
	dest=$(DATASETS_DIR)/$$archive; \
	if [ -f "$$dest" ]; then \
		$(call LOG_INFO,Archive already present: $$archive); \
	else \
		$(call LOG_INFO,Downloading $$archive); \
		curl -fL -o "$$dest" "$$base_url/$$archive" || { $(call LOG_ERROR,Failed to download $$archive); rm -f "$$dest"; exit 1; }; \
		$(call LOG_SUCCESS,Downloaded $$archive); \
	fi


# check-golden reports whether golden data exists locally (extracted or archived).
check-golden:
	@$(call validate_params)
	@echo "Checking golden data for $(EXAMPLE)-$(NUM_MESSAGES)..."
	@if [ -f "$(DATASETS_DIR)/$(EXAMPLE)-$(NUM_MESSAGES)/.extracted" ]; then \
		$(call LOG_SUCCESS,Golden data available locally: $(EXAMPLE)-$(NUM_MESSAGES)); \
		echo "  Location: $(DATASETS_DIR)/$(EXAMPLE)-$(NUM_MESSAGES)/"; \
	else \
		found=0; \
		for f in $(DATASETS_DIR)/$(EXAMPLE)-*-$(NUM_MESSAGES).tar.gz; do [ -e "$$f" ] && found=1; done; \
		if [ "$$found" -eq 1 ]; then \
			$(call LOG_INFO,Golden archive found but not extracted); \
			echo "  Run: make extract EXAMPLE=$(EXAMPLE) NUM_MESSAGES=$(NUM_MESSAGES)"; \
		else \
			$(call LOG_WARNING,Golden data not found locally); \
			echo "  Run: make download-golden EXAMPLE=$(EXAMPLE) NUM_MESSAGES=$(NUM_MESSAGES)"; \
		fi; \
	fi


# Batch operations for matrix testing across all examples or all message counts.

define run_all_examples
all-$(1):
	@# Run the connector preflight once for the whole batch; sub-makes inherit GUARD_SKIP=1.
	$(call assert_no_connector)
	@# Iterate all examples with fixed message count - useful for validating parser changes
	@for example in $(EXAMPLES); do \
		echo "Running $(1) for $$$$example with NUM_MESSAGES=$(NUM_MESSAGES)"; \
		$(MAKE) $(1) EXAMPLE=$$$$example NUM_MESSAGES=$(NUM_MESSAGES) GUARD_SKIP=1 || exit 1; done
endef
$(foreach action,test generate-golden package-golden extract,$(eval $(call run_all_examples,$(action))))
test-all: all-test
generate-all: all-generate-golden
package-all: all-package-golden
extract-all: all-extract


define run_all_sizes
$(1)-all-sizes:
	@# Run the connector preflight once for the whole batch; sub-makes inherit GUARD_SKIP=1.
	$(call assert_no_connector)
	@# Iterate all message counts with fixed example - useful for performance profiling
	@for n in $(NUM_MESSAGES_SET); do \
		echo "Running $(1) with EXAMPLE=$(EXAMPLE) NUM_MESSAGES=$$$$n"; \
		$(MAKE) $(1) EXAMPLE=$(EXAMPLE) NUM_MESSAGES=$$$$n GUARD_SKIP=1 || exit 1; done
endef
$(foreach action,test generate-golden package-golden,$(eval $(call run_all_sizes,$(action))))


# help (default target) provides a concise usage summary.
help:
	@echo "qdb-nats-connector Examples - Usage: make <target> EXAMPLE=<example> NUM_MESSAGES=<count>"
	@echo "Parameters: EXAMPLE=$(EXAMPLES) (default: $(EXAMPLE)), NUM_MESSAGES=$(NUM_MESSAGES_SET) (default: $(NUM_MESSAGES))"
	@echo "Main: test, generate-golden, extract, package-golden"
	@echo "Golden: download-golden, check-golden"
	@echo "Convenience: test-quick (10000), test-full (1000000)"
	@echo "Batch: test-all, generate-all, package-all, extract-all, test-all-sizes, generate-golden-all-sizes"
	@echo "Utility: debug, print-vars, test-validation, build-tools"
	@echo "Examples: make test EXAMPLE=finance-ohlc NUM_MESSAGES=10000; make test-all NUM_MESSAGES=100000"


# debug provides detailed configuration and path information for troubleshooting.
debug:
	@echo "Config: EXAMPLE=$(EXAMPLE), NUM_MESSAGES=$(NUM_MESSAGES), DATE=$(DATE)"
	@echo "Paths: $(DATASETS_DIR)/$(EXAMPLE)-$(NUM_MESSAGES)/, Archive: $(EXAMPLE)-$(DATE)-$(NUM_MESSAGES).tar.gz"
	@echo "Available: Examples=$(EXAMPLES), Message counts=$(NUM_MESSAGES_SET)"
	@echo "Env: $(shell pwd), Make=$(MAKE_VERSION)"
