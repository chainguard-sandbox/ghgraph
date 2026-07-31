# Makefile for ghgraph
# Self-documenting: run `make` or `make help` to see available targets.
#
# ghgraph is a design scaffold — function bodies are `todo!()` stubs, so
# `build` compiles but `run` will panic until the bodies land. See DESIGN.md.

.PHONY: help doctor config build release run test fuzz mutants fmt lint check check-full audit clean install setup setup-vet

BINARY_NAME := ghgraph

# Fuzzing knobs (see the `fuzz` target).
TARGET ?= config_gate
SECS ?= 60

# Mutation-testing knobs (see the `mutants` target). Scoped to the implemented
# modules by default — mutating the todo!() stubs only yields false survivors;
# widen MUTANTS_FILES as modules land.
# JOBS is deliberately modest: each job is a full build tree plus a test
# suite, and a mutant that breaks a loop's progress can allocate at memory-
# bandwidth speed until TIMEOUT kills it — 4 concurrent runaways have OOMed
# a 16GB machine. Loop-bearing code should also carry a progress
# debug_assert (see gh::scrub_tokens) so that class dies by panic instead.
MUTANTS_FILES ?= --file src/db.rs --file src/config.rs --file src/time.rs --file src/identity.rs --file src/queries.rs --file src/parse.rs --file src/gh.rs
JOBS ?= 2
TIMEOUT ?= 60

help: ## Show this help
	@echo "$(BINARY_NAME) — make <target>"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

#
# Getting started
#

doctor: ## Check prerequisites: gh CLI (authenticated) and the Rust toolchain
	@command -v cargo >/dev/null 2>&1 && echo "✓ cargo $$(cargo --version | cut -d' ' -f2)" || { echo "✗ cargo not found — https://rustup.rs"; exit 1; }
	@command -v gh >/dev/null 2>&1 && echo "✓ gh $$(gh --version | head -1 | cut -d' ' -f3)" || { echo "✗ gh not found — ghgraph's only transport (https://cli.github.com)"; exit 1; }
	@gh auth status >/dev/null 2>&1 && echo "✓ gh authenticated" || { echo "✗ gh not authenticated — run: gh auth login"; exit 1; }
	@echo "✓ ready"

config: ## Write a starter config to $XDG_CONFIG_HOME/ghgraph/ (never overwrites)
	@dest="$${XDG_CONFIG_HOME:-$$HOME/.config}/ghgraph/config.json"; \
	if [ -e "$$dest" ]; then echo "exists, not overwriting: $$dest"; \
	else mkdir -p "$$(dirname "$$dest")" && cp config.example.json "$$dest" && echo "wrote $$dest — edit it, then: ghgraph sync"; fi

#
# Build and run
#

build: ## Build (debug)
	cargo build

release: ## Build (optimized)
	cargo build --release

run: ## Run ghgraph — pass args with ARGS, e.g. make run ARGS="attention"
	@cargo run -- $(ARGS)

install: ## Install the binary into ~/.cargo/bin
	cargo install --path .

#
# Quality — `make check` is the pre-commit gate
#

fmt: ## Format the source
	cargo fmt --all

lint: ## Clippy, warnings as errors
	cargo clippy --all-targets -- -D warnings

test: ## Run the test suite
	cargo test

fuzz: ## Fuzz a target (out-of-build, nightly). TARGET=config_gate SECS=60
	@command -v cargo-fuzz >/dev/null 2>&1 || { echo "cargo-fuzz not found — run: cargo install cargo-fuzz"; exit 1; }
	@nb="$$(dirname "$$(rustup which --toolchain nightly cargo)")"; \
	echo "fuzzing $(TARGET) for $(SECS)s on nightly…"; \
	PATH="$$nb:$$HOME/.cargo/bin:$$PATH" cargo fuzz run $(TARGET) -- -max_total_time=$(SECS)

mutants: ## Mutation-test the implemented modules (needs cargo-mutants). MUTANTS_FILES/JOBS/TIMEOUT
	@command -v cargo-mutants >/dev/null 2>&1 || { echo "cargo-mutants not found — run: cargo install cargo-mutants"; exit 1; }
	cargo mutants $(MUTANTS_FILES) --jobs $(JOBS) --timeout $(TIMEOUT)

check: ## Fast pre-commit gate: format, clippy, check, test
	cargo fmt --all -- --check
	cargo clippy --all-targets -- -D warnings
	cargo check --all-targets
	cargo test
	@echo "✓ all checks passed"

check-full: check audit ## check, plus the dependency advisory scan

#
# Supply chain (dependency policy — see DESIGN.md)
#

audit: ## Scan dependencies for known advisories (needs cargo-audit; make setup)
	@command -v cargo-audit >/dev/null 2>&1 || { echo "cargo-audit not found — run 'make setup' (or: cargo install cargo-audit)"; exit 1; }
	cargo audit

setup: ## Install the dev tools the quality targets need (cargo-audit)
	cargo install cargo-audit

setup-vet: ## Initialize the cargo-vet store (one-time; hardening milestone)
	cargo vet init

#
# Housekeeping
#

clean: ## Remove build artifacts
	cargo clean

.DEFAULT_GOAL := help
