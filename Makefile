# Makefile for ghgraph
# Self-documenting: run `make` or `make help` to see available targets.
#
# ghgraph is a design scaffold — function bodies are `todo!()` stubs, so
# `build` compiles but `run` will panic until the bodies land. See DESIGN.md.

.PHONY: help doctor build release run test fmt lint check check-full audit clean install setup-vet

BINARY_NAME := ghgraph

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

audit: ## Scan dependencies for known advisories (cargo-audit)
	cargo audit

setup-vet: ## Initialize the cargo-vet store (one-time; hardening milestone)
	cargo vet init

#
# Housekeeping
#

clean: ## Remove build artifacts
	cargo clean

.DEFAULT_GOAL := help
