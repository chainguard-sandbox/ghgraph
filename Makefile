# Makefile for ghgraph
# Self-documenting: run `make` or `make help` to see available targets.
#
# ghgraph is a design scaffold — function bodies are `todo!()` stubs, so
# `build` compiles but `run` will panic until the bodies land. See DESIGN.md.

.PHONY: help doctor config build release run test check-heavy fuzz fuzz-all fuzz-replay fuzz-cmin dict dict-check mutants fmt lint check check-full audit vet tree tree-check clean install setup

BINARY_NAME := ghgraph

# Fuzzing knobs (see the `fuzz` target).
TARGET ?= config_gate
SECS ?= 60
# Sanitizer for fuzz runs. ASan is the default (guards the one C, bundled
# SQLite, and serde internals); SAN=none roughly doubles exec/s for long
# soaks of pure-safe-Rust targets. Changing the DEFAULT waits on a Linux
# ASan cross-check — macOS and Linux ASan differ — which itself waits on
# the shared seed corpus so the Linux run starts warm (the sequencing is
# deliberate).
SAN ?= address
# Every fuzz target, derived from the harness sources so the list cannot
# drift from fuzz/Cargo.toml.
FUZZ_TARGETS := $(notdir $(basename $(wildcard fuzz/fuzz_targets/*.rs)))
# The nightly bin dir the fuzz targets need on PATH.
NIGHTLY_BIN = $$(dirname "$$(rustup which --toolchain nightly cargo)")

# Mutation-testing knobs (see the `mutants` target). Scoped to the implemented
# modules by default — mutating the todo!() stubs only yields false survivors;
# widen MUTANTS_FILES as modules land.
# JOBS is deliberately modest: each job is a full build tree plus a test
# suite, and a mutant that breaks a loop's progress can allocate at memory-
# bandwidth speed until TIMEOUT kills it — 4 concurrent runaways have OOMed
# a 16GB machine. Loop-bearing code should also carry a progress
# debug_assert (see gh::scrub_tokens) so that class dies by panic instead.
MUTANTS_FILES ?= --file src/db.rs --file src/config.rs --file src/time.rs --file src/identity.rs --file src/queries.rs --file src/parse.rs --file src/gh.rs --file src/refs.rs --file src/sync.rs --file src/attention.rs --file src/report.rs
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

# A target picks up its dictionary (fuzz/dict/<target>.dict) and the
# curated seeds (fuzz/seeds/<target>: pins + handwritten shapes)
# automatically; the working corpus stays local and gitignored.
fuzz: ## Fuzz a target (out-of-build, nightly). TARGET=config_gate SECS=60 SAN=address
	@command -v cargo-fuzz >/dev/null 2>&1 || { echo "cargo-fuzz not found — run: cargo install cargo-fuzz"; exit 1; }
	@echo "fuzzing $(TARGET) for $(SECS)s on nightly (sanitizer: $(SAN))…"; \
	PATH="$(NIGHTLY_BIN):$$HOME/.cargo/bin:$$PATH" cargo fuzz run -s $(SAN) $(TARGET) \
		fuzz/corpus/$(TARGET) $(wildcard fuzz/seeds/$(TARGET)) -- \
		$(if $(wildcard fuzz/dict/$(TARGET).dict),-dict=fuzz/dict/$(TARGET).dict,) \
		-max_total_time=$(SECS)

fuzz-all: ## Sweep every fuzz target for SECS each (10 targets: ~10min at the default)
	@for t in $(FUZZ_TARGETS); do $(MAKE) fuzz TARGET=$$t SECS=$(SECS) SAN=$(SAN) || exit 1; done

fuzz-replay: ## Replay seeds+corpus through every target deterministically (no fuzzing)
	@command -v cargo-fuzz >/dev/null 2>&1 || { echo "cargo-fuzz not found — run: cargo install cargo-fuzz"; exit 1; }
	@for t in $(FUZZ_TARGETS); do \
		echo "replaying $$t…"; \
		PATH="$(NIGHTLY_BIN):$$HOME/.cargo/bin:$$PATH" cargo fuzz run -s $(SAN) $$t \
			fuzz/corpus/$$t $(wildcard fuzz/seeds/$$t) -- -runs=0 || exit 1; \
	done

# The BULK corpus stays local and gitignored, DECIDED: a cmin'd corpus is
# tens of MB of unreviewable binary churn per refresh, forever, in a repo
# whose posture is that every committed artifact is reviewable — and its
# value splits cleanly. The dictionaries and the handwritten seeds carry
# the discovery speed in a few KB of reviewable text; pinned crash/slow
# inputs carry the regression evidence; the blob mass carries only plateau
# warmth, which a local soak rebuilds in hours. Share warmth as a tarball
# when a fresh machine needs it, never as history. fuzz/seeds/ therefore
# holds ONLY curated files — pins (never 40-hex names) and handwritten
# shapes — each reviewable on its own.
fuzz-cmin: ## Minimize a target's local corpus in place (merges seeds in first). TARGET=…
	@mkdir -p fuzz/corpus/$(TARGET)
	@cp -n fuzz/seeds/$(TARGET)/* fuzz/corpus/$(TARGET)/ 2>/dev/null || true
	@PATH="$(NIGHTLY_BIN):$$HOME/.cargo/bin:$$PATH" cargo fuzz cmin $(TARGET)

# The response_parse dictionary is GENERATED from parse.rs's serde surface
# (field idents camelCased + explicit renames), so it cannot drift from the
# types: dict-check regenerates and diffs, the tree-check pattern. The
# static tail (enum values, shape openers) lives in the recipe below —
# stable strings the parser treats as data, listed once.
# The sort is pinned to LC_ALL=C because the generated file is COMMITTED and
# diffed: under a UTF-8 collation locale `sort` ignores punctuation at the
# primary level, so `__typename` lands next to `typename` instead of first,
# and dict-check fails on a clean checkout for no reason but the operator's
# $LANG. The bytes, not the locale, decide the order.
dict: ## Regenerate fuzz/dict/response_parse.dict from src/parse.rs
	@mkdir -p fuzz/dict; t=$$(mktemp); \
	{ echo "# GENERATED by 'make dict' from src/parse.rs — do not hand-edit."; \
	  awk '/#\[serde\(rename = /{ match($$0, /"[^"]+"/); r = substr($$0, RSTART+1, RLENGTH-2); print r; next } \
	       /^[[:space:]]+pub [a-z_]+:/{ f = $$2; sub(/:.*/, "", f); out = ""; up = 0; \
	         for (i = 1; i <= length(f); i++) { c = substr(f, i, 1); \
	           if (c == "_") { up = 1; continue }; out = out (up ? toupper(c) : c); up = 0 }; \
	         print out }' src/parse.rs | LC_ALL=C sort -u | \
	  awk '{ printf "key_%s=\"\\\"%s\\\":\"\n", $$1, $$1 }'; \
	  printf '%s\n' \
	    'val_OPEN="\"OPEN\""' 'val_CLOSED="\"CLOSED\""' 'val_MERGED="\"MERGED\""' \
	    'val_APPROVED="\"APPROVED\""' 'val_CHANGES_REQUESTED="\"CHANGES_REQUESTED\""' \
	    'val_COMMENTED="\"COMMENTED\""' 'val_DISMISSED="\"DISMISSED\""' \
	    'val_OWNER="\"OWNER\""' 'val_MEMBER="\"MEMBER\""' 'val_COLLABORATOR="\"COLLABORATOR\""' \
	    'val_User="\"User\""' 'val_Bot="\"Bot\""' 'val_Mannequin="\"Mannequin\""' \
	    'ts="\"2026-01-02T03:04:05Z\""' \
	    'objnode="{\"node\":{"' 'objdata="{\"data\":{"' 'objerrors="{\"errors\":[{"' \
	    'objpage="{\"pageInfo\":{"'; \
	} > $$t && mv $$t fuzz/dict/response_parse.dict && \
	echo "wrote fuzz/dict/response_parse.dict ($$(grep -c '=' fuzz/dict/response_parse.dict) entries)"

dict-check: ## Fail if the dictionary drifted from parse.rs
	@t=$$(mktemp -d); cp fuzz/dict/response_parse.dict $$t/have && \
	$(MAKE) -s dict && diff -u $$t/have fuzz/dict/response_parse.dict || \
	{ rm -rf $$t; echo "dictionary diverged — 'make dict' regenerated it; review and commit"; exit 1; }; \
	rm -rf $$t

mutants: ## Mutation-test the implemented modules (needs cargo-mutants). MUTANTS_FILES/JOBS/TIMEOUT
	@command -v cargo-mutants >/dev/null 2>&1 || { echo "cargo-mutants not found — run: cargo install cargo-mutants"; exit 1; }
	cargo mutants $(MUTANTS_FILES) --jobs $(JOBS) --timeout $(TIMEOUT)

check: ## Fast pre-commit gate: format, clippy, check, test
	cargo fmt --all -- --check
	cargo clippy --all-targets -- -D warnings
	cargo check --all-targets
	cargo test
	@echo "✓ all checks passed"

check-heavy: ## The ignored heavy tests (e.g. the 120s live watchdog stall)
	cargo test -- --ignored --skip capture_

check-full: check audit vet tree-check ## check, plus the supply-chain checks CI runs

#
# Supply chain (dependency policy — see DESIGN.md; all four run in CI)
#

audit: ## Scan dependencies for known advisories (needs cargo-audit; make setup)
	@command -v cargo-audit >/dev/null 2>&1 || { echo "cargo-audit not found — run 'make setup' (or: cargo install cargo-audit)"; exit 1; }
	cargo audit

vet: ## Vet the dependency tree against supply-chain/ (needs cargo-vet; make setup)
	@command -v cargo-vet >/dev/null 2>&1 || { echo "cargo-vet not found — run 'make setup' (or: cargo install cargo-vet)"; exit 1; }
	cargo vet --locked

# The snapshot's first line embeds the local checkout path (cargo prints the
# root package's manifest dir); sed strips it so the snapshot is
# host-portable — CI checkouts and contributor clones live elsewhere. Both
# targets write to a temp file first: a plain redirect would truncate the
# committed snapshot before a failing cargo runs, and a pipe into diff would
# let diff's exit status mask cargo's.
TREE_CMD := cargo tree --locked --edges normal --target all

tree: ## Regenerate the dependency-graph snapshot (run after any Cargo.toml/lock change)
	@t=$$(mktemp); $(TREE_CMD) > $$t || { rm -f $$t; echo "cargo tree failed (lockfile drift?)"; exit 1; }; \
		sed -E -i.bak '1s| \(.*\)$$||' $$t && rm -f $$t.bak; \
		mv $$t supply-chain/cargo-tree.txt && \
		echo "wrote supply-chain/cargo-tree.txt — review the diff like code"

tree-check: ## Fail if the dependency graph moved without a snapshot update
	@t=$$(mktemp); $(TREE_CMD) > $$t || { rm -f $$t; echo "cargo tree failed (lockfile drift?)"; exit 1; }; \
		sed -E -i.bak '1s| \(.*\)$$||' $$t && rm -f $$t.bak; \
		diff -u supply-chain/cargo-tree.txt $$t || \
		{ rm -f $$t; echo "dependency graph diverged from supply-chain/cargo-tree.txt — run 'make tree' and review"; exit 1; }; \
		rm -f $$t

# Versions match .github/workflows/ci.yml — bump both together, by diff.
setup: ## Install the dev tools the quality targets need (cargo-audit, cargo-vet)
	cargo install cargo-audit --version 0.22.2 --locked
	cargo install cargo-vet --version 0.10.2 --locked

#
# Housekeeping
#

clean: ## Remove build artifacts
	cargo clean

.DEFAULT_GOAL := help
