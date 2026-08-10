# Makefile for ghgraph
# Self-documenting: run `make` or `make help` to see available targets.

.PHONY: help doctor config build release run test check-heavy fuzz fuzz-all fuzz-replay fuzz-cmin fuzz-targets-check dict dict-check mutants mutants-diff mutants-extreme mutants-equiv fmt lint check check-full audit vet tree tree-check clean install setup

BINARY_NAME := ghgraph

# Fuzzing knobs (see the `fuzz` target).
TARGET ?= config_gate
SECS ?= 60
# Sanitizer for fuzz SOAKS. The Linux ASan cross-check this default was
# waiting on has run: every target replayed clean over the pinned inputs
# and seeds, and a ~2.2B-execution soak under ASan
# — plus a --careful pass with an instrumented std — found nothing. Linux
# and macOS ASan agree there is nothing here to find, which is what the
# deferral asked for.
#
# So soaks default to none. The cost ASan was buying is measured, not
# assumed: on this box it is 2.15x (refs_extract) to 3.98x (scrub_tokens)
# of exec/s, well past the "roughly doubles" this comment used to claim.
# Every current target is pure safe Rust over borrowed bytes, `unsafe` is
# forbidden crate-wide, and none of them reach bundled SQLite — so ASan is
# watching C that these targets never execute, at 2-4x the price.
#
# What reverses this: a target that actually touches rusqlite (the archive
# harness the report/db coverage gap needs). The moment one lands, its
# soaks want SAN=address again, because that is the first time the C is
# under the fuzzer at all. Reach for SAN=address deliberately then; the
# knob stays.
SAN ?= none
# The replay gate keeps ASan regardless. fuzz-replay is deterministic and
# finishes in seconds, so the sanitizer is free precisely where detection
# matters most — every committed seed, including the pinned crash inputs,
# re-proven under the stronger checker on every run.
REPLAY_SAN ?= address
# Every fuzz target, derived from the harness sources. Deriving from the
# sources does not by itself prevent drift from fuzz/Cargo.toml — it picks
# one of TWO sources of truth, and the build follows the other one. A .rs
# added without its [[bin]] silently disappears from the build while
# fuzz-all and fuzz-replay still try to run it; the reverse orphans a
# [[bin]]. fuzz-targets-check pins the two together.
FUZZ_TARGETS := $(notdir $(basename $(wildcard fuzz/fuzz_targets/*.rs)))
# The nightly bin dir the fuzz targets need on PATH.
NIGHTLY_BIN = $$(dirname "$$(rustup which --toolchain nightly cargo)")

# Mutation-testing knobs (see the `mutants` targets). Scope, timeout policy,
# and the known-equivalent exclusions live in .cargo/mutants.toml so every
# invocation shares them; FILE narrows a run, SINCE picks the diff base for
# `mutants-diff` (the per-milestone form).
# JOBS is deliberately modest: each job is a full build tree plus a test
# suite, and a mutant that breaks a loop's progress can allocate at memory-
# bandwidth speed until the timeout kills it — 4 concurrent runaways have
# OOMed a 16GB machine. Loop-bearing code should also carry a progress
# debug_assert (see gh::scrub_tokens) so that class dies by panic instead.
JOBS ?= 2
FILE ?=
SINCE ?= main

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
fuzz: ## Fuzz a target (out-of-build, nightly). TARGET=config_gate SECS=60 SAN=none
	@command -v cargo-fuzz >/dev/null 2>&1 || { echo "cargo-fuzz not found — run: cargo install cargo-fuzz"; exit 1; }
	@mkdir -p fuzz/corpus/$(TARGET)
	@echo "fuzzing $(TARGET) for $(SECS)s on nightly (sanitizer: $(SAN))…"; \
	PATH="$(NIGHTLY_BIN):$$HOME/.cargo/bin:$$PATH" cargo fuzz run -s $(SAN) $(TARGET) \
		fuzz/corpus/$(TARGET) $(wildcard fuzz/seeds/$(TARGET)) -- \
		$(if $(wildcard fuzz/dict/$(TARGET).dict),-dict=fuzz/dict/$(TARGET).dict,) \
		-max_total_time=$(SECS)

fuzz-all: ## Sweep every fuzz target for SECS each (10 targets: ~10min at the default)
	@for t in $(FUZZ_TARGETS); do $(MAKE) fuzz TARGET=$$t SECS=$(SECS) SAN=$(SAN) || exit 1; done

fuzz-replay: ## Replay seeds+corpus through every target deterministically (ASan, no fuzzing)
	@command -v cargo-fuzz >/dev/null 2>&1 || { echo "cargo-fuzz not found — run: cargo install cargo-fuzz"; exit 1; }
	@for t in $(FUZZ_TARGETS); do \
		mkdir -p fuzz/corpus/$$t; \
		echo "replaying $$t (sanitizer: $(REPLAY_SAN))…"; \
		PATH="$(NIGHTLY_BIN):$$HOME/.cargo/bin:$$PATH" cargo fuzz run -s $(REPLAY_SAN) $$t \
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

mutants: ## Mutation-test the crate (full sweep ~4h; needs cargo-mutants). FILE=src/foo.rs narrows
	@command -v cargo-mutants >/dev/null 2>&1 || { echo "cargo-mutants not found — run: cargo install cargo-mutants"; exit 1; }
	cargo mutants $(if $(FILE),--file $(FILE)) --jobs $(JOBS)

mutants-diff: ## Mutation-test only code changed since SINCE (default: main)
	@command -v cargo-mutants >/dev/null 2>&1 || { echo "cargo-mutants not found — run: cargo install cargo-mutants"; exit 1; }
	@t=$$(mktemp); git diff $$(git merge-base $(SINCE) HEAD) > $$t; \
		cargo mutants --in-diff $$t --jobs $(JOBS); s=$$?; rm -f $$t; exit $$s

# Function-replacement mutants only — the pseudo-tested-code sweep: a
# survivor here is a function whose ENTIRE body can vanish unnoticed, the
# signal operator-level noise drowns. The ' in ' discriminator relies on
# cargo-mutants' mutant-naming convention (operator mutants read "replace X
# with Y in fn"; body replacements read "replace fn -> T with v") — verified
# exact against --list at 0.27; re-verify after a cargo-mutants major bump.
mutants-extreme: ## Pseudo-tested-code sweep: function-replacement mutants only (~35 min)
	@command -v cargo-mutants >/dev/null 2>&1 || { echo "cargo-mutants not found — run: cargo install cargo-mutants"; exit 1; }
	cargo mutants --exclude-re ' in ' --jobs $(JOBS)

# The inverse gate over the argued-equivalent ledger: each entry is
# "pattern|expected-missed-count", and the run must miss EXACTLY that many.
# Fewer missed means a note rotted in the secretly-killable direction (a
# test now discriminates it — the db.rs reverse-selects hook did exactly
# this): delete the entry and its code note, and record the killing test
# there instead. MORE missed means a new survivor appeared inside the same
# function — triage it. Counts, not names, because mutant names embed
# line numbers that drift. Entries mirror .cargo/mutants.toml exclude_re
# plus the documented-at-code survivors.
MUTANTS_EQUIV := \
	'replace match guard e.kind\(\) == std::io::ErrorKind::BrokenPipe with true in emit|1' \
	'replace - with \+ in overhead_intercept_ms|2' \
	'replace < with <= in wrong_version|1' \
	'replace > with >= in wrong_version|1' \
	'replace \| with . in open_rw|3' \
	'replace \| with \^ in open_ro$$|1' \
	'replace \| with \^ in open_ro_audit|1' \
	'replace == with != in open_ro_audit|1' \
	'replace match guard e.kind\(\) == std::io::ErrorKind::AlreadyExists with true|1' \
	'replace configure_conn -> Result<\(\)> with Ok\(\(\)\)|1' \
	'replace - with / in cap$$|1' \
	'replace > with >= in incremental_since|1' \
	'replace < with <= in split_point|1' \
	'replace match guard !has_prev with true in refresh_one|1' \
	'replace \+= with \*= in refresh_one|1' \
	'replace match guard Some\(&c\) != cursor.as_ref\(\) with false in refresh_one|1'

mutants-equiv: ## Verify the argued-equivalent mutants still survive, exactly (drift either way fails)
	@command -v cargo-mutants >/dev/null 2>&1 || { echo "cargo-mutants not found — run: cargo install cargo-mutants"; exit 1; }
	@for entry in $(MUTANTS_EQUIV); do \
		re=$${entry%|*}; want=$${entry##*|}; \
		cargo mutants --no-config --re "$$re" --jobs $(JOBS) >/dev/null 2>&1; \
		got=$$(wc -l < mutants.out/missed.txt | tr -d ' '); \
		[ "$$got" -eq "$$want" ] || { echo "equiv ledger drift for $$re: expected $$want missed, got $$got — a stale note (fewer) or a new survivor (more)"; exit 1; }; \
		echo "as argued ($$got missed): $$re"; \
	done

check: ## Fast pre-commit gate: format, clippy, check, test
	cargo fmt --all -- --check
	cargo clippy --all-targets -- -D warnings
	cargo check --all-targets
	cargo test
	@echo "✓ all checks passed"

check-heavy: ## The ignored heavy tests (e.g. the 120s live watchdog stall)
	cargo test -- --ignored --skip capture_

# The harness has two lists of its targets — fuzz_targets/*.rs, which
# fuzz-all and fuzz-replay sweep, and the [[bin]] tables in
# fuzz/Cargo.toml, which the build follows. A sweep over a target that was
# never registered is the failure this catches: it exits 0 per target and
# reports coverage for a binary that does not exist, so the miss looks like
# a pass. Same shape as tree-check and dict-check — a committed pair that
# must agree, diffed rather than trusted. LC_ALL=C for the same reason the
# dict recipe pins it: byte order, not the operator's locale.
fuzz-targets-check: ## Fail if fuzz_targets/*.rs and fuzz/Cargo.toml [[bin]] disagree
	@s=$$(mktemp); b=$$(mktemp); \
	printf '%s\n' $(FUZZ_TARGETS) | LC_ALL=C sort > $$s; \
	awk '/^\[\[bin\]\]/ { inbin = 1; next } \
	     inbin && /^name[[:space:]]*=/ { match($$0, /"[^"]+"/); \
	       print substr($$0, RSTART + 1, RLENGTH - 2); inbin = 0 }' \
	  fuzz/Cargo.toml | LC_ALL=C sort > $$b; \
	if ! diff -u $$s $$b > /dev/null; then \
		echo "fuzz target drift — fuzz_targets/*.rs (-) vs fuzz/Cargo.toml [[bin]] (+):"; \
		diff -u $$s $$b | tail -n +3 | grep -E '^[-+]' | sed 's/^/  /'; \
		rm -f $$s $$b; exit 1; \
	fi; \
	echo "✓ $$(grep -c . $$s) fuzz targets — sources and manifest agree"; \
	rm -f $$s $$b

check-full: check audit vet tree-check fuzz-targets-check dict-check ## check, plus the supply-chain checks CI runs

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
