# supply-chain/

The cargo-vet store (config.toml, audits.toml, imports.lock) plus the
dependency-graph snapshot (cargo-tree.txt) the CI `tree-check` diffs
against. config.toml is TOOL-MANAGED — `cargo vet` rewrites it and
comments do not survive, so rationale lives here.

Imports pull real third-party audits (Mozilla, Google, Bytecode
Alliance) so `cargo vet` asserts more than "the tree did not change".
The exemptions are the init-shaped residue; burn them down as imported
audits (or our own) cover them — `cargo vet prune` after refreshing
imports does the bookkeeping.

Provenance questions a review raised, resolved 2026-08-06:

- **serde_core** is a workspace member of serde-rs/serde itself (the
  2025 crate split of serde's core traits) — same repository, same
  publisher as serde.
- **zmij** is dtolnay's Schubfach-based float formatter, adopted by
  serde_json after ryu — same trust domain and publisher as serde_json.

Checksum integrity is cargo's own mechanism, not a manual step: the
lockfile pins SHA-256 hashes and cargo refuses a registry artifact
that does not match, on every fetch.

The tree snapshot is host-portable (the root line's absolute manifest
path is stripped by `make tree`); regenerate with `make tree` after any
Cargo.toml/Cargo.lock change and review the diff like code.
