#![forbid(unsafe_code)]
// Design-phase scaffold: the command surface, module boundaries, types, and
// invariants are the deliverable. Most bodies are todo!() stubs.
#![allow(dead_code)]

// Unix only, by declaration: cancellation is process-group SIGINT semantics
// and archive protection is mode bits (0700/0600). A port would need a second
// mechanism — and a second proof — for each. Not a missing feature; a fence.
#[cfg(windows)]
compile_error!(
    "ghgraph is Unix-only: its cancellation and file-mode invariants are Unix semantics (see DESIGN.md)"
);

// The library crate owns the modules so both the binary (src/main.rs) and
// out-of-build verification harnesses (fuzz/) can reach them. A bin-only crate
// exposes no library API, which is why fuzzing a module with crate-internal
// dependencies (e.g. `config`, which uses `crate::error`) needs this split.
pub mod attention;
pub mod config;
pub mod db;
pub mod error;
pub mod gh;
pub mod queries;
pub mod refs;
pub mod report;
pub mod sync;
pub mod time;
