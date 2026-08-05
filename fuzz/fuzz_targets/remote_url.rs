#![no_main]
//! The cwd git remote is attacker-chosen content (clone a repo, its
//! .git/config feeds `pr <bare-number>` resolution), so the URL→repo parser
//! (report::github_repo_from_remote_url) gets the adversarial treatment:
//!
//!   1. Totality: no panic on arbitrary UTF-8.
//!   2. Host pinning cannot be bypassed: every ACCEPTED input starts with
//!      one of the three exact github.com prefixes, re-checked here
//!      independently (byte prefixes vs the parser's strip_prefix chain —
//!      same definition, different code path, so an over-acceptance in one
//!      cannot hide in the other).
//!   3. Accepted values are RepoName-canonical: exactly one '/', lowercase,
//!      and re-parsing "https://github.com/<accepted>" is identity — the
//!      canonicalization can never widen what a crafted remote can name.

use libfuzzer_sys::fuzz_target;

const PREFIXES: [&str; 3] = [
    "git@github.com:",
    "ssh://git@github.com/",
    "https://github.com/",
];

fuzz_target!(|data: &[u8]| {
    let Ok(s) = std::str::from_utf8(data) else {
        return;
    };
    let Some(repo) = ghgraph::report::github_repo_from_remote_url(s) else {
        return;
    };
    assert!(
        PREFIXES.iter().any(|p| s.starts_with(p)),
        "host pin bypassed: {s:?} -> {}",
        repo.as_str()
    );
    let name = repo.as_str();
    assert_eq!(
        name.bytes().filter(|&b| b == b'/').count(),
        1,
        "not owner/name: {name:?}"
    );
    assert_eq!(name, name.to_ascii_lowercase(), "not canonical: {name:?}");
    let again = ghgraph::report::github_repo_from_remote_url(&format!(
        "https://github.com/{name}"
    ))
    .expect("canonical form must re-parse");
    assert_eq!(again.as_str(), name, "round trip must be identity");
});
