// The refs.rs witnesses: extract() is TOTAL over arbitrary text (never
// panics — a PR body is third-party input), DETERMINISTIC (same input, same
// output), and its output contract holds structurally (strictly sorted and
// deduped by (kind, repo, number); numbers ≥ 1; repos canonical lowercase;
// a ref can only exist if the body contains a '#'). parse_pr_ref rides
// along on the same input: total, and its accepted output is a validated
// (RepoName, n ≥ 1) pair.
//
// The first input line doubles as the src_repo argument, so the fold path
// (`repo` always canonical regardless of the SOURCE repo's case) is under
// fuzz too — in production src_repo is a RepoName, but totality should not
// depend on that precondition.
#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Ok(s) = std::str::from_utf8(data) else {
        return;
    };
    let (src, body) = match s.split_once('\n') {
        Some((a, b)) => (a, b),
        None => ("Own.er/Na_me", s),
    };

    let once = ghgraph::refs::extract(body, src).expect("extract never errors");
    let twice = ghgraph::refs::extract(body, src).expect("extract never errors");
    assert_eq!(once, twice, "deterministic");
    assert!(
        once.windows(2).all(|w| w[0] < w[1]),
        "strictly sorted and deduped"
    );
    let src_folded = src.to_ascii_lowercase();
    for r in &once {
        assert!(r.number >= 1, "GitHub numbers start at 1");
        assert_eq!(
            r.repo,
            r.repo.to_ascii_lowercase(),
            "target repos are canonical"
        );
        // A bare `#N` inherits src_repo verbatim-folded (extract's contract
        // takes a validated RepoName; the fold is all it adds). A repo
        // parsed OUT OF THE BODY must have the owner/name shape.
        if r.repo != src_folded {
            assert!(
                r.repo.contains('/') && !r.repo.starts_with('/') && !r.repo.ends_with('/'),
                "body-parsed target must be owner/name: {:?}",
                r.repo
            );
        }
    }
    if !once.is_empty() {
        assert!(body.contains('#'), "a ref cannot appear from nothing");
    }

    if let Some((repo, n)) = ghgraph::refs::parse_pr_ref(s) {
        assert!(n >= 1);
        assert_eq!(repo.as_str(), repo.as_str().to_ascii_lowercase());
        assert!(repo.as_str().contains('/'));
    }
});
