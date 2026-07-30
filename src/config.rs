use std::env;
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::error::{Error, Result};
use crate::identity::{AuthorPattern, Login, RepoName};

fn default_lookback_days() -> u32 {
    90
}

// Quiet mutations (comment edits and deletions, thread resolves) never bump
// the PR's updatedAt, so the watermark cannot see them; a periodic complete
// refetch is the only cure. Tiered because relevance decays with state: open
// PRs drive attention and re-verify often, closed and merged rarely change.
// The open tier is LOOKBACK-EXEMPT — OPEN state is the relevance signal,
// not recency; otherwise a quiet open PR closed upstream past the lookback
// sits in waiting_on_me forever. The closed tier is bounded by the
// lookback, measured against closed_at/merged_at.
fn default_reverify_open_days() -> u32 {
    7
}

fn default_reverify_closed_days() -> u32 {
    30
}

fn default_workers() -> usize {
    // Concurrent gh subprocesses. Small on purpose: GitHub secondary rate
    // limits punish burst concurrency, and 2-10 repos don't need more.
    3
}

fn default_rate_limit_floor() -> u32 {
    // The GraphQL point budget (5,000/hr) is shared with the operator's
    // interactive gh use and anything else on the token. When `remaining`
    // falls below the floor, sync defers the rest of the run — typed
    // Deferred messages, watermarks holding at the last completed window —
    // instead of draining the budget to zero. 10% of the hourly budget by
    // default. The floor is SOFT by K·c_max (workers in flight when the
    // threshold trips finish their calls); do not "fix" this with worker
    // coordination — the softness is documented, the coordination is not
    // worth its complexity. `sync --pr` is exempt: the floor exists to
    // protect interactive use, and --pr is interactive use.
    500
}

/// Which slice of a repo discovery walks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Scope {
    /// The operator's working set, plus tracked people's involvement.
    #[default]
    Working,
    /// The whole PR and issue stream — the maintainer view.
    Project,
}

/// One `repos` entry: a bare "owner/name" (working scope, all defaults) or
/// the full object. The config file is a public interface — the eighth verb —
/// so the shape is closed (deny_unknown_fields) and every default is
/// resolved in code, in one place, where a test can see it.
pub enum RepoEntry {
    Name(RepoName),
    Detailed(RepoConfig),
}

// Hand-written rather than #[serde(untagged)]: an untagged enum collapses a
// bad object into "data did not match any variant of untagged enum
// RepoEntry", naming neither the entry nor the field. Dispatching on the JSON
// type instead lets RepoConfig's deny_unknown_fields surface the offending
// field by name (config errors name the field, per the config contract).
impl<'de> Deserialize<'de> for RepoEntry {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct RepoEntryVisitor;
        impl<'de> serde::de::Visitor<'de> for RepoEntryVisitor {
            type Value = RepoEntry;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str(r#"a repo string "owner/name" or a repo object"#)
            }
            fn visit_str<E: serde::de::Error>(self, v: &str) -> std::result::Result<RepoEntry, E> {
                // Route through RepoName's Deserialize so the bare-string and
                // object forms share one validation and one error message.
                RepoName::deserialize(serde::de::value::StrDeserializer::new(v))
                    .map(RepoEntry::Name)
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                map: A,
            ) -> std::result::Result<RepoEntry, A::Error> {
                RepoConfig::deserialize(serde::de::value::MapAccessDeserializer::new(map))
                    .map(RepoEntry::Detailed)
            }
        }
        deserializer.deserialize_any(RepoEntryVisitor)
    }
}

#[derive(Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct RepoConfig {
    /// "owner/name", validated and case-folded by the RepoName newtype.
    pub repo: RepoName,
    #[serde(default)]
    pub scope: Scope,
    /// Sync the repo's issue stream. Default: on at project scope, and
    /// meaningless below it — `true` at working scope is a CONFIGURATION
    /// error, never a silent no-op (there is no standalone issue loop
    /// outside project scope).
    #[serde(default)]
    issues: Option<bool>,
    /// Override the global lookback for this repo — a huge archive is
    /// opt-in, not the price of project scope.
    #[serde(default)]
    pub lookback_days: Option<u32>,
    /// Ingest PRs authored by bot accounts (author type `Bot`). Default:
    /// true at working scope (a bot PR you were asked to review is your
    /// work), false at project scope (the firehose is mostly dependabot).
    #[serde(default)]
    bots: Option<bool>,
    /// Logins whose PRs are never ingested for this repo. Matching is one
    /// function (identity::AuthorPattern::matches, on identity::login_eq):
    /// ASCII-case-insensitive; a bare "x" matches an author with login x of
    /// EITHER type, User or Bot (excluding "dependabot" must filter the
    /// dependabot bot — the rationale is recorded at AuthorPattern);
    /// "x[bot]" NARROWS the match to GraphQL author type Bot (the API
    /// returns bare logins for bots — a literal bracket match would never
    /// fire). PLANNED (milestone 2 sync applies it at discovery — excluded
    /// PRs are skipped before hydration, so they cost discovery only — and
    /// enforces it at ingest; milestone 4 lands the project-scope filter
    /// defaults around it).
    /// Filters govern ingest, never deletion: excluding an author later
    /// does not touch their archived rows. Note bot-typed authors are
    /// already excluded by default at project scope — exclude_authors is
    /// for humans and for bots you have opted back in via bots: true.
    #[serde(default)]
    pub exclude_authors: Vec<AuthorPattern>,
}

impl RepoConfig {
    pub fn issues(&self) -> bool {
        self.issues.unwrap_or(self.scope == Scope::Project)
    }

    pub fn bots(&self) -> bool {
        self.bots.unwrap_or(self.scope == Scope::Working)
    }
}

impl RepoEntry {
    /// Shorthand → full form; defaults resolve here and nowhere else. Case
    /// folding is RepoName's (construction folds to lowercase, so `Foo/Bar`
    /// and `foo/bar` can never split the (repo, number) key — see
    /// identity.rs).
    pub fn resolved(&self) -> RepoConfig {
        match self {
            RepoEntry::Detailed(rc) => rc.clone(),
            RepoEntry::Name(name) => RepoConfig {
                repo: name.clone(),
                scope: Scope::Working,
                issues: None,
                lookback_days: None,
                bots: None,
                exclude_authors: Vec::new(),
            },
        }
    }
}

// Deserialization IS validation: every identifier field is a newtype
// (identity.rs) whose Deserialize impl runs the injection gate, so a caller
// that bypasses `parse` — `serde_json::from_str::<Config>` directly — still
// cannot obtain an unvalidated identifier. (Before milestone 1 this was a
// known, fenced gap held by discipline; the direct-deserialize test below is
// the closure's witness.) `parse` remains the entry point: it labels the
// source and runs `validate`, which now carries only the CROSS-FIELD rules —
// the per-identifier gate lives in the types.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// The operator's GitHub login. "my PRs" vs "PRs I review" derives from
    /// this at query time; it is never stored per row.
    pub viewer: Login,
    /// Repositories to sync; see RepoEntry.
    pub repos: Vec<RepoEntry>,
    /// GitHub logins to track in working-scope repos: collaborators or
    /// contributors the operator opts in. Their involvement (authored,
    /// assigned, mentioned, commented) is discovered and archived alongside
    /// the operator's own; `attention` surfaces their unreviewed open PRs.
    /// Project-scope repos subsume this — everyone is already in.
    #[serde(default)]
    pub people: Vec<Login>,
    /// Archive path. Default: $XDG_DATA_HOME/ghgraph/ghgraph.db.
    #[serde(default)]
    pub db_path: Option<PathBuf>,
    /// How far back a first (or --full) sync reaches.
    #[serde(default = "default_lookback_days")]
    pub lookback_days: u32,
    /// Complete-refetch period for open PRs (quiet-mutation recovery).
    #[serde(default = "default_reverify_open_days")]
    pub reverify_open_days: u32,
    /// Complete-refetch period for closed and merged PRs, within lookback.
    #[serde(default = "default_reverify_closed_days")]
    pub reverify_closed_days: u32,
    #[serde(default = "default_workers")]
    pub workers: usize,
    /// Stop syncing when the GraphQL rate limit's `remaining` drops below
    /// this; the deferral is recorded, never silent.
    #[serde(default = "default_rate_limit_floor")]
    pub rate_limit_floor: u32,
}

impl Config {
    pub fn db_path(&self) -> Result<PathBuf> {
        if let Some(p) = &self.db_path {
            return Ok(p.clone());
        }
        Ok(xdg_dir("XDG_DATA_HOME", ".local/share")?.join("ghgraph/ghgraph.db"))
    }
}

fn home() -> Result<PathBuf> {
    env::var_os("HOME")
        .map(PathBuf::from)
        .ok_or_else(|| Error::config("HOME is not set"))
}

fn xdg_dir(var: &str, fallback: &str) -> Result<PathBuf> {
    resolve_xdg(env::var_os(var), fallback)
}

/// Pure policy half of [`xdg_dir`], split from the env read so the
/// set/empty/unset cases are testable: mutating process env in tests would
/// need `std::env::set_var`, which is `unsafe` in edition 2024 and unsafe is
/// forbidden crate-wide. An empty XDG var is treated as unset, per the Base
/// Directory spec.
fn resolve_xdg(value: Option<std::ffi::OsString>, fallback: &str) -> Result<PathBuf> {
    match value {
        Some(v) if !v.is_empty() => Ok(PathBuf::from(v)),
        _ => Ok(home()?.join(fallback)),
    }
}

pub fn config_path(flag: Option<&Path>) -> Result<PathBuf> {
    if let Some(p) = flag {
        return Ok(p.to_path_buf());
    }
    Ok(xdg_dir("XDG_CONFIG_HOME", ".config")?.join("ghgraph/config.json"))
}

pub fn load(flag: Option<&Path>) -> Result<Config> {
    let path = config_path(flag)?;
    let raw = std::fs::read_to_string(&path)
        .map_err(|e| Error::config(format!("cannot read config {}: {e}", path.display())))?;
    parse(&raw, &path.display().to_string())
}

/// Parse and validate a config from JSON text. `source` labels the input in the
/// deserialization error (a path for [`load`], `"<fuzz>"`/`"<test>"` elsewhere).
/// Shared by `load`, the tests, and the fuzz harness so all three exercise the
/// one injection gate rather than a re-implementation.
pub fn parse(raw: &str, source: &str) -> Result<Config> {
    let cfg: Config = serde_json::from_str(raw)
        .map_err(|e| Error::config(format!("invalid config {source}: {e}")))?;
    validate(&cfg)?;
    Ok(cfg)
}

/// Cross-field validation. The per-identifier injection gate lives in the
/// identity newtypes' Deserialize impls (identity.rs) — by the time a Config
/// value exists, every identifier is validated — so what remains here are
/// the rules that span fields.
fn validate(cfg: &Config) -> Result<()> {
    for entry in &cfg.repos {
        let rc = entry.resolved();
        if rc.scope == Scope::Working && rc.issues() {
            return Err(Error::config(format!(
                "repo {:?}: linked issues are already cached at working scope; \
                 the issue *stream* requires scope: project",
                rc.repo.as_str()
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{Config, RepoEntry, parse};

    // The public entry the fuzz harness uses: a config whose viewer would
    // smuggle a search qualifier is rejected end-to-end (serde + gate), not
    // just by the private predicate.
    #[test]
    fn parse_rejects_injection_config() {
        let bad = r#"{"viewer":"me involves:target","repos":["o/n"]}"#;
        let err = parse(bad, "<test>").err().expect("must reject");
        assert_eq!(err.code, crate::error::Code::Configuration);
        let ok = parse(r#"{"viewer":"octocat","repos":["o/n"]}"#, "<test>");
        assert!(ok.is_ok(), "a clean config must parse");
    }

    // A bad field in an object entry must be named, not collapsed into serde's
    // opaque untagged-enum message — the reason RepoEntry has a hand-written
    // Deserialize instead of #[serde(untagged)].
    #[test]
    fn repo_entry_object_names_bad_field() {
        let err = serde_json::from_str::<RepoEntry>(r#"{"repo":"a/b","bogus":1}"#)
            .err()
            .expect("should error")
            .to_string();
        assert!(err.contains("bogus"), "should name the field: {err}");
        assert!(
            !err.contains("did not match any variant"),
            "should not be the untagged message: {err}"
        );
    }

    // Repo is case-folded at the boundary (by RepoName's constructor) so
    // Foo/Bar and foo/bar are one key. The charset, boundary, and injection
    // tests for the newtypes themselves live in identity.rs.
    #[test]
    fn repo_entry_lowercases() {
        let e: RepoEntry = serde_json::from_str(r#""Foo/Bar""#).unwrap();
        assert_eq!(e.resolved().repo.as_str(), "foo/bar");
    }

    // The closure witness for the pre-milestone-1 fenced gap (see the Config
    // comment): bypassing parse() via serde directly can no longer yield an
    // unvalidated identifier, because validation runs inside Deserialize.
    #[test]
    fn direct_deserialize_cannot_bypass_the_gate() {
        assert!(
            serde_json::from_str::<Config>(r#"{"viewer":"me involves:target","repos":["o/n"]}"#)
                .is_err(),
            "an injection viewer must fail even without parse()"
        );
        assert!(
            serde_json::from_str::<Config>(r#"{"viewer":"v","repos":["o/n is:issue"]}"#).is_err(),
            "an injection repo must fail even without parse()"
        );
    }

    // The scope-derived defaults are policy: issues() off / bots() on at
    // working scope, and the reverse at project scope.
    #[test]
    fn scope_defaults_for_issues_and_bots() {
        let w = parse(
            r#"{"viewer":"v","repos":[{"repo":"o/n","scope":"working"}]}"#,
            "<test>",
        )
        .unwrap()
        .repos[0]
            .resolved();
        assert!(!w.issues(), "working: issues() defaults off");
        assert!(w.bots(), "working: bots() defaults on");

        let p = parse(
            r#"{"viewer":"v","repos":[{"repo":"o/n","scope":"project"}]}"#,
            "<test>",
        )
        .unwrap()
        .repos[0]
            .resolved();
        assert!(p.issues(), "project: issues() defaults on");
        assert!(!p.bots(), "project: bots() defaults off");
    }

    // The global defaults are a documented contract; a silent change is a
    // regression (see the default_* fns and their rationale).
    #[test]
    fn defaults_match_documented_policy() {
        let c = parse(r#"{"viewer":"v","repos":["o/n"]}"#, "<test>").unwrap();
        assert_eq!(c.lookback_days, 90);
        assert_eq!(c.reverify_open_days, 7);
        assert_eq!(c.reverify_closed_days, 30);
        assert_eq!(c.workers, 3);
        assert_eq!(c.rate_limit_floor, 500);
    }

    // validate() must reject a malformed repo (not just a bad viewer) and the
    // working-scope issue-stream error, and accept a clean config.
    #[test]
    fn validate_rejects_bad_repo_and_working_issues() {
        assert!(
            parse(r#"{"viewer":"v","repos":["bad repo"]}"#, "<test>").is_err(),
            "a malformed repo must be rejected"
        );
        let e = parse(
            r#"{"viewer":"v","repos":[{"repo":"o/n","scope":"working","issues":true}]}"#,
            "<test>",
        )
        .err()
        .expect("issues:true at working scope must be rejected");
        assert_eq!(e.code, crate::error::Code::Configuration);
        assert!(
            parse(
                r#"{"viewer":"v","repos":[{"repo":"o/n","scope":"project"}]}"#,
                "<test>"
            )
            .is_ok(),
            "a clean config must parse"
        );
    }

    // exclude_authors are gated too (by AuthorPattern's Deserialize): a
    // malformed entry is rejected; a bare login and a [bot]-suffixed one are
    // accepted. Matching semantics are tested in identity.rs.
    #[test]
    fn validate_checks_exclude_authors() {
        assert!(
            parse(
                r#"{"viewer":"v","repos":[{"repo":"o/n","exclude_authors":["bad login"]}]}"#,
                "<test>"
            )
            .is_err(),
            "a malformed exclude_authors entry must be rejected"
        );
        assert!(
            parse(
                r#"{"viewer":"v","repos":[{"repo":"o/n","exclude_authors":["alice","dependabot[bot]"]}]}"#,
                "<test>"
            )
            .is_ok(),
            "valid exclude_authors (incl. [bot]) must be accepted"
        );
    }

    // Duplicate keys are rejected, never last-wins (ROADMAP milestone 1:
    // "rejects duplicate keys explicitly, or closure regresses while
    // diagnosability improves"). serde's duplicate-field detection is the
    // mechanism; this pins it so a Deserialize rewrite cannot drop it.
    #[test]
    fn duplicate_keys_are_rejected() {
        assert!(
            serde_json::from_str::<Config>(r#"{"viewer":"a","viewer":"b","repos":["o/n"]}"#)
                .is_err(),
            "duplicate top-level key must be rejected"
        );
        assert!(
            serde_json::from_str::<RepoEntry>(r#"{"repo":"a/b","repo":"c/d"}"#).is_err(),
            "duplicate key in a repos object entry must be rejected"
        );
    }

    // The shipped example config is the operator's template (and what
    // `make config` installs); it must always pass the real gate.
    #[test]
    fn example_config_parses() {
        parse(include_str!("../config.example.json"), "<example>")
            .expect("config.example.json must parse");
    }

    // The XDG policy is pure and pinned: a set, nonempty var wins verbatim;
    // empty and unset both fall back to $HOME/<fallback> (empty-is-unset per
    // the Base Directory spec).
    #[test]
    fn resolve_xdg_set_empty_unset() {
        use std::ffi::OsString;
        use std::path::PathBuf;
        let home = super::home().expect("HOME is set in any test environment");
        assert_eq!(
            super::resolve_xdg(Some(OsString::from("/xdg")), ".config").unwrap(),
            PathBuf::from("/xdg")
        );
        assert_eq!(
            super::resolve_xdg(Some(OsString::new()), ".config").unwrap(),
            home.join(".config")
        );
        assert_eq!(
            super::resolve_xdg(None, ".config").unwrap(),
            home.join(".config")
        );
    }

    // The default paths are anchored, never relative: home() is the real
    // $HOME and config_path(None) resolves under it (or under a real XDG
    // dir), ending at the documented location.
    #[test]
    fn default_paths_are_absolute_and_named() {
        let h = super::home().unwrap();
        assert!(h.is_absolute(), "home must be absolute: {h:?}");
        let p = super::config_path(None).unwrap();
        assert!(p.is_absolute(), "config path must be absolute: {p:?}");
        assert!(
            p.ends_with("ghgraph/config.json"),
            "documented location: {p:?}"
        );
    }

    // A non-string, non-object repos entry earns the visitor's expecting
    // message — the diagnosability the hand-written Deserialize exists for.
    #[test]
    fn repo_entry_wrong_type_names_expectation() {
        let err = serde_json::from_str::<RepoEntry>("42")
            .err()
            .expect("should error")
            .to_string();
        assert!(err.contains("a repo string"), "should say what fits: {err}");
    }

    // db_path honors an explicit override rather than deriving from XDG.
    #[test]
    fn db_path_uses_explicit_override() {
        let c = parse(
            r#"{"viewer":"v","repos":["o/n"],"db_path":"/tmp/ghgraph-x.db"}"#,
            "<test>",
        )
        .unwrap();
        assert_eq!(
            c.db_path().unwrap(),
            std::path::PathBuf::from("/tmp/ghgraph-x.db")
        );
    }
}
