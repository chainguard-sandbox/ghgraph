use std::env;
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::error::{Error, Result};

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
    Name(String),
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
                Ok(RepoEntry::Name(v.to_string()))
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
    /// "owner/name".
    pub repo: String,
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
    /// function, called everywhere logins compare (milestone 1):
    /// ASCII-case-insensitive; "x[bot]" matches login "x" with GraphQL
    /// author type Bot (the API returns bare logins for bots — a literal
    /// bracket match would never fire); bare "x" matches a User. Applied at
    /// discovery (excluded PRs are skipped before hydration, so they cost
    /// discovery only) and enforced at ingest. Filters govern ingest, never
    /// deletion: excluding an author later does not touch their archived
    /// rows. Note bot-typed authors are already excluded by default at
    /// project scope — exclude_authors is for humans and for bots you have
    /// opted back in via bots: true.
    #[serde(default)]
    pub exclude_authors: Vec<String>,
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
    /// Shorthand → full form; defaults resolve here and nowhere else. The
    /// repo is case-folded to lowercase: GitHub treats owner/name
    /// case-insensitively, so folding at the boundary (and at API ingest)
    /// keeps `Foo/Bar` and `foo/bar` from splitting the (repo, number) key
    /// or tripping rename detection against the canonical `nameWithOwner`.
    pub fn resolved(&self) -> RepoConfig {
        let mut rc = match self {
            RepoEntry::Detailed(rc) => rc.clone(),
            RepoEntry::Name(name) => RepoConfig {
                repo: name.clone(),
                scope: Scope::Working,
                issues: None,
                lookback_days: None,
                bots: None,
                exclude_authors: Vec::new(),
            },
        };
        rc.repo = rc.repo.to_ascii_lowercase();
        rc
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// The operator's GitHub login. "my PRs" vs "PRs I review" derives from
    /// this at query time; it is never stored per row.
    pub viewer: String,
    /// Repositories to sync; see RepoEntry.
    pub repos: Vec<RepoEntry>,
    /// GitHub logins to track in working-scope repos: collaborators or
    /// contributors the operator opts in. Their involvement (authored,
    /// assigned, mentioned, commented) is discovered and archived alongside
    /// the operator's own; `attention` surfaces their unreviewed open PRs.
    /// Project-scope repos subsume this — everyone is already in.
    #[serde(default)]
    pub people: Vec<String>,
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
    match env::var_os(var) {
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

/// The injection gate. Every identifier is interpolated into a GitHub search
/// qualifier (queries.rs); a value containing a space or ':' could smuggle a
/// second qualifier — "owner/name involves:someone-else" — so validate at the
/// boundary. This charset gate is the interim; the validating RepoName/Login
/// newtypes that make injection unrepresentable by type land in milestone 1.
fn validate(cfg: &Config) -> Result<()> {
    for login in cfg.people.iter().chain([&cfg.viewer]) {
        if !is_login(login) {
            return Err(Error::config(format!(
                "login {login:?} is not a valid GitHub login (letters, digits, hyphen)"
            )));
        }
    }
    for entry in &cfg.repos {
        let rc = entry.resolved();
        if !is_repo(&rc.repo) {
            return Err(Error::config(format!(
                "repo {:?} is not of the form owner/name",
                rc.repo
            )));
        }
        for a in &rc.exclude_authors {
            if !is_exclude_author(a) {
                return Err(Error::config(format!(
                    "exclude_authors entry {a:?} is not a valid login (optionally with a [bot] suffix)"
                )));
            }
        }
        if rc.scope == Scope::Working && rc.issues() {
            return Err(Error::config(format!(
                "repo {:?}: linked issues are already cached at working scope; \
                 the issue *stream* requires scope: project",
                rc.repo
            )));
        }
    }
    Ok(())
}

fn is_login(s: &str) -> bool {
    !s.is_empty() && s.len() <= 39 && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
}

fn is_repo(s: &str) -> bool {
    match s.split_once('/') {
        Some((owner, name)) => {
            is_login(owner)
                && !name.is_empty()
                && name
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.')
        }
        None => false,
    }
}

fn is_exclude_author(s: &str) -> bool {
    is_login(s.strip_suffix("[bot]").unwrap_or(s))
}

#[cfg(test)]
mod tests {
    use super::{RepoEntry, is_exclude_author, is_login, is_repo, parse};

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

    // Repo is case-folded at the boundary so Foo/Bar and foo/bar are one key.
    #[test]
    fn repo_entry_lowercases() {
        let e: RepoEntry = serde_json::from_str(r#""Foo/Bar""#).unwrap();
        assert_eq!(e.resolved().repo, "foo/bar");
    }

    // is_repo must reject malformed forms ("/owner/name", "owner//name",
    // "owner/name/") and the ':'/space forms that could smuggle a second
    // search qualifier — the reason the validation exists. A charset
    // allowlist enforces both; a slash-count check would not.
    #[test]
    fn repo_rejects_malformed_and_injection() {
        for bad in [
            "/owner/name",
            "owner//name",
            "owner/name/",
            "owner",
            "owner/",
            "/name",
            "",
            "owner/na me",
            "owner/na:me",
            "owner/name involves:x",
            "a/b is:issue",
        ] {
            assert!(!is_repo(bad), "should reject {bad:?}");
        }
        for ok in ["owner/name", "o/n", "owner/name.rs", "o-w/n_1"] {
            assert!(is_repo(ok), "should accept {ok:?}");
        }
    }

    #[test]
    fn login_rejects_qualifier_injection() {
        for bad in [
            "me involves:target",
            "a:b",
            "has space",
            "",
            &"x".repeat(40),
        ] {
            assert!(!is_login(bad), "should reject {bad:?}");
        }
        assert!(is_login("octocat"));
        assert!(is_login("a-b-1"));
    }

    #[test]
    fn exclude_author_admits_bot_suffix() {
        assert!(is_exclude_author("dependabot[bot]"));
        assert!(is_exclude_author("alice"));
        assert!(!is_exclude_author("bad login[bot]"));
    }
}
