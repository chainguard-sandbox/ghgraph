#![forbid(unsafe_code)]
//! ghgraph-mcp: the MCP server, as an external per-call wrapper that spawns
//! the `ghgraph` CLI (DESIGN.md, command surface). The process boundary is
//! the decision, and crash isolation is the reason: under forbid(unsafe)
//! and the standing catch_unwind rejection, a panic is containable only by
//! a process — here it is one child exiting nonzero with an empty stdout,
//! mapped to an INTERNAL envelope, and the session survives. The runner-up
//! was in-process per-call (the verbs are lib functions, so it too costs
//! zero new dependencies): it matches this shape on statelessness, config
//! freshness, and WAL safety, loses only the containment, and reopens only
//! if the catch_unwind rejection ever does. A resident reader loses more —
//! it is exactly the long-lived snapshot holder db.rs's WAL truncate names
//! as its defeat — and measured spawn latency pays for neither (ROADMAP,
//! deferred, carries the numbers).
//!
//! One tool per verb, seven tools total — the 1:1 mapping is the
//! constraint that keeps the CLI and the MCP server ONE surface with one
//! contract. The wrapper implements the protocol and NOTHING of ghgraph:
//! no verb logic, no archive access, no JSON reshaping of verb output — a
//! tool result carries the CLI's stdout document byte-for-byte (modulo the
//! contract's two enumerated timing fields, exactly what the one-surface
//! test masks), so every CLI invariant (one JSON document, typed
//! envelopes, determinism, in-band `_meta` freshness) is inherited by
//! construction rather than re-implemented.
//!
//! Transport: MCP over stdio — newline-delimited JSON-RPC 2.0, one message
//! per line, responses in one `write` under a lock (tool calls run on
//! threads so a read verb is never queued behind a multi-hour cold sync;
//! the archive already supports concurrent read-under-sync via WAL, and
//! serializing here would forfeit that for nothing). stdout carries ONLY
//! protocol frames; children inherit stderr, so verb progress ("ghgraph: "
//! lines) flows to the wrapper's stderr exactly as it does in a shell —
//! stderr stays non-contract on both surfaces. That routing sets the sync
//! posture: a COLD sync (minutes-to-hours, progress invisible to a client,
//! client tool timeouts short) belongs in an operator shell; the MCP-shaped
//! freshness path is the sync tool's `pr` argument — one bounded hydration,
//! now.
//!
//! Exit-code mapping, decided here: exit 0 → the document as text content;
//! nonzero with a document on stdout → the same text with `isError: true`
//! (the typed envelope's actor code — USER_INPUT, CONFIGURATION, TRANSIENT,
//! INTERNAL — rides inside, untranslated: MCP's error bit says "this call
//! failed", the envelope says who can fix it); nonzero with EMPTY stdout
//! (a killed or crashed child) → a synthesized INTERNAL envelope, matching
//! the CLI's own doctrine that empty-stdout-nonzero reads as INTERNAL.
//! Precondition, load-bearing under wrapper↔CLI version skew (two binaries
//! from one install can drift apart across an upgrade): main.rs intercepts
//! every clap refusal into a USER_INPUT envelope ON STDOUT, which is what
//! makes a skewed flag surface as the client's honest USER_INPUT — if that
//! interception regressed, skew would read as INTERNAL instead.
//!
//! Gate flags (`--strict`, `--fail-if-any`) are deliberately NOT exposed as
//! tool arguments: they exist to change a shell exit code, MCP has no exit
//! codes, and by the gate invariant they never change a byte of JSON — the
//! disclosed fields the gates read are already in the document the tool
//! returns. `query`'s read-sql-from-stdin form is likewise unexposed
//! (`sql` is a required argument; children get a null stdin). Cancellation
//! notifications are ignored on the same doctrine the CLI records:
//! cancellation is the absence of a handler — an MCP client that stops
//! caring drops the session, stdin EOFs, and in-flight children finish
//! against an archive whose writers are crash-safe anyway. There is no
//! lifecycle gate either — requests arriving before initialize are
//! served: the spec's pre-initialization SHOULDs bind the CLIENT, and a
//! stateless dispatch has no session state to desync; revisit if a
//! revision adds a server-side MUST.
//!
//! Accepted trades for a LOCAL, single-client transport, recorded: per-call
//! concurrency is uncapped (N pipelined calls = N threads and N children —
//! the client end of a stdio pipe is one process pacing itself; the join
//! ledger grows one handle per CALL for the session's lifetime, cumulative
//! rather than concurrent, a few dozen bytes each), stdin lines buffer
//! unbounded (the client is the operator's own agent, not a network peer),
//! and EOF shutdown waits on in-flight children without a deadline (a hung
//! child would be ghgraph's own bug; killing it here would trade a visible
//! hang for a hidden one). The no-deadline wait leans on a precondition
//! ghgraph itself provides: it bounds every gh interaction (gh.rs,
//! DRAIN_GRACE) and then exits, closing its stdout even when a gh-spawned
//! credential helper lingers holding gh's pipe — so a wait here ends when
//! ghgraph ends, not when the slowest grandchild does.
//!
//! Untrusted input is data, here too: tool arguments become argv VALUES,
//! never argv grammar — flag-shaped values cannot become flags because
//! every option is passed as one `--flag=value` token and positionals sit
//! behind a literal `--` end-of-options marker. Nothing from the client
//! reaches a shell (std spawns execve-style), the config path, or the
//! binary path: those come from the wrapper's OWN argv/environment, set by
//! whoever configured the MCP client.

use std::collections::BTreeMap;
use std::io::{BufRead, Write};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};

use clap::Parser;
use serde_json::{Value, json};

/// The protocol revision this wrapper implements — the only member of its
/// supported set, so initialize always answers it (the handler records the
/// negotiation rule). Everything this server uses (initialize / tools/list
/// / tools/call, newline-delimited stdio) is stable across published
/// revisions, so older clients interoperate in practice — but whether to
/// proceed on a version mismatch is the client's decision under the spec,
/// not a support claim this server may assert on its behalf.
const PROTOCOL_VERSION: &str = "2025-06-18";

#[derive(Parser)]
#[command(name = "ghgraph-mcp", version)]
/// MCP stdio server for ghgraph: seven tools, one per verb, each call
/// spawning the CLI. Configure your MCP client to run this binary; point
/// --ghgraph at the CLI if it is not adjacent or on PATH.
struct Cli {
    /// Path to the ghgraph binary (default: a `ghgraph` beside this
    /// executable, then `ghgraph` on PATH).
    #[arg(long)]
    ghgraph: Option<PathBuf>,

    /// Config file, passed through to every spawned verb (default:
    /// ghgraph's own default, $XDG_CONFIG_HOME/ghgraph/config.json).
    #[arg(long, env = "GHGRAPH_CONFIG")]
    config: Option<PathBuf>,
}

fn main() {
    let cli = Cli::parse();
    // Resolve the CLI once, at startup: a sibling `ghgraph` first (the
    // cargo-install and target-dir layouts both put the two binaries side
    // by side), then PATH. Resolution failures surface per CALL as a
    // spawn error inside a tool result — the server itself must come up
    // even when misconfigured, or the client shows a dead server instead
    // of an actionable message.
    let ghgraph = cli.ghgraph.unwrap_or_else(|| {
        let sibling = std::env::current_exe()
            .ok()
            .and_then(|p| p.parent().map(|d| d.join("ghgraph")))
            .filter(|p| p.is_file());
        sibling.unwrap_or_else(|| PathBuf::from("ghgraph"))
    });
    serve(Server {
        ghgraph,
        config: cli.config,
    });
}

struct Server {
    ghgraph: PathBuf,
    config: Option<PathBuf>,
}

/// The request loop: one JSON-RPC message per stdin line. tools/call runs
/// on a thread (responses interleave by id, which JSON-RPC licenses);
/// everything else answers inline. EOF drains in-flight calls, then exits
/// 0 — the MCP shutdown story is "close stdin", nothing else.
fn serve(server: Server) {
    let server = Arc::new(server);
    let stdout = Arc::new(Mutex::new(std::io::stdout()));
    let mut in_flight: Vec<std::thread::JoinHandle<()>> = Vec::new();

    let stdin = std::io::stdin();
    for line in stdin.lock().lines() {
        let line = match line {
            Ok(line) => line,
            // Non-UTF8 on the wire is a broken frame, not a shutdown:
            // lines() has already consumed through the newline, so the
            // session continues at the next frame — silently treating
            // this as EOF would drop everything after one flipped byte.
            // A genuine read error (the pipe itself failing) ends the
            // session like EOF does. Mutation note: widening this guard to
            // all errors survives the suite — the discriminating input is
            // a non-InvalidData stdin failure (EIO) no portable harness
            // constructs; held by review, the main.rs BrokenPipe precedent.
            Err(e) if e.kind() == std::io::ErrorKind::InvalidData => {
                respond(
                    &stdout,
                    &rpc_error(Value::Null, -32700, "parse error: frame is not UTF-8"),
                );
                continue;
            }
            Err(_) => break,
        };
        if line.trim().is_empty() {
            continue;
        }
        let msg: Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(e) => {
                // Parse errors have no id to echo; -32700 with id:null per
                // JSON-RPC 2.0.
                respond(
                    &stdout,
                    &rpc_error(Value::Null, -32700, &format!("parse error: {e}")),
                );
                continue;
            }
        };
        // A request is an OBJECT carrying jsonrpc "2.0" and a string
        // method: anything else that parsed as JSON (a bare string, an
        // array — batching left the spec — a wrong or absent version) is
        // an invalid request, not a notification to swallow.
        if !msg.is_object() || msg["jsonrpc"] != "2.0" || !msg["method"].is_string() {
            respond(
                &stdout,
                &rpc_error(
                    msg.get("id").cloned().unwrap_or(Value::Null),
                    -32600,
                    "invalid request: expected an object with jsonrpc \"2.0\" and a \
                     string method",
                ),
            );
            continue;
        }
        let id = msg.get("id").cloned();
        // MCP narrows JSON-RPC here: an id, when present, MUST NOT be
        // null — a null id is an invalid request, not a notification (the
        // absence of the member is what makes a notification) and not a
        // request to service under a sentinel id.
        if id == Some(Value::Null) {
            respond(
                &stdout,
                &rpc_error(
                    Value::Null,
                    -32600,
                    "invalid request: id must not be null (omit id to send a \
                     notification)",
                ),
            );
            continue;
        }
        let method = msg.get("method").and_then(Value::as_str).unwrap_or("");
        match (method, id) {
            // Notifications (no id) are consumed without response;
            // notifications/cancelled deliberately so (module docs).
            (_, None) => {}
            ("initialize", Some(id)) => {
                // Version negotiation, by the spec's rule: answer the
                // requested revision when the server supports it,
                // otherwise a revision the server DOES support — and the
                // client decides whether to proceed. The supported set
                // here has size one, so the answer is constant. Echoing
                // an unsupported request (an older revision, or garbage)
                // was tried and rejected: it asserts support nobody
                // implemented, and interop with older clients — real,
                // since every surface this server uses is stable across
                // published revisions — is the client's call to make,
                // not ours (PROTOCOL_VERSION docs).
                respond(
                    &stdout,
                    &rpc_result(
                        id,
                        json!({
                            "protocolVersion": PROTOCOL_VERSION,
                            "capabilities": { "tools": { "listChanged": false } },
                            "serverInfo": {
                                "name": "ghgraph-mcp",
                                "version": env!("CARGO_PKG_VERSION"),
                            },
                            "instructions":
                                "Local GitHub work memory: query an offline SQLite archive of \
                                 PRs, review threads, and issues synced via the gh CLI. Every \
                                 read carries in-band `_meta` freshness (age, stale, hint) — \
                                 advisory, reads never fail stale; run the sync tool when it \
                                 says so. Coverage is the configured repos only: an empty \
                                 result means not-in-archive, never nonexistent-on-GitHub. \
                                 Start from attention (what needs the operator) or prs (the \
                                 listing) and follow their references into pr. Results are \
                                 deterministic JSON documents.",
                        }),
                    ),
                );
            }
            ("ping", Some(id)) => respond(&stdout, &rpc_result(id, json!({}))),
            ("tools/list", Some(id)) => {
                respond(&stdout, &rpc_result(id, json!({ "tools": tools() })));
            }
            ("tools/call", Some(id)) => {
                let server = Arc::clone(&server);
                let stdout = Arc::clone(&stdout);
                let params = msg.get("params").cloned().unwrap_or(Value::Null);
                in_flight.push(std::thread::spawn(move || {
                    let response = match call(&server, &params) {
                        Ok(result) => rpc_result(id, result),
                        Err(msg) => rpc_error(id, -32602, &msg),
                    };
                    respond(&stdout, &response);
                }));
            }
            (_, Some(id)) => {
                respond(
                    &stdout,
                    &rpc_error(id, -32601, &format!("method not found: {method:?}")),
                );
            }
        }
    }
    for handle in in_flight {
        let _ = handle.join();
    }
}

/// One response frame: compact JSON, one line, one write under the lock so
/// threaded tool results never interleave bytes. A closed stdout means the
/// client is gone — exit 0 quietly, the CLI's own EPIPE posture.
fn respond(stdout: &Mutex<std::io::Stdout>, frame: &Value) {
    let mut out = stdout.lock().expect("stdout lock");
    if writeln!(out, "{frame}").and_then(|()| out.flush()).is_err() {
        std::process::exit(0);
    }
}

fn rpc_result(id: Value, result: Value) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "result": result })
}

fn rpc_error(id: Value, code: i64, message: &str) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } })
}

// ---------------------------------------------------------------------------
// The tool table: one entry per verb, schema mirroring the CLI flags it
// exposes. Kept as data so tools/list and the call dispatcher cannot drift.
//
// Tools only — MCP resources were considered and REJECTED for the reads:
// they fit a read-mostly archive on paper, but a resource read is not a
// CLI verb invocation (exposing one forks the one-surface contract), and
// resources are model-passive by design where this consumer is an agent
// driving parameterized calls — a shape real clients also under-support
// next to tools. Revisit if client resource support reaches parity.

/// Validated tool arguments, keyed by argument name.
type Args = BTreeMap<String, Value>;
/// An argv builder: the arguments AFTER the subcommand, or a validation
/// error naming the offending argument.
type ArgvBuilder = fn(&Args) -> Result<Vec<String>, String>;

/// One verb's tool entry. Sharing the table ties tools/list to dispatch by
/// construction; the schema↔builder KEY agreement it cannot tie (two
/// independent fn fields relate by convention), so the table test below
/// pins it: every declared property is read by its builder, every
/// `required` key enforced by it.
struct Tool {
    name: &'static str,
    description: &'static str,
    schema: fn() -> Value,
    argv: ArgvBuilder,
    /// Spec tool annotations — hints for client scheduling and approval
    /// UX (absent hints read as worst-case), never enforcement: the
    /// read-only guarantee lives at the connection (read-only open, one
    /// prepared statement), not in this JSON.
    annotations: fn() -> Value,
}

/// The six read verbs' annotations: local, no mutation.
fn read_annotations() -> Value {
    json!({ "readOnlyHint": true, "openWorldHint": false })
}

fn tools_table() -> &'static [Tool] {
    &[
        Tool {
            name: "attention",
            description: "What needs the operator's attention, derived from archive \
                          state. Buckets: waiting_on_me, they_replied (activity by \
                          another party since your last, on PRs you participate \
                          in), ready_to_merge, people_prs (tracked people's open \
                          PRs) — plus needs_reviewer and untriaged issues for \
                          project-scope repos. Uncertainty only escalates: it can \
                          add to waiting_on_me, never qualify ready_to_merge.",
            schema: || {
                obj_schema(
                    json!({
                        "limit": { "type": "integer", "minimum": 0,
                            "description": "Cap rows per bucket; totals stay disclosed." },
                    }),
                    &[],
                )
            },
            argv: |args| {
                let mut v = vec![];
                push_uint(&mut v, args, "limit")?;
                Ok(v)
            },
            annotations: read_annotations,
        },
        Tool {
            name: "pr",
            description: "One PR from the archive, whole: reviews (effective review \
                          state, per-review freshness), threads, comments, refs, \
                          and linked issues. Pass the qualified reference — \
                          owner/name#123 or the GitHub URL (bare numbers are a \
                          CLI-cwd convenience that does not exist here); prs, \
                          attention, and search results all carry it.",
            schema: || {
                obj_schema(
                    json!({
                        "reference": { "type": "string",
                            "description": "\"owner/name#123\" or a GitHub PR URL." },
                        "max_body_bytes": { "type": "integer", "minimum": 0,
                            "description": "Truncate each body to at most this many bytes \
                                            (code-point safe); elided bodies say so." },
                    }),
                    &["reference"],
                )
            },
            argv: |args| {
                let mut v = vec![];
                push_uint(&mut v, args, "max_body_bytes")?;
                let reference = req_string(args, "reference")?;
                // The description's "bare numbers do not exist here" is
                // this refusal, not a hope: the CLI resolves a bare number
                // through --repo and then the CWD GIT REMOTE — and the
                // wrapper's cwd is wherever the MCP client launched it, an
                // input no tool argument controls. Qualified forms only.
                if !reference.contains('#') && !reference.contains("://") {
                    return Err("reference: pass the qualified form — owner/name#123 or a \
                         GitHub PR URL (bare numbers would resolve against the \
                         server's launch directory)"
                        .into());
                }
                v.push("--".into());
                v.push(reference);
                Ok(v)
            },
            annotations: read_annotations,
        },
        Tool {
            name: "prs",
            description: "List PRs in the archive (open by default; all=true adds \
                          merged/closed/deleted). The matching total is always \
                          disclosed — limits govern presentation, never derivation. \
                          For one PR's full detail, use the pr tool.",
            schema: || {
                obj_schema(
                    json!({
                        "repo": { "type": "string", "description": "owner/name filter." },
                        "author": { "type": "string",
                            "description": "Only PRs authored by this login." },
                        "all": { "type": "boolean",
                            "description": "Include merged, closed, and upstream-deleted PRs." },
                        "limit": { "type": "integer", "minimum": 0,
                            "description": "Cap returned rows; the matching total \
                                            stays disclosed." },
                    }),
                    &[],
                )
            },
            argv: |args| {
                let mut v = vec![];
                push_string(&mut v, args, "repo")?;
                push_string(&mut v, args, "author")?;
                push_flag(&mut v, args, "all")?;
                push_uint(&mut v, args, "limit")?;
                Ok(v)
            },
            annotations: read_annotations,
        },
        Tool {
            name: "query",
            description: "One read-only SQL statement against the archive (SQLite; \
                          FTS5 available). One statement per call, no parameters — \
                          inline values. The schema is introspectable \
                          (sqlite_master). For plain text lookup, the search tool \
                          is the shorter path; this one is for counts, joins, and \
                          filters.",
            schema: || {
                obj_schema(
                    json!({
                        "sql": { "type": "string", "description": "The SELECT to run." },
                        "limit": { "type": "integer", "minimum": 0,
                            "description": "Row cap (default 100); truncation is disclosed." },
                    }),
                    &["sql"],
                )
            },
            argv: |args| {
                let mut v = vec![];
                push_uint(&mut v, args, "limit")?;
                v.push("--".into());
                v.push(req_string(args, "sql")?);
                Ok(v)
            },
            annotations: read_annotations,
        },
        Tool {
            name: "search",
            description: "Full-text search over archived PR/issue titles+bodies and \
                          comments (FTS5 syntax: terms, quoted phrases, AND/OR/NOT). \
                          Results group by PR/issue, recency-ordered. For \
                          structural or aggregate questions, use the query tool \
                          (SQL).",
            schema: || {
                obj_schema(
                    json!({
                        "query": { "type": "string", "description": "FTS5 match expression." },
                        "limit": { "type": "integer", "minimum": 0,
                            "description": "Result-group cap (default 20)." },
                    }),
                    &["query"],
                )
            },
            argv: |args| {
                let mut v = vec![];
                push_uint(&mut v, args, "limit")?;
                v.push("--".into());
                v.push(req_string(args, "query")?);
                Ok(v)
            },
            annotations: read_annotations,
        },
        Tool {
            name: "stats",
            description: "Archive counts, size, per-repo sync state and staleness, \
                          run-trend telemetry, and the integrity audits (an intact \
                          archive reads all zeros).",
            schema: || obj_schema(json!({}), &[]),
            // Unknown keys are already refused upstream against the empty
            // schema (call()), so the builder has nothing to check.
            argv: |_| Ok(vec![]),
            annotations: read_annotations,
        },
        Tool {
            name: "sync",
            description: "Fetch configured repos into the archive via the gh CLI \
                          (network; a concurrent sync returns a TRANSIENT \
                          already-running envelope). A COLD start runs \
                          minutes-to-hours and belongs in an operator shell, \
                          where progress is visible — here, an incremental \
                          sync is quick and pr=\"owner/name#123\" hydrates one \
                          PR now, the read-time freshness path; full=true \
                          ignores watermarks and refetches the lookback window.",
            schema: || {
                obj_schema(
                    json!({
                        "full": { "type": "boolean",
                            "description": "Refetch the whole lookback window." },
                        "pr": { "type": "string",
                            "description": "Hydrate one PR now (owner/name#123 or URL); \
                                            mutually exclusive with full." },
                    }),
                    &[],
                )
            },
            argv: |args| {
                let mut v = vec![];
                push_flag(&mut v, args, "full")?;
                push_string(&mut v, args, "pr")?;
                Ok(v)
            },
            // Writes, but honestly: upserts and soft deletes only, never
            // destruction (destructiveHint false), and an unchanged resync
            // writes nothing — fixture-replay proven — so retries are safe
            // (idempotentHint true). openWorld: the network, via gh.
            annotations: || {
                json!({
                    "readOnlyHint": false,
                    "destructiveHint": false,
                    "idempotentHint": true,
                    "openWorldHint": true,
                })
            },
        },
    ]
}

/// The tools/list payload, generated from the same table calls dispatch on.
fn tools() -> Vec<Value> {
    tools_table()
        .iter()
        .map(|t| {
            json!({
                "name": t.name,
                "description": t.description,
                "inputSchema": (t.schema)(),
                "annotations": (t.annotations)(),
            })
        })
        .collect()
}

fn obj_schema(properties: Value, required: &[&str]) -> Value {
    json!({
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": false,
    })
}

// ---------------------------------------------------------------------------
// Argument validation: JSON in, argv values out. Every option becomes ONE
// `--flag=value` token and positionals sit behind `--`, so a value shaped
// like a flag stays a value (pinned by test against the live CLI).

/// A NUL cannot travel in Unix argv: catching it here keeps the refusal a
/// protocol-level invalid-params naming the argument, instead of a spawn
/// error blamed on the binary path (the actor-honesty rule).
fn no_nul(key: &str, s: &str) -> Result<(), String> {
    if s.contains('\0') {
        return Err(format!(
            "{key}: contains a NUL byte, which argv cannot carry"
        ));
    }
    Ok(())
}

fn req_string(args: &Args, key: &str) -> Result<String, String> {
    match args.get(key) {
        Some(Value::String(s)) => {
            no_nul(key, s)?;
            Ok(s.clone())
        }
        Some(_) => Err(format!("{key}: expected a string")),
        None => Err(format!("{key}: required")),
    }
}

fn push_string(v: &mut Vec<String>, args: &Args, key: &str) -> Result<(), String> {
    match args.get(key) {
        None => Ok(()),
        Some(Value::String(s)) => {
            no_nul(key, s)?;
            v.push(format!("--{}={s}", key.replace('_', "-")));
            Ok(())
        }
        Some(_) => Err(format!("{key}: expected a string")),
    }
}

fn push_uint(v: &mut Vec<String>, args: &Args, key: &str) -> Result<(), String> {
    match args.get(key) {
        None => Ok(()),
        Some(Value::Number(n)) if n.as_u64().is_some() => {
            v.push(format!(
                "--{}={}",
                key.replace('_', "-"),
                n.as_u64().unwrap()
            ));
            Ok(())
        }
        Some(_) => Err(format!("{key}: expected a non-negative integer")),
    }
}

fn push_flag(v: &mut Vec<String>, args: &Args, key: &str) -> Result<(), String> {
    match args.get(key) {
        None | Some(Value::Bool(false)) => Ok(()),
        Some(Value::Bool(true)) => {
            v.push(format!("--{key}"));
            Ok(())
        }
        Some(_) => Err(format!("{key}: expected a boolean")),
    }
}

// ---------------------------------------------------------------------------
// The call path: validate → spawn → map. Err(String) is a PROTOCOL-level
// invalid-params (-32602: the client sent something the schema forbids);
// everything after a successful spawn decision is a RESULT, error or not —
// a failing verb is the tool working, and its envelope names the actor.

fn call(server: &Server, params: &Value) -> Result<Value, String> {
    let name = params
        .get("name")
        .and_then(Value::as_str)
        .ok_or("name: required")?;
    let tool = tools_table()
        .iter()
        .find(|t| t.name == name)
        .ok_or_else(|| format!("unknown tool: {name:?}"))?;
    let args: Args = match params.get("arguments") {
        None | Some(Value::Null) => BTreeMap::new(),
        Some(Value::Object(m)) => {
            if let Some(unknown) = m
                .keys()
                .find(|k| (tool.schema)()["properties"].get(k.as_str()).is_none())
            {
                return Err(format!("{unknown}: unknown argument for {name}"));
            }
            m.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
        }
        Some(_) => return Err("arguments: expected an object".into()),
    };
    let argv = (tool.argv)(&args)?;

    let mut cmd = Command::new(&server.ghgraph);
    if let Some(config) = &server.config {
        cmd.arg("--config").arg(config);
    }
    cmd.arg(name);
    cmd.args(&argv);
    // Null stdin (no tool reads it — module docs); stdout captured whole
    // (one JSON document by the CLI contract); stderr inherited so verb
    // progress stays visible and non-contract.
    cmd.stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    let output = match cmd.output() {
        Ok(o) => o,
        // Spawn failures split by actor: a missing or unrunnable binary is
        // CONFIGURATION with the install remedy; an argv the OS refuses
        // (past ARG_MAX) traces to the CALL's inputs, and blaming the
        // binary path would launder a client's oversized argument into a
        // reinstall chase — the blanket-From rule, one code over.
        Err(e)
            if matches!(
                e.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::PermissionDenied
            ) =>
        {
            return Ok(tool_result(
                &json!({ "error": { "code": "CONFIGURATION", "message": format!(
                    "cannot run ghgraph at {}: {e} — install it beside ghgraph-mcp, \
                     put it on PATH, or pass --ghgraph",
                    server.ghgraph.display()
                )}})
                .to_string(),
                true,
            ));
        }
        Err(e) if e.kind() == std::io::ErrorKind::ArgumentListTooLong => {
            return Ok(tool_result(
                &json!({ "error": { "code": "USER_INPUT", "message": format!(
                    "cannot spawn ghgraph with these arguments: {e}"
                )}})
                .to_string(),
                true,
            ));
        }
        // The residue is resource pressure in every population a review
        // could name — EMFILE/ENFILE/ENOMEM/EAGAIN, the failure mode of
        // the accepted uncapped-concurrency trade (module docs) — and it
        // clears as in-flight calls drain: TRANSIENT, the retry actor.
        // Calling it USER_INPUT (as this arm once did) blamed the one
        // party who cannot fix it. Untestable by construction: no
        // portable harness constructs a spawn failure that is neither
        // NotFound/PermissionDenied nor E2BIG (ENOEXEC does not qualify —
        // posix_spawn launches the child, which dies at exec, landing in
        // the empty-stdout INTERNAL path below); the ArgumentListTooLong
        // guard's widening mutant survives on that argument, ledgered in
        // .cargo/mutants.toml.
        Err(e) => {
            return Ok(tool_result(
                &json!({ "error": { "code": "TRANSIENT", "message": format!(
                    "cannot spawn ghgraph: {e} — likely resource pressure from \
                     concurrent calls; retry after in-flight calls finish"
                )}})
                .to_string(),
                true,
            ));
        }
    };
    let stdout = String::from_utf8_lossy(&output.stdout);
    let doc = stdout.trim();
    if output.status.success() {
        return Ok(tool_result(doc, false));
    }
    if doc.is_empty() || serde_json::from_str::<Value>(doc).is_err() {
        // Empty stdout + abnormal exit reads as INTERNAL — the CLI's own
        // doctrine (DESIGN.md, command surface), synthesized here because
        // the child never got to say it. A NON-document (a child killed
        // mid-write leaves a fragment) gets the same treatment, fragment
        // attached: passing it through as tool text would hand consumers
        // the one thing the surface promises never to emit — partial JSON.
        return Ok(tool_result(
            &json!({ "error": { "code": "INTERNAL", "message": format!(
                "ghgraph exited abnormally ({}) without a JSON document — file a \
                 ghgraph bug{}",
                output.status,
                if doc.is_empty() {
                    String::new()
                } else {
                    format!(" (partial output: {doc:.120})")
                }
            )}})
            .to_string(),
            true,
        ));
    }
    // Nonzero WITH a document: the typed envelope (exit 2). Exit 1 cannot
    // happen — gate flags are not exposed — but mapping any nonzero the
    // same way stays total without inventing a third state.
    Ok(tool_result(doc, true))
}

fn tool_result(text: &str, is_error: bool) -> Value {
    json!({
        "content": [ { "type": "text", "text": text } ],
        "isError": is_error,
    })
}

#[cfg(test)]
mod table {
    use super::*;

    /// One representative value per schema type. The string doubles as a
    /// qualified PR reference so it passes `pr`'s bare-number gate — the
    /// one builder that inspects a value beyond its type.
    fn sample(spec: &Value) -> Value {
        match spec["type"].as_str().expect("every property is typed") {
            "string" => json!("octo/alpha#1"),
            "integer" => json!(1),
            "boolean" => json!(true),
            other => panic!("unhandled schema type {other} — extend sample()"),
        }
    }

    /// The schema↔builder agreement the Tool comment claims, made a
    /// mechanism: a declared property no builder reads would be a silently
    /// ignored argument (it passes the unknown-key gate, then vanishes),
    /// and an unenforced `required` key would surface as a confusing CLI
    /// error blamed on the wrong actor. Both directions, per tool.
    #[test]
    fn every_schema_key_is_read_and_every_required_key_enforced() {
        for tool in tools_table() {
            let schema = (tool.schema)();
            let props = schema["properties"]
                .as_object()
                .expect("object schema")
                .clone();
            let required: Vec<&str> = schema["required"]
                .as_array()
                .expect("required is a list")
                .iter()
                .map(|k| k.as_str().unwrap())
                .collect();
            for key in &required {
                assert!(
                    props.contains_key(*key),
                    "{}: required key {key} is not a declared property",
                    tool.name
                );
            }
            let base: Args = props
                .iter()
                .filter(|(k, _)| required.contains(&k.as_str()))
                .map(|(k, spec)| (k.clone(), sample(spec)))
                .collect();
            for (key, spec) in &props {
                let mut with = base.clone();
                with.insert(key.clone(), sample(spec));
                let argv_with = (tool.argv)(&with)
                    .unwrap_or_else(|e| panic!("{}: {key} sample refused: {e}", tool.name));
                let mut without = with.clone();
                without.remove(key);
                match (tool.argv)(&without) {
                    // Optional key: its presence must change the argv, or
                    // the builder never read it.
                    Ok(argv_without) => assert_ne!(
                        argv_with, argv_without,
                        "{}: declared property {key} is never read by the builder",
                        tool.name
                    ),
                    // Refusing the removal is enforcement — and only a
                    // key the schema marks required may be enforced.
                    Err(msg) => assert!(
                        required.contains(&key.as_str()),
                        "{}: builder demands {key} but the schema does not: {msg}",
                        tool.name
                    ),
                }
            }
            for key in &required {
                let mut without = base.clone();
                without.remove(*key);
                let err = (tool.argv)(&without).expect_err(&format!(
                    "{}: required key {key} is not enforced by the builder",
                    tool.name
                ));
                assert!(
                    err.contains(key),
                    "{}: the {key} refusal does not name the argument: {err}",
                    tool.name
                );
            }
        }
    }
}
