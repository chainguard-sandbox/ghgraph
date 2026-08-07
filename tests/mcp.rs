//! The MCP wrapper suite: the one-surface invariant (a tool result IS the
//! CLI's stdout document, byte-for-byte modulo the contract's enumerated
//! timing fields), the typed envelope riding through `isError` with its
//! actor code intact, argv injection-proofing witnessed against the LIVE
//! CLI (a flag-shaped value must reach the verb as data), and the JSON-RPC
//! protocol edges. The driver writes every request, closes stdin, reads to
//! EOF, and indexes responses by id — tool calls run on threads in the
//! server, so arrival order is licensed to differ from request order and
//! the suite must not accidentally pin it.

use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};

use serde_json::{Value, json};

use ghgraph::db;

struct Scratch {
    dir: PathBuf,
}

impl Scratch {
    fn new() -> Scratch {
        static N: AtomicU32 = AtomicU32::new(0);
        let dir = std::env::temp_dir().join(format!(
            "ghgraph-mcp-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        Scratch { dir }
    }

    fn db_path(&self) -> PathBuf {
        self.dir.join("archive/ghgraph.db")
    }

    fn config_path(&self) -> PathBuf {
        self.dir.join("config.json")
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// A minimal archive: one synced repo, one open PR — enough for every read
/// verb to produce a non-trivial document.
fn seed(s: &Scratch) {
    std::fs::write(
        s.config_path(),
        json!({
            "viewer": "me",
            "repos": ["octo/alpha"],
            "db_path": s.db_path().to_str().unwrap(),
        })
        .to_string(),
    )
    .unwrap();
    let arch = db::open_rw(&s.db_path()).unwrap();
    arch.conn()
        .execute_batch(
            "INSERT INTO sync_state (repo, stream, last_item_updated_at, last_checked_at, \
                                     runs_since_advance, fingerprint) \
             VALUES ('octo/alpha', 'pr', '2026-01-05T00:00:00Z', '2026-01-05T00:10:00Z', 0, \
                     '{\"bots\":true,\"exclude_authors\":[],\"lookback_days\":90,\
                       \"people\":[],\"scope\":\"working\",\"viewer\":\"me\"}'); \
             INSERT INTO prs (id, repo, number, title, body, state, author, created_at, \
                              updated_at, url) \
             VALUES ('PR_1', 'octo/alpha', 1, 'Fix the frobnicator', 'searchable body', \
                     'OPEN', 'alice', '2026-01-01T00:00:00Z', '2026-01-02T00:00:00Z', \
                     'https://github.com/octo/alpha/pull/1')",
        )
        .unwrap();
}

/// Send every request line, close stdin, collect responses to EOF, keyed
/// by id (null-id errors key under Value::Null). Returns the map and the
/// server's exit status.
fn drive(s: &Scratch, requests: &[Value]) -> (HashMap<String, Value>, std::process::ExitStatus) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_ghgraph-mcp"))
        .arg("--ghgraph")
        .arg(env!("CARGO_BIN_EXE_ghgraph"))
        .arg("--config")
        .arg(s.config_path())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn ghgraph-mcp");
    {
        let mut stdin = child.stdin.take().unwrap();
        for req in requests {
            writeln!(stdin, "{req}").unwrap();
        }
        // Dropping stdin closes it: the server drains in-flight calls and
        // exits — the whole shutdown contract.
    }
    let out = child.wait_with_output().expect("collect ghgraph-mcp");
    let mut by_id = HashMap::new();
    for line in String::from_utf8(out.stdout).unwrap().lines() {
        let v: Value = serde_json::from_str(line).expect("every frame parses");
        assert_eq!(v["jsonrpc"], "2.0", "every frame is JSON-RPC 2.0");
        by_id.insert(v["id"].to_string(), v);
    }
    (by_id, out.status)
}

fn init_request(id: u64) -> Value {
    json!({"jsonrpc":"2.0","id":id,"method":"initialize","params":{
        "protocolVersion":"2025-06-18","capabilities":{},
        "clientInfo":{"name":"suite","version":"0"}}})
}

fn call(id: u64, name: &str, arguments: Value) -> Value {
    json!({"jsonrpc":"2.0","id":id,"method":"tools/call",
           "params":{"name":name,"arguments":arguments}})
}

/// The document inside a tool result's text content, parsed.
fn tool_doc(resp: &Value) -> (Value, bool) {
    let result = &resp["result"];
    let text = result["content"][0]["text"].as_str().expect("text content");
    (
        serde_json::from_str(text).expect("tool text is one JSON document"),
        result["isError"].as_bool().expect("isError is present"),
    )
}

/// Mask the contract's enumerated nondeterminism — generated_at and
/// age_seconds, the same two fields the golden suite masks — so documents
/// from two invocations compare byte-equal.
fn mask(doc: &mut Value) {
    if let Some(meta) = doc.get_mut("_meta") {
        meta["generated_at"] = json!("<TIME>");
        if let Some(archive) = meta.get_mut("archive").and_then(Value::as_array_mut) {
            for repo in archive {
                if let Some(streams) = repo.get_mut("streams").and_then(Value::as_array_mut) {
                    for s in streams {
                        s["age_seconds"] = json!("<AGE>");
                    }
                }
            }
        }
    }
}

#[test]
fn handshake_lists_the_seven_verbs() {
    let s = Scratch::new();
    seed(&s);
    let (resp, status) = drive(
        &s,
        &[
            init_request(1),
            json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
            json!({"jsonrpc":"2.0","id":2,"method":"tools/list"}),
        ],
    );
    assert!(status.success(), "EOF is a clean exit");
    let init = &resp["1"]["result"];
    assert_eq!(init["protocolVersion"], "2025-06-18", "echoes the request");
    assert_eq!(init["serverInfo"]["name"], "ghgraph-mcp");
    let tools: Vec<&str> = resp["2"]["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert_eq!(
        tools,
        ["attention", "pr", "prs", "query", "search", "stats", "sync"],
        "one tool per verb, deterministic order"
    );
    for tool in resp["2"]["result"]["tools"].as_array().unwrap() {
        assert_eq!(
            tool["inputSchema"]["additionalProperties"], false,
            "{}: closed schema",
            tool["name"]
        );
        let text = tool.to_string();
        assert!(
            !text.contains("strict") && !text.contains("fail_if_any"),
            "gate flags are exit-code plumbing and stay off the MCP surface"
        );
    }
}

#[test]
fn tool_result_is_the_cli_document() {
    // The one-surface invariant, witnessed: what the tool returns IS what
    // the CLI prints, byte-for-byte after masking the two enumerated
    // timing fields. Any wrapper reshaping — reordering, renumbering,
    // "helpfully" summarizing — fails this.
    let s = Scratch::new();
    seed(&s);
    for (name, arguments, argv) in [
        ("stats", json!({}), vec!["stats"]),
        (
            "attention",
            json!({"limit": 5}),
            vec!["attention", "--limit=5"],
        ),
        (
            "prs",
            json!({"repo": "octo/alpha", "all": true}),
            vec!["prs", "--repo=octo/alpha", "--all"],
        ),
        (
            "search",
            json!({"query": "searchable"}),
            vec!["search", "--", "searchable"],
        ),
        (
            "query",
            json!({"sql": "SELECT count(*) FROM prs"}),
            vec!["query", "--", "SELECT count(*) FROM prs"],
        ),
    ] {
        let direct = Command::new(env!("CARGO_BIN_EXE_ghgraph"))
            .arg("--config")
            .arg(s.config_path())
            .args(&argv)
            .output()
            .unwrap();
        assert!(direct.status.success(), "{name}: CLI baseline runs");
        let mut cli_doc: Value = serde_json::from_slice(&direct.stdout).expect("one JSON document");

        let (resp, _) = drive(&s, &[init_request(1), call(2, name, arguments)]);
        let (mut mcp_doc, is_error) = tool_doc(&resp["2"]);
        assert!(!is_error, "{name}: success is not an error");
        mask(&mut cli_doc);
        mask(&mut mcp_doc);
        assert_eq!(cli_doc, mcp_doc, "{name}: the two surfaces are one");
    }
}

#[test]
fn typed_envelope_rides_is_error_with_actor_intact() {
    let s = Scratch::new();
    seed(&s);
    let (resp, _) = drive(
        &s,
        &[
            init_request(1),
            call(2, "query", json!({"sql": "DELETE FROM prs"})),
            call(3, "pr", json!({"reference": "not-a-ref"})),
        ],
    );
    let (doc, is_error) = tool_doc(&resp["2"]);
    assert!(is_error, "a refused write is a failed call");
    assert_eq!(
        doc["error"]["code"], "USER_INPUT",
        "the actor code is untranslated: {doc}"
    );
    let (doc, is_error) = tool_doc(&resp["3"]);
    assert!(is_error);
    assert_eq!(doc["error"]["code"], "USER_INPUT", "{doc}");
}

#[test]
fn flag_shaped_values_stay_data() {
    // Injection-proofing witnessed against the live CLI: a value that
    // parses as a flag must arrive at the verb as DATA. If "--limit=999"
    // became a flag, search would run with a limit instead of matching
    // nothing; if "-x" became a flag, query would refuse argv instead of
    // refusing SQL.
    let s = Scratch::new();
    seed(&s);
    let (resp, _) = drive(
        &s,
        &[
            init_request(1),
            // The quotes make it a valid FTS5 phrase, so the whole
            // flag-shaped token must travel to MATCH as data and match
            // nothing — had it become argv grammar, clap would refuse
            // the unknown flag before any search ran.
            call(2, "search", json!({"query": "\"--limit=999\""})),
            call(3, "query", json!({"sql": "-x"})),
            call(4, "prs", json!({"author": "--all"})),
        ],
    );
    let (doc, is_error) = tool_doc(&resp["2"]);
    assert!(
        !is_error,
        "flag-shaped phrase is a search, not a flag: {doc}"
    );
    assert_eq!(doc["total"], 0, "and it matches nothing: {doc}");
    let (doc, is_error) = tool_doc(&resp["3"]);
    assert!(is_error);
    let msg = doc["error"]["message"].as_str().unwrap();
    assert_eq!(
        doc["error"]["code"], "USER_INPUT",
        "\"-x\" reached SQLite as SQL: {doc}"
    );
    assert!(
        msg.contains("syntax") || msg.contains("SQL"),
        "refused as SQL, not as argv: {msg}"
    );
    // "--all" as an author is a validation refusal from the verb's own
    // login gate (USER_INPUT), not a silently-widened listing.
    let (doc, is_error) = tool_doc(&resp["4"]);
    assert!(is_error, "{doc}");
    assert_eq!(doc["error"]["code"], "USER_INPUT", "{doc}");
}

#[test]
fn protocol_edges_answer_by_the_book() {
    let s = Scratch::new();
    seed(&s);
    let (resp, status) = drive(
        &s,
        &[
            init_request(1),
            json!({"jsonrpc":"2.0","id":2,"method":"no/such/method"}),
            call(3, "no_such_tool", json!({})),
            call(4, "stats", json!({"bogus": 1})),
            call(5, "attention", json!({"limit": "five"})),
            call(6, "pr", json!({})),
            json!({"jsonrpc":"2.0","id":7,"method":"ping"}),
        ],
    );
    assert!(status.success());
    assert_eq!(resp["2"]["error"]["code"], -32601, "unknown method");
    assert_eq!(resp["3"]["error"]["code"], -32602, "unknown tool");
    assert_eq!(resp["4"]["error"]["code"], -32602, "unknown argument");
    assert_eq!(resp["5"]["error"]["code"], -32602, "mistyped argument");
    assert_eq!(resp["6"]["error"]["code"], -32602, "missing required arg");
    assert_eq!(resp["7"]["result"], json!({}), "ping pongs");
}

#[test]
fn garbage_lines_get_parse_errors_and_never_kill_the_session() {
    let s = Scratch::new();
    seed(&s);
    let (resp, status) = drive(
        &s,
        &[
            json!("this is a string, not a request"),
            init_request(1),
            json!({"jsonrpc":"2.0","id":2,"method":"ping"}),
        ],
    );
    assert!(status.success());
    // The string request has no usable id: JSON-RPC says id null. (A raw
    // non-JSON line would do the same; the driver can only ship valid
    // JSON, so the string stands in for malformed input.)
    assert_eq!(
        resp["null"]["error"]["code"],
        json!(-32600),
        "invalid request"
    );
    assert!(resp.contains_key("2"), "the session survived");
}
