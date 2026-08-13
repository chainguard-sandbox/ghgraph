# ghgraph

Local GitHub work memory. Syncs GitHub conversation into SQLite and answers
from the archive instead of the API.

Two scopes, chosen per repo. Your working set: the PRs you author and
review, with their threads, comments, and linked issues. Or, for a repo you
maintain, the whole PR and issue stream — triage questions ("what needs a
reviewer", "what arrived overnight") become archive queries too. Either
way, you can opt in named people — collaborators or contributors — whose
work you want tracked alongside your own.

Status: working. Both scopes sync and read end-to-end; the output contract
is frozen at `schema_version: 1` (additive-only), and an MCP server
(`ghgraph-mcp`) exposes the same verbs as tools. [DESIGN.md](DESIGN.md)
has the architecture.

Requires the [gh](https://cli.github.com) CLI — ghgraph carries no HTTP, TLS,
or auth code by design (gh itself runs on Linux, macOS, and Windows). ghgraph
is Unix-only for its own reasons: cancellation is process-group signals and
the archive is protected by file-mode bits, neither of which has a Windows
equivalent.

    ghgraph sync                 fetch configured repos into the archive
    ghgraph attention            what is waiting on you, or on your project
    ghgraph pr owner/name#123    one PR, full context
    ghgraph search "fts query"   full-text over the archive
    ghgraph query "select ..."   read-only SQL against the archive

stdout is always one JSON document. Progress goes to stderr. Errors are typed
envelopes with exit code 2.

Getting started: `make doctor` checks your prerequisites (the gh CLI, signed
in, and the Rust toolchain), `make install` puts `ghgraph` and `ghgraph-mcp`
side by side in `~/.cargo/bin`, `make check` runs everything CI does, and
`make help` lists the rest.

Config: `~/.config/ghgraph/config.json` — see
[config.example.json](config.example.json).

MCP: `ghgraph-mcp` serves the same seven verbs as tools over stdio, one
`ghgraph` invocation per call — the CLI and the server are one surface,
so every result is the same JSON document the CLI prints. Point your
client at the binary:

    { "mcpServers": { "ghgraph": { "command": "ghgraph-mcp" } } }

(`--ghgraph <path>` names the CLI if it is not adjacent or on PATH;
`--config` — or the `GHGRAPH_CONFIG` environment variable — passes a
config file through to every call.) To smoke-test without a client:

    printf '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}\n' | ghgraph-mcp

prints one `serverInfo` frame and exits on EOF.

Security reports: [SECURITY.md](SECURITY.md).

License: Apache-2.0.
