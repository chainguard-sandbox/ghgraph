# ghgraph

Local GitHub work memory. Syncs GitHub conversation into SQLite and answers
from the archive instead of the API.

Two scopes, chosen per repo. Your working set: the PRs you author and
review, with their threads, comments, and linked issues. Or, for a repo you
maintain, the whole PR and issue stream — triage questions ("what needs a
reviewer", "what arrived overnight") become archive queries too. Either
way, you can opt in named people — collaborators or contributors — whose
work you want tracked alongside your own.

Status: design scaffold. The command surface, schema, and invariants are the
current deliverable; bodies are stubs. [DESIGN.md](DESIGN.md) has the
architecture; [ROADMAP.md](ROADMAP.md) sequences the build.

Unix only. Requires the [gh](https://cli.github.com) CLI: ghgraph carries no
HTTP, TLS, or auth code by design.

    ghgraph sync                 fetch configured repos into the archive
    ghgraph attention            what is waiting on you, or on your project
    ghgraph pr owner/name#123    one PR, full context
    ghgraph search "fts query"   full-text over the archive
    ghgraph query "select ..."   read-only SQL against the archive

stdout is always one JSON document. Progress goes to stderr. Errors are typed
envelopes with exit code 2.

Config: `~/.config/ghgraph/config.json` — see
[config.example.json](config.example.json).

License: Apache-2.0.
