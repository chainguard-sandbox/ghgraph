// Help-text lints shared by both binaries' test modules (src/main.rs and
// src/bin/mcp.rs include this file via #[path]). Without clap's
// `wrap_help` feature — rejected for its terminal_size/rustix cost
// (src/main.rs, struct Cli) — clap prints help text exactly as written,
// so the doc comments ARE the layout and these lints are what hold it.

use clap::Command;

/// Wider than this wraps or scrolls on a stock terminal.
const MAX_COLUMNS: usize = 80;

/// Every command in the tree, built (so global args and bin names have
/// propagated), labeled by its invocation path.
fn walk(root: Command) -> Vec<(String, Command)> {
    fn visit(path: String, cmd: &Command, out: &mut Vec<(String, Command)>) {
        out.push((path.clone(), cmd.clone()));
        for sub in cmd.get_subcommands() {
            visit(format!("{path} {}", sub.get_name()), sub, out);
        }
    }
    let mut root = root;
    root.build();
    let mut out = Vec::new();
    visit(root.get_name().to_owned(), &root, &mut out);
    out
}

/// One violation per over-wide line, across `-h` and `--help` for every
/// command; each names the invocation, line number, width, and text.
pub fn overwide_lines(root: Command) -> Vec<String> {
    let mut bad = Vec::new();
    for (path, mut cmd) in walk(root) {
        for (flag, text) in [
            ("-h", cmd.render_help()),
            ("--help", cmd.render_long_help()),
        ] {
            for (i, line) in text.to_string().lines().enumerate() {
                let cols = line.chars().count();
                if cols > MAX_COLUMNS {
                    bad.push(format!(
                        "`{path} {flag}` line {}: {cols} cols: {line}",
                        i + 1
                    ));
                }
            }
        }
    }
    bad
}

/// One violation per command without an about line or argument without a
/// help line — the summaries `-h` exists to show.
pub fn undocumented(root: Command) -> Vec<String> {
    let mut bad = Vec::new();
    for (path, cmd) in walk(root) {
        if cmd.get_about().is_none() {
            bad.push(format!("`{path}`: command has no about"));
        }
        for arg in cmd.get_arguments() {
            if arg
                .get_help()
                .is_none_or(|h| h.to_string().trim().is_empty())
            {
                bad.push(format!("`{path}`: argument `{}` has no help", arg.get_id()));
            }
        }
    }
    bad
}
