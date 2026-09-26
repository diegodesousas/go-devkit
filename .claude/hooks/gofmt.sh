#!/bin/sh
# PostToolUse hook for Write|Edit. Runs inside the `dev` container: reads the
# hook payload from stdin and gofmts the edited file when it is a .go file in
# this repo. CLAUDE_PROJECT_DIR is the host path, forwarded by the hook
# command, and maps to /workspace here.
f=$(jq -r '.tool_response.filePath // .tool_input.file_path')

case "$f" in
    "$CLAUDE_PROJECT_DIR"/*.go) gofmt -w "${f#"$CLAUDE_PROJECT_DIR"/}" ;;
esac
