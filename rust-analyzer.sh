#!/bin/sh
# Use the following VSCode setting:
# "rust-analyzer.server.path": "${workspaceFolder}/rust-analyzer.sh"
exec direnv exec "$(dirname "$0")" rust-analyzer "$@"
