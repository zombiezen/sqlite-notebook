#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $(basename "$0") PROGRAM" >&2
  exit 64
fi

json_path="$(echo -n "$1" | jq --raw-input --slurp .)"
echo '{'
echo "  \"argv\": [$json_path, \"--\", \"{connection_file}\"],"
echo '  "interrupt_mode": "message",'
echo '  "display_name": "SQLite",'
echo '  "language": "sql"'
echo '}'
