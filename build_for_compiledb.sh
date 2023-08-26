#!/bin/sh
set -e
myconfigure="$(cd "$(dirname "$0")" && pwd)/configure"
mkdir -p "$1"
cd "$1"
configureWithFlags() {
  # Bear sets CC and CXX, which redoconf doesn't like.
  # Capture them as arguments to ../configure.
  if [ -n "$CC" ]; then
    set -- "CC=$CC" "$@"
    unset CC
  fi
  if [ -n "$CXX" ]; then
    set -- "CXX=$CXX" "$@"
    unset CXX
  fi

  "$myconfigure" "$@"
}
configureWithFlags

redo -k sqlite-notebook || true
