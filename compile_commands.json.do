# shellcheck shell=sh
exec >&2
myoutdir="$(mktemp -d)"
trap 'rm -rf "$myoutdir"' EXIT

redo-ifchange \
  build_for_compiledb.sh \
  configure \
  all.od \
  all.rc.od

bear --output "$3" --force-wrapper -- \
  ./build_for_compiledb.sh "$myoutdir"
