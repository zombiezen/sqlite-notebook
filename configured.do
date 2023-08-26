# shellcheck shell=sh
# Ensure that an out directory exists during development.

exec >&2
if ! [ -d out ]; then
  configureWithFlags() {
    # Nix sets CC and CXX, which redoconf doesn't like.
    # Capture them as arguments to ../configure.
    if [ -n "$CC" ]; then
      set -- "CC=$CC" "$@"
      unset CC
    fi
    if [ -n "$CXX" ]; then
      set -- "CXX=$CXX" "$@"
      unset CXX
    fi

    ../configure "$@"
  }
  ( mkdir out && cd out && configureWithFlags )
fi

# By declaring a dependency on this file *after* running
# configure, we can tell redo that reconfiguration is
# needed if this file ever disappears (for example, if
# the whole out/ directory disappears).
redo-ifchange out/default.do
