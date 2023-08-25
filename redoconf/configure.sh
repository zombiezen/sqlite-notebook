#!/bin/sh -e
if [ -z "$S" ]; then
	exec >&2
	echo "configure.sh: must include this from a 'configure' script"
	exit 99
fi
if [ -e "configure" ] || [ -e "rc.sh" ]; then
	exec >&2
	echo "$0: run this script from an empty output directory."
	echo "  For example:"
	echo "    (mkdir out && cd out && ../configure && redo -j10)"
	exit 99
fi

S="$(dirname "$0")"
rm -f src Makefile
echo "$S" >src

# Don't regenerate these files unless they're missing.  Otherwise redo
# will treat it as a changed dependency and rebuild a bunch of files
# unnecessarily.
[ -e redoconf.rc ] || cat >redoconf.rc <<-EOF
	# Automatically generated by $0
	read -r S <src
	. "\$S/redoconf/rc.sh"
EOF

# Instead of a symlink, we could make this a script like redoconf.rc,
# except then we would have to redi-ifchange default.do.sh, which would
# greatly increase the number of calls to redo-ifchange in the fast path.
# That turns out to slow things down quite a bit.  So let redo follow
# the symlink and note the dependency internally, which is faster.
[ -e default.do ] || ln -sf "$S/redoconf/default.do.sh" default.do

cat >Makefile <<'EOF'
# A wrapper for people who like to type 'make' instead of 'redo'
all $(filter-out all,$(MAKECMDGOALS)):
	+redo "$@"
.PHONY: $(MAKECMDGOALS) all
EOF

# Don't include rc.sh here, because that might call redo-ifchange,
# and we're not ready for that yet.
. "$S/redoconf/utils.sh"

usage() {
	exec >&2
	printf 'Usage: %s %s' "$0" \
'[options...] [KEY=value] [--with-key=value]

    --prefix=       Change installation prefix (usually /usr/local)
    --host=         Architecture prefix for output (eg. i686-w64-mingw32-)
    --enable-static               Link binaries statically
    --disable-shared              Do not build shared libraries
    --{dis,en}able-optimization   Disable/enable optimization for C/C++
    --{dis,en}able-debug          Disable/enable debugging flags for C/C++
    -h, --help      This help message
'
	if [ -e "$S/configure.help" ]; then
		printf '\nProject-specific flags:\n'
		sort "$S/configure.help" |
		while read k msg; do
			# skip blanks and comments
			[ -n "${k%%\#*}" ] || continue
			# skip options with syntax already explained above
			[ "$k" != "ARCH" ] || continue
			[ "$k" != "PREFIX" ] || continue
			[ "$k" != "STATIC" ] || continue
			printf '    %-15s %s\n' "$k=..." "$msg"
		done
	else
		printf '\nNo extra help yet: configure.help is missing.\n'
	fi
	exit 1
}

upper() {
	xecho "$1" | tr 'a-z' 'A-Z' | sed 's/[^A-Z0-9]/_/g'
}

lower() {
	xecho "$1" | tr 'A-Z' 'a-z' | sed 's/[^a-z0-9]/-/g'
}

emit() {
	xecho "replaceln" "$1" "$(shquote "$(rc_splitwords "$2")")"
}

emit_append() {
	xecho "appendln" "$1" "$(shquote "$(rc_splitwords "$2")")"
}

for k in ARCH CC CXX CPPFLAGS CFLAGS CXXFLAGS \
         OPTFLAGS LINK LDFLAGS LIBS xCFLAGS; do
	eval v=\$$k
	if [ -n "$v" ]; then
		echo "$0: Fatal: '$k' environment variable is set." >&2
		echo "  This can cause inconsistent builds." >&2
		echo "  Pass variables as arguments to $0 instead." >&2
		echo "  Example: $0 $k=$(shquote "$v")" >&2
		exit 4
	fi
done

rm -f _flags.tmp
echo "# Auto-generated by $0" >_flags.tmp
for d in "$@"; do
	case $d in
		--help|-h|-\?)
			usage
			;;
		--host=*)
			v="${d#*=}"
			emit "ARCH" "$v"
			;;
		--enable-static)
			emit "STATIC" "1"
			;;
		--disable-shared)
			emit "NOSHARED" "1"
			;;
		--enable-optimization)
			emit_append OPTFLAGS "-O2"
			;;
		--disable-optimization)
			emit_append OPTFLAGS "-O0"
			;;
		--enable-debug)
			emit_append OPTFLAGS "-g"
			;;
		--disable-debug)
			emit_append OPTFLAGS "-g0"
			;;
		--prefix=*)
			v="${d#*=}"
			emit "PREFIX" "$v"
			;;
		--with-[A-Za-z]*)
			if [ "$d" = "${d%%=*}" ]; then
				xecho "$0: in $(shquote "$d"):" \
				      "must supply '--with-<key>=<value>'" >&2
				exit 3
			fi
			k="${d%%=*}"
			k="${k#--with-}"
			v="${d#*=}"
			kl=$(lower "$k")
			if [ "$k" != "$kl" ]; then
				xecho "$0: in $(shquote "--with-$k=..."):" \
				      "must be all lowercase and dashes" >&2
				exit 3
			fi
			ku=$(upper "$k")
			emit "$ku" "$v"
			;;
		--without-[A-Za-z]*)
			if [ "$d" != "${d%%=*}" ]; then
				xecho "$0: in $(shquote "$d"):" \
				      "do not supply '=...' with --without" >&2
				exit 3
			fi
			k="${d#--without-}"
			kl=$(lower "$k")
			if [ "$k" != "$kl" ]; then
				xecho "$0: in $(shquote "$d"):" \
				      "must be all lowercase and dashes" >&2
				exit 3
			fi
			ku=$(upper "$k")
			# This causes tests to try to link using an extra
			# option "NONE", which will fail, thus making the
			# package appear missing.
			emit "$ku" "NONE"
			;;
		[A-Za-z_]*=*)
			k="${d%%=*}"
			v="${d#*=}"
			ku=$(upper "$k")
			if [ "$k" != "$ku" ]; then
				xecho "$0: in $(shquote "$d"):" \
				      "invalid KEY=value;" \
				      "must be all caps and underscores" >&2
				exit 3
			fi
			emit "$ku" "$v"
			;;
		*)
			xecho "$0: in $(shquote "$d"):" \
			      "invalid option; use --help for help." >&2
			exit 2
			;;
	esac
done >>_flags.tmp

# Avoid replacing the file if it's identical, so we don't trigger
# unnecessary rebuilds.  redo-stamp can do this for a redo target,
# but _flags is being generated by this configure script, which is
# not a redo target.
if [ -e _flags ] && cmp -s _flags.tmp _flags; then
	rm -f _flags.tmp
else
	rm -f _flags
	mv _flags.tmp _flags
fi
