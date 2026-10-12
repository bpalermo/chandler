#!/usr/bin/env bash
# The pinned cosign is the version the bazel_dep says it is. A cosign 2 here
# would read cosign 3 signatures as "no signatures found", so the major matters.
# --- begin runfiles.bash initialization v3 ---
# shellcheck disable=SC1090
set -uo pipefail
set +e
f=bazel_tools/tools/bash/runfiles/runfiles.bash
source "${RUNFILES_DIR:-/dev/null}/$f" 2>/dev/null ||
	source "$(grep -sm1 "^$f " "${RUNFILES_MANIFEST_FILE:-/dev/null}" | cut -f2- -d' ')" 2>/dev/null ||
	source "$0.runfiles/$f" 2>/dev/null ||
	source "$(grep -sm1 "^$f " "$0.runfiles_manifest" | cut -f2- -d' ')" 2>/dev/null ||
	source "$(grep -sm1 "^$f " "$0.exe.runfiles_manifest" | cut -f2- -d' ')" 2>/dev/null ||
	{
		echo >&2 "ERROR: cannot find $f"
		exit 1
	}
f=
set -e
# --- end runfiles.bash initialization v3 ---
EXPECTED="v3.1.2"
cosign="$(rlocation "${COSIGN_RLOCATIONPATH:?set by the BUILD target}")"
got="$("$cosign" version 2>&1 | awk '/^GitVersion:/ {print $2}')"
if [ "$got" != "$EXPECTED" ]; then
	echo "cosign ${got}, want ${EXPECTED} (the rules_img_signer_cosign bazel_dep and this test move together)" >&2
	exit 1
fi
echo "cosign ${got}"
