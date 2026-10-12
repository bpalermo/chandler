#!/usr/bin/env bash
# Runs the pinned cosign (@rules_img_signer_cosign//cosign) from the invoking directory.
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
cosign="$(rlocation "${COSIGN_RLOCATIONPATH:?set by the BUILD target}")"
if [ -z "$cosign" ] || [ ! -x "$cosign" ]; then
	echo "ERROR: cosign binary not found in runfiles (${COSIGN_RLOCATIONPATH})" >&2
	exit 2
fi
cd "${BUILD_WORKING_DIRECTORY:-.}"
exec "$cosign" "$@"
