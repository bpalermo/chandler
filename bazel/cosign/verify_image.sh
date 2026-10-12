#!/usr/bin/env bash
# Runs scripts/verify-image-signatures.sh with the pinned cosign.
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
COSIGN="$(rlocation "${COSIGN_RLOCATIONPATH:?set by the BUILD target}")"
script="$(rlocation "${VERIFY_SCRIPT_RLOCATIONPATH:?set by the BUILD target}")"
for x in "$COSIGN" "$script"; do
	if [ -z "$x" ] || [ ! -e "$x" ]; then
		echo "ERROR: missing runfile (${COSIGN_RLOCATIONPATH} / ${VERIFY_SCRIPT_RLOCATIONPATH})" >&2
		exit 2
	fi
done
export COSIGN
cd "${BUILD_WORKING_DIRECTORY:-.}"
exec bash "$script" "$@"
