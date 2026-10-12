#!/usr/bin/env bash
# The packaged chart pins the CSI image the way the publish workflow relies on.
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
chart="$(rlocation "${CHART_RLOCATIONPATH:?set by the BUILD target}")"
[ -r "$chart" ] || { echo "no chart at ${CHART_RLOCATIONPATH}" >&2; exit 1; }

fail=0
bad() { echo "FAIL: $*" >&2; fail=1; }

values="$(tar -xzOf "$chart" chandler/values.yaml)"
chartyaml="$(tar -xzOf "$chart" chandler/Chart.yaml)"

# The csi.image block: from `  image:` to the next key at the same depth.
image="$(printf '%s\n' "$values" | awk '/^csi:/ {c=1; next} c && /^  image:/ {i=1; next} i && /^  [^ ]/ {exit} i')"
repo="$(printf '%s\n' "$image" | sed -nE 's/^    repository:[[:space:]]*"?([^"]*)"?[[:space:]]*$/\1/p')"
digest="$(printf '%s\n' "$image" | sed -nE 's/^    digest:[[:space:]]*"?([^"]*)"?[[:space:]]*$/\1/p')"

[ "$repo" = "${EXPECT_CSI_REPOSITORY:?}" ] || bad "csi.image.repository is '${repo}', want '${EXPECT_CSI_REPOSITORY}'"
[[ "$digest" =~ ^sha256:[0-9a-f]{64}$ ]] || bad "csi.image.digest is '${digest}', want a sha256 digest"

version="$(printf '%s\n' "$chartyaml" | sed -nE 's/^version:[[:space:]]*"?([^"]*)"?[[:space:]]*$/\1/p')"
name="$(printf '%s\n' "$chartyaml" | sed -nE 's/^name:[[:space:]]*(.*)$/\1/p')"
# Stamped: 0.0.0-dev.<commit time>.g<commit>. Unstamped: rules_helm's
# placeholder in the same position.
[[ "$version" =~ ^0\.0\.0-dev\.([0-9]+\.g[0-9a-z]+|STABLE-CHART-SNAPSHOT)$ ]] || bad "chart version is '${version}', want 0.0.0-dev.<commit time>.g<commit>"
[ "$name" = "${EXPECT_CHART_REPOSITORY##*/}" ] || bad "chart name is '${name}', but helm push publishes it to ${EXPECT_CHART_REPOSITORY}"

[ "$fail" = 0 ] || exit 1
echo "chart ${name} ${version} pins ${repo}@${digest}"
