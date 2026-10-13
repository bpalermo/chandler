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
chart_pin="$(rlocation "${CHART_PIN_RLOCATIONPATH:?set by the BUILD target}")"
[ -r "$chart" ] || {
	echo "no chart at ${CHART_RLOCATIONPATH}" >&2
	exit 1
}

fail=0
bad() {
	echo "FAIL: $*" >&2
	fail=1
}

chartyaml="$(tar -xzOf "$chart" chandler/Chart.yaml)"
name="$(printf '%s\n' "$chartyaml" | sed -nE 's/^name:[[:space:]]*(.*)$/\1/p')"

pins=""
for repository in ${EXPECT_REPOSITORIES:?}; do
	if digest="$("$chart_pin" "$chart" "$name" "$repository")"; then
		pins="${pins} ${repository}@${digest}"
	else
		bad "the chart does not pin ${repository} by digest"
	fi
done

version="$(printf '%s\n' "$chartyaml" | sed -nE 's/^version:[[:space:]]*"?([^"]*)"?[[:space:]]*$/\1/p')"
# Stamped: 0.1.0-dev.<commit time>.g<commit>. Unstamped: rules_helm's
# placeholder in the same position.
[[ "$version" =~ ^0\.1\.0-dev\.([0-9]+\.g[0-9a-z]+|STABLE-CHART-SNAPSHOT)$ ]] || bad "chart version is '${version}', want 0.1.0-dev.<commit time>.g<commit>"
[ "$name" = "${EXPECT_CHART_REPOSITORY##*/}" ] || bad "chart name is '${name}', but helm push publishes it to ${EXPECT_CHART_REPOSITORY}"

[ "$fail" = 0 ] || exit 1
echo "chart ${name} ${version} pins${pins}"
