#!/usr/bin/env bash
# The registry is ONE setting: bazel/registry.bzl.
#
# Moving registries is only a one-file edit while nothing else spells the
# registry out, and a review cannot be trusted to keep that true: a literal
# pasted into a workflow keeps working until the day a move leaves it pointing
# at the old registry. So this asserts, on every change:
#
#   1. scripts/registry.sh parses the setting.
#   2. charts/chandler/values.yaml takes the CSI repository and digest from the
#      //csi:image_push target (which gets them from registry.bzl) instead of
#      spelling them out. The values are data, so they cannot be Starlark; the
#      package-time substitution is what derives them, and
#      //charts/chandler:pin_test asserts the result equals
#      image_reference("csi").
#   3. No tracked file outside the allow-list contains `<host>/<namespace>`,
#      computed from the setting, not typed here.
#
# The literal hunt runs a positive control first, so it cannot pass by matching
# nothing.
#
# ALLOW-LIST:
#   bazel/registry.bzl             the setting
#   scripts/registry.sh            its usage text
#   scripts/check-registry-config.sh
# Documentation that tells a user what to pull (README, docs/) is not exempt
# by default: add it here, with the reason, when it first needs the literal.
#
# No network, no Bazel. Exit 0 clean, 1 on a violation, 2 when the check itself
# cannot run.
set -uo pipefail
cd "$(dirname "$0")/.." || exit 2

fail=0
bad() {
	echo "::error::$*"
	fail=1
}

# --- 1. the setting parses ---------------------------------------------------
if ! host="$(scripts/registry.sh host)" || ! csi="$(scripts/registry.sh ref csi)" ||
	! ns_repo="$(scripts/registry.sh repo csi)" || [ -z "$host" ] || [ -z "$csi" ]; then
	echo "::error::scripts/registry.sh cannot parse bazel/registry.bzl" >&2
	exit 2
fi
namespace="${ns_repo%/csi}"
echo "setting: ${host}/${namespace}"

esc() { printf '%s' "$1" | sed -E 's/[][\.^$*+?(){}|/]/\\&/g'; }

# --- 2. the chart's CSI default is derived -----------------------------------
values=charts/chandler/values.yaml
n_repo="$(grep -cE '^[[:space:]]*repository:[[:space:]]*"\{@//csi:image_push\.repository\}"[[:space:]]*$' "$values" || true)"
n_digest="$(grep -cE '^[[:space:]]*digest:[[:space:]]*"\{@//csi:image_push\.digest\}"[[:space:]]*$' "$values" || true)"
if [ "$n_repo" != 1 ] || [ "$n_digest" != 1 ]; then
	bad "${values} must default csi.image.repository to \"{@//csi:image_push.repository}\" and csi.image.digest to \"{@//csi:image_push.digest}\" (found ${n_repo} and ${n_digest}) -- the chart's CSI default would not come from bazel/registry.bzl and the pushed digest"
else
	echo "ok: ${values} derives the CSI image from //csi:image_push (${csi})"
fi

# --- 3. no literal outside the allow-list ------------------------------------
allow=(
	':(exclude)bazel/registry.bzl'
	':(exclude)scripts/registry.sh'
	':(exclude)scripts/check-registry-config.sh'
)
pattern="$(esc "${host}/${namespace}")(?![[:alnum:]_.-])"
for probe in "registry: ${host}/${namespace}" "image=${csi}@sha256:0"; do
	if ! printf '%s\n' "$probe" | grep -qP -- "$pattern"; then
		echo "::error::the literal hunt does not match '${probe}' -- the pattern is broken" >&2
		exit 2
	fi
done
hits="$(git grep -nP -- "$pattern" -- . "${allow[@]}")"
rc=$?
case "$rc" in
0)
	bad "the registry is spelled out outside bazel/registry.bzl. Derive it (Bazel: load //bazel:registry.bzl; shell and workflows: scripts/registry.sh), or add the path to the allow-list in scripts/check-registry-config.sh with a reason:"
	printf '%s\n' "$hits" | sed 's/^/  /'
	;;
1) echo "ok: no registry literal outside the allow-list" ;;
*)
	echo "::error::git grep failed (exit ${rc})" >&2
	exit 2
	;;
esac

[ "$fail" -eq 0 ] || exit 1
echo "registry config: one setting, bazel/registry.bzl"
