#!/usr/bin/env bash
# Print the registry setting, parsed from bazel/registry.bzl.
#
# bazel/registry.bzl is the ONE place the registry host, namespace and naming
# rules are written down. Bazel loads it; everything that is not Bazel -- the
# workflows and the verifier -- asks this script, so moving registries is an
# edit to that file and nothing else.
#
# The parse is deliberately strict: each assignment must appear exactly once,
# on one line, as `NAME = "value"`. A setting this script cannot read is exit 2
# with nothing on stdout, never a default: a default here would quietly publish
# or verify against a registry nobody chose.
#
# Usage:
#   registry.sh                     KEY=VALUE lines (for "$GITHUB_ENV"):
#       IMAGE_REGISTRY_HOST      the registry host
#       IMAGE_NAMESPACE          the organization under it
#       SERVER_IMAGE             image_reference("server")
#       CSI_IMAGE                image_reference("csi")
#       PROVIDER_S3_IMAGE        image_reference("provider-s3")
#       CHART_REPOSITORY         chart_registry_url("chandler")
#       IMAGE_SIGNATURE_LAYOUT   referrer | tag
#   registry.sh host                the registry host alone
#   registry.sh repo <component>    image_repository(component)
#   registry.sh ref <component>     image_reference(component)
#   registry.sh chart-ref <chart>   chart_registry_url(chart)
#   registry.sh signature-layout    SIGNATURE_LAYOUT
#
# Every value is validated against [a-z0-9./_-] (plus :port on the host), so
# the KEY=VALUE form is safe to append to $GITHUB_ENV unquoted.
set -euo pipefail

bzl="${REGISTRY_BZL:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/bazel/registry.bzl}"

die() {
	echo "registry.sh: ${bzl}: $*" >&2
	exit 2
}

[ -r "$bzl" ] || die "not readable"

# one <NAME> <value regex> -> the value of the single `NAME = "value"` line.
one() {
	local name="$1" re="$2" lines
	lines="$(grep -E "^${name}[[:space:]]*=" "$bzl" || true)"
	[ -n "$lines" ] || die "no ${name} assignment"
	[ "$(printf '%s\n' "$lines" | wc -l)" -eq 1 ] || die "${name} is assigned more than once"
	[[ "$lines" =~ ^${name}\ =\ \"(${re})\"$ ]] || die "cannot parse: ${lines}"
	printf '%s\n' "${BASH_REMATCH[1]}"
}

host="$(one IMAGE_REGISTRY '[a-z0-9]([a-z0-9.-]*[a-z0-9])?(:[0-9]+)?')"
namespace="$(one IMAGE_NAMESPACE '[a-z0-9]([a-z0-9._/-]*[a-z0-9])?')"

name_ok() {
	[[ "$1" =~ ^[a-z0-9]([a-z0-9._-]*[a-z0-9])?$ ]] || {
		echo "registry.sh: not a component/chart name: '$1'" >&2
		exit 2
	}
}

case "${1:-env}" in
env)
	layout="$(one SIGNATURE_LAYOUT 'referrer|tag')"
	printf 'IMAGE_REGISTRY_HOST=%s\n' "$host"
	printf 'IMAGE_NAMESPACE=%s\n' "$namespace"
	printf 'SERVER_IMAGE=%s/%s/server\n' "$host" "$namespace"
	printf 'CSI_IMAGE=%s/%s/csi\n' "$host" "$namespace"
	printf 'PROVIDER_S3_IMAGE=%s/%s/provider-s3\n' "$host" "$namespace"
	printf 'CHART_REPOSITORY=%s/%s/chandler\n' "$host" "$namespace"
	printf 'IMAGE_SIGNATURE_LAYOUT=%s\n' "$layout"
	;;
host) printf '%s\n' "$host" ;;
repo)
	[ "$#" -eq 2 ] || die "usage: repo <component>"
	name_ok "$2"
	printf '%s/%s\n' "$namespace" "$2"
	;;
ref)
	[ "$#" -eq 2 ] || die "usage: ref <component>"
	name_ok "$2"
	printf '%s/%s/%s\n' "$host" "$namespace" "$2"
	;;
chart-ref)
	[ "$#" -eq 2 ] || die "usage: chart-ref <chart>"
	name_ok "$2"
	printf '%s/%s/%s\n' "$host" "$namespace" "$2"
	;;
signature-layout) one SIGNATURE_LAYOUT 'referrer|tag' ;;
*)
	echo "usage: $0 [env | host | repo <component> | ref <component> | chart-ref <chart> | signature-layout]" >&2
	exit 2
	;;
esac
