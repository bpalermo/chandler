#!/usr/bin/env bash
# Fail if the protos in this checkout break the wire contract of an earlier
# commit: a field renumbered, retyped or reused, something deleted without its
# number reserved, an RPC or a service renamed. The rules are `breaking` in
# api/buf.yaml.
#
# Both sides are compiled by Bazel, each from its own tree, and compared by
# the buf that MODULE.bazel pins; buf compiles nothing. The earlier commit is
# checked out into a git worktree outside this one, always at the same path so
# that its Bazel output base is reused.
#
# Usage:
#   proto-breaking.sh [<ref>] [<bazel flag>...]
#
# <ref> is what to compare against, by default origin/main. The comparison is
# with the commit this checkout branched from it, so a branch behind <ref> is
# not blamed for what <ref> has gained since.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

ref="origin/main"
if [[ $# -gt 0 && "$1" != -* ]]; then
	ref="$1"
	shift
fi
base="$(git merge-base HEAD "${ref}")"

# In the target configuration only: after a build, cquery also answers with
# the configurations other rules built the same protos in, and each file would
# be in the image once per configuration.
protos="config(kind(proto_library, deps(//api/chandler/v1:chandlerv1_proto)), target)"

# image <tree> <output> [<bazel flag>...] writes the descriptors of the protos
# and of everything they import, as one FileDescriptorSet: buf resolves the
# imports from its input.
image() {
	local tree="$1" output="$2"
	shift 2
	(
		cd "${tree}"
		local targets
		mapfile -t targets < <(bazel cquery "$@" "${protos}" --output=label 2>/dev/null | cut -d' ' -f1)
		bazel build "$@" -- "${targets[@]}"
		# A serialized message with one repeated field: concatenating merges.
		bazel cquery "$@" "${protos}" --output=files 2>/dev/null | xargs cat >"${output}"
	)
	[[ -s "${output}" ]] || {
		echo "proto-breaking: no descriptors were built in ${tree}" >&2
		exit 2
	}
}

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

baseline="${XDG_CACHE_HOME:-${HOME}/.cache}/chandler/proto-baseline"
if [[ -e "${baseline}/.git" ]]; then
	git -C "${baseline}" checkout --quiet --detach "${base}"
else
	mkdir -p "$(dirname "${baseline}")"
	git worktree prune
	git worktree add --quiet --detach "${baseline}" "${base}"
fi

image "${baseline}" "${work}/base.binpb" "$@"
image "${PWD}" "${work}/head.binpb" "$@"

# --path keeps the check to Chandler's own files: the images also hold what
# they import, and a dependency's change is not ours to refuse.
bazel run "$@" @rules_buf_toolchains//:buf -- breaking "${work}/head.binpb" \
	--against "${work}/base.binpb" --config "${PWD}/api/buf.yaml" --path chandler
echo "proto-breaking: no breaking change since ${base:0:12}"
