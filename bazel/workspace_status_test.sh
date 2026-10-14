#!/usr/bin/env bash
# What workspace_status.sh says a commit is: a snapshot, a release candidate or
# a release, from the VERSION file and the tags on the commit.
set -euo pipefail

script="$(cd "$(dirname "$0")" && pwd)/workspace_status.sh"
[ -r "${script}" ] || script="${TEST_SRCDIR}/_main/bazel/workspace_status.sh"

repo="$(mktemp -d)"
trap 'rm -rf "${repo}"' EXIT
cd "${repo}"
export GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1
git init --quiet .
git config user.email test@example.com
git config user.name test
echo 0.1.0 >VERSION
git add VERSION
git commit --quiet -m first

fail=0
expect() {
	local key="$1" want="$2" got
	got="$(bash "${script}" | sed -n "s/^${key} //p")"
	if [[ ! "${got}" =~ ^${want}$ ]]; then
		echo "FAIL: ${3}: ${key} is '${got}', want /${want}/" >&2
		fail=1
	fi
}

expect STABLE_CHART_VERSION '0\.1\.0-dev\.[0-9]{14}\.g[0-9a-f]{12}' "an untagged commit"
expect STABLE_RELEASE_TAG none "an untagged commit"

git tag v0.2.0
expect STABLE_RELEASE_TAG none "a tag that is not VERSION's"
expect STABLE_CHART_VERSION '0\.1\.0-dev\..*' "a tag that is not VERSION's"

git tag v0.1.0-rc.1
git tag v0.1.0-rc.10
expect STABLE_RELEASE_TAG 'v0\.1\.0-rc\.10' "candidates"
expect STABLE_CHART_VERSION '0\.1\.0-rc\.10' "candidates"
expect STABLE_GIT_VERSION 'v0\.1\.0-rc\.10' "candidates"

echo change >>VERSION.extra
git add VERSION.extra
expect STABLE_RELEASE_TAG none "a candidate's commit with uncommitted changes"
git reset --quiet --hard

git tag v0.1.0
expect STABLE_RELEASE_TAG 'v0\.1\.0' "a release and its candidates on one commit"
expect STABLE_CHART_VERSION '0\.1\.0' "a release"
expect STABLE_GIT_VERSION 'v0\.1\.0' "a release"

git commit --quiet --allow-empty -m next
expect STABLE_RELEASE_TAG none "the commit after a release"
expect STABLE_CHART_VERSION '0\.1\.0-dev\..*' "the commit after a release"

[ "${fail}" = 0 ]
echo "workspace status: snapshot, candidates and release as expected"
