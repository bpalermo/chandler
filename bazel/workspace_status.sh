#!/usr/bin/env bash
# Supplies the build with the git state that ends up in image tags, in the
# binaries' version and in the chart's version.
#
# Every key here is STABLE_. Unprefixed keys go to volatile-status.txt, which
# Bazel treats as constant metadata: an action that embeds one is not
# invalidated when it changes, so a cached action can keep publishing a stale
# value. Everything below identifies a commit and must invalidate when the
# commit changes, so none of it belongs there. Nothing is lost to caching --
# these values change only with the commit, which changes the build anyway.
#
# The script never fails, so it cannot break a build: outside a git checkout
# the values are "unknown" (the snapshot "0.gunknown").
set -euo pipefail

commit="$(git rev-parse HEAD 2>/dev/null || echo unknown)"

# VERSION is the release being worked towards. A commit tagged v<VERSION>, or
# v<VERSION>-rc.<n> for a release candidate, is that release; any other commit,
# including one with a tag that does not match VERSION, is a snapshot of it.
next="$(tr -d '[:space:]' <VERSION 2>/dev/null || true)"
next="${next:-0.0.0}"
release=""
if [[ -z "$(git status --porcelain --untracked-files=no 2>/dev/null || echo dirty)" ]]; then
	tags="$(git tag --points-at HEAD --list "v${next}" "v${next}-rc.*" 2>/dev/null || true)"
	# The release itself before any candidate on the same commit, and the
	# newest candidate before older ones.
	if grep -qx "v${next}" <<<"${tags}"; then
		release="v${next}"
	else
		release="$(grep -E "^v${next//./\\.}-rc\.[0-9]+$" <<<"${tags}" | sort -t. -k4,4n | tail -1 || true)"
	fi
fi

# What the binaries report and the chart's appVersion: the release tag, or a
# describe string, so a build can always be traced back.
version="${release:-$(git describe --tags --always --dirty 2>/dev/null || echo unknown)}"

# A snapshot's pre-release part: the commit time in UTC, then the abbreviated
# commit. Semver compares the time numerically, so snapshots order by age and
# a version range picks the newest one. The "g" keeps the commit an
# alphanumeric identifier: an abbreviation made only of digits with a leading
# zero is not valid semver on its own.
committed="$(TZ=UTC git show -s --date=format-local:%Y%m%d%H%M%S --format=%cd HEAD 2>/dev/null || echo 0)"
short="$(git rev-parse --short=12 HEAD 2>/dev/null || echo unknown)"
snapshot="${committed}.g${short}"

if [[ -n "${release}" ]]; then
	chart_version="${release#v}"
else
	chart_version="${next}-dev.${snapshot}"
fi

echo "STABLE_GIT_VERSION ${version}"
echo "STABLE_CHART_SNAPSHOT ${snapshot}"
echo "STABLE_CHART_VERSION ${chart_version}"
echo "STABLE_RELEASE_TAG ${release:-none}"
echo "STABLE_GIT_COMMIT ${commit}"
