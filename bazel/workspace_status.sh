#!/usr/bin/env bash
# Supplies the build with the git state that ends up in image tags and in the
# chart's version.
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

# Chart.yaml stamps its appVersion from this; a tagged build reports the tag,
# an untagged one a describe string, so a chart can always be traced back.
version="$(git describe --tags --always --dirty 2>/dev/null || echo unknown)"

# Chart.yaml stamps its version's pre-release part from this: the commit time
# in UTC, then the abbreviated commit. Semver compares the time numerically, so
# snapshots order by age and a version range picks the newest one. The "g"
# keeps the commit an alphanumeric identifier: an abbreviation made only of
# digits with a leading zero is not valid semver on its own.
committed="$(TZ=UTC git show -s --date=format-local:%Y%m%d%H%M%S --format=%cd HEAD 2>/dev/null || echo 0)"
short="$(git rev-parse --short=12 HEAD 2>/dev/null || echo unknown)"

echo "STABLE_GIT_VERSION ${version}"
echo "STABLE_CHART_SNAPSHOT ${committed}.g${short}"
echo "STABLE_GIT_COMMIT ${commit}"
