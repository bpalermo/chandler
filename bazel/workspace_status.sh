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
# both values are "unknown".
set -euo pipefail

commit="$(git rev-parse HEAD 2>/dev/null || echo unknown)"

# Chart.yaml stamps its appVersion from this; a tagged build reports the tag,
# an untagged one a describe string, so a chart can always be traced back.
version="$(git describe --tags --always --dirty 2>/dev/null || echo unknown)"

echo "STABLE_GIT_VERSION ${version}"
echo "STABLE_GIT_COMMIT ${commit}"
