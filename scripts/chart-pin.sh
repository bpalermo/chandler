#!/usr/bin/env bash
# Print the image digest a packaged chart pins for one repository.
#
# The chart's values.yaml sets every image as a `repository:` followed, in the
# same block, by a `digest:`. This prints the digest that follows the given
# repository, so the publish workflow can compare it with the digest of the
# image it is about to push, and the chart's own test can check that a pin is
# there at all.
#
# Usage: chart-pin.sh <chart.tgz> <chart-name> <repository>
#
# Exit 1, with nothing on stdout, unless the repository appears exactly once
# and is followed by a sha256 digest before the next repository.
set -euo pipefail

[ "$#" -eq 3 ] || {
	echo "usage: chart-pin.sh <chart.tgz> <chart-name> <repository>" >&2
	exit 2
}
chart="$1" name="$2" repository="$3"

values="$(tar -xzOf "$chart" "${name}/values.yaml")"
digests="$(printf '%s\n' "$values" | awk -v want="$repository" '
	function unquote(s) { gsub(/^[[:space:]]*"?|"?[[:space:]]*$/, "", s); return s }
	/^[[:space:]]*repository:/ {
		value = $0; sub(/^[[:space:]]*repository:/, "", value)
		inside = (unquote(value) == want)
		if (inside) found++
		next
	}
	inside && /^[[:space:]]*digest:/ {
		value = $0; sub(/^[[:space:]]*digest:/, "", value)
		print unquote(value)
		inside = 0
	}
	END { if (found != 1) exit 1 }
')" || {
	echo "chart-pin.sh: ${repository} does not appear exactly once as a repository in ${name}/values.yaml" >&2
	exit 1
}
if ! [[ "$digests" =~ ^sha256:[0-9a-f]{64}$ ]]; then
	echo "chart-pin.sh: ${repository} is pinned to '${digests}', not to a sha256 digest" >&2
	exit 1
fi
printf '%s\n' "$digests"
