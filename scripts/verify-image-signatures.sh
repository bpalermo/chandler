#!/usr/bin/env bash
# cosign-verify published images: each INDEX and EVERY child manifest it lists,
# and assert WHERE each signature is published.
#
# The publish workflow signs with `cosign sign --recursive`, which signs a
# multi-arch index and each per-architecture manifest under it. `cosign verify`
# has no --recursive, so verifying the index alone would pass while the
# manifests a node actually pulls went unchecked. This walks the children the
# way the signer does and verifies each one with the same identity and issuer.
#
# Then it asks the REGISTRY which layout landed, rather than asking the binary
# that wrote the signature whether it likes what it wrote: `cosign verify`
# accepts every layout cosign has ever written. EXPECT_SIGNATURE_LAYOUT (default:
# SIGNATURE_LAYOUT of bazel/registry.bzl when this runs from a checkout, else
# `referrer`) is asserted for every manifest: `referrer` means an OCI 1.1
# referrer and NO `sha256-<hex>` tag of either shape; a signature in two places
# is a failure, not a healthy signature. Set it to `any` to skip the assertion.
#
#   bazel run //bazel/cosign:verify_image -- <ref> [<ref>...]
#   bazel run //bazel/cosign:verify_image -- --single <chart ref>
#   COSIGN=/path/to/cosign scripts/verify-image-signatures.sh <ref>...
#
# Each ref is `<registry>/<repo>@sha256:<digest>`: a digest, never a tag. It is
# the digest of a multi-arch index, or with --single of one manifest, such as
# a Helm chart's.
#
# ENVIRONMENT
#   COSIGN                cosign binary (default `cosign` on PATH); must be v3+.
#   CERT_IDENTITY_REGEXP  default: publish.yml of bpalermo/stevedore on main.
#   CERT_OIDC_ISSUER      default https://token.actions.githubusercontent.com
#   REGISTRY_USERNAME, REGISTRY_PASSWORD, REGISTRY_CREDENTIAL_HOST
#                         optional; public repositories read anonymously.
#
# READ-ONLY. Exit 0: everything verified. 1: a manifest failed verification or
# is signed in the wrong layout. 2: the check could not be performed.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/registry-lib.sh
. "${here}/registry-lib.sh"

cosign_bin="${COSIGN:-cosign}"
identity="${CERT_IDENTITY_REGEXP:-^https://github\.com/${GITHUB_REPOSITORY:-bpalermo/stevedore}/\.github/workflows/publish\.yml@refs/heads/main$}"
issuer="${CERT_OIDC_ISSUER:-https://token.actions.githubusercontent.com}"
expect="${EXPECT_SIGNATURE_LAYOUT:-}"
if [ -z "$expect" ]; then
	if [ -x "${here}/registry.sh" ]; then
		expect="$("${here}/registry.sh" signature-layout)" || exit 2
	else
		expect=referrer
	fi
fi
case "$expect" in referrer | tag | any) ;; *)
	echo "::error::EXPECT_SIGNATURE_LAYOUT must be referrer, tag or any (got '${expect}')" >&2
	exit 2
	;;
esac

# --single: the refs are single manifests, not indexes -- a Helm chart, say --
# so there are no children to walk and asking for them would be an error.
single=0
if [ "${1:-}" = "--single" ]; then
	single=1
	shift
fi
refs=("$@")
if [ "${#refs[@]}" -eq 0 ]; then
	echo "::error::no image refs to verify" >&2
	exit 2
fi
if ! command -v "$cosign_bin" >/dev/null 2>&1; then
	echo "::error::cosign not found (${cosign_bin})" >&2
	exit 2
fi

verified=0
failed=0

# check <what> <repo> <digest> <token>
check() {
	local what="$1" repo="$2" digest="$3" tok="$4" ref err layout
	ref="${REGISTRY_HOST}/${repo}@${digest}"
	err="$(mktemp)"
	if "$cosign_bin" verify \
		--certificate-identity-regexp "$identity" \
		--certificate-oidc-issuer "$issuer" \
		"$ref" >/dev/null 2>"$err"; then
		echo "  verified ${what} ${ref}"
		verified=$((verified + 1))
	else
		echo "  FAILED   ${what} ${ref}: $(tail -1 "$err")"
		echo "::error::signature did not verify: ${what} ${ref}"
		failed=$((failed + 1))
	fi
	rm -f "$err"
	[ "$expect" = any ] && return 0
	if ! layout="$(registry_signature_layout "$repo" "$digest" "$tok")"; then
		echo "::error::could not determine the signature layout of ${ref}" >&2
		exit 2
	fi
	if registry_layout_satisfies "$layout" "$expect"; then
		echo "           layout ${layout}"
	else
		echo "  FAILED   ${what} ${ref}: signature layout is '${layout}', want '${expect}'"
		echo "::error::signature layout '${layout}' (want '${expect}'): ${what} ${ref}"
		failed=$((failed + 1))
	fi
}

for ref in "${refs[@]}"; do
	if ! [[ "$ref" =~ ^([a-z0-9]([a-z0-9.-]*[a-z0-9])?(:[0-9]+)?)/([^@]+)@(sha256:[0-9a-f]{64})$ ]]; then
		echo "::error::not a <registry>/<repo>@sha256:<digest> reference: ${ref}" >&2
		exit 2
	fi
	REGISTRY_HOST="${BASH_REMATCH[1]}"
	repo="${BASH_REMATCH[4]}"
	digest="${BASH_REMATCH[5]}"

	echo "${ref}"
	if ! tok="$(registry_token "$repo")" || [ -z "$tok" ]; then
		echo "::error::could not obtain a pull token for ${repo}" >&2
		exit 2
	fi
	if [ "$single" = 1 ]; then
		check manifest "$repo" "$digest" "$tok"
		continue
	fi
	check index "$repo" "$digest" "$tok"
	if ! children="$(registry_index_children "$repo" "$digest" "$tok")" || [ -z "$children" ]; then
		echo "::error::could not enumerate the child manifests of ${ref} (not an index, or no children)" >&2
		exit 2
	fi
	n_children=0
	while read -r child; do
		check child "$repo" "$child" "$tok"
		n_children=$((n_children + 1))
	done <<<"$children"
	echo "  ${n_children} child manifest(s) walked"
done

echo ""
# A chart is one manifest: saying "index(es)" of it would misdescribe what was
# checked, in the failure as much as in the pass.
if [ "$single" = 1 ]; then
	if [ "$failed" -gt 0 ]; then
		echo "FAIL: ${failed} check(s) failed, ${verified} manifest(s) verified, of ${#refs[@]} single manifest(s)"
		exit 1
	fi
	echo "PASS: ${verified} manifest(s) verified, layout ${expect}"
	exit 0
fi
if [ "$failed" -gt 0 ]; then
	echo "FAIL: ${failed} check(s) failed, ${verified} manifest(s) verified, across ${#refs[@]} index(es)"
	exit 1
fi
echo "PASS: ${verified} manifest(s) verified (${#refs[@]} index(es) + their children), layout ${expect}"
