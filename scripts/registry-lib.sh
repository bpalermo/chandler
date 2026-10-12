#!/usr/bin/env bash
# Read-only OCI registry helpers. SOURCED, never executed.
#
# Everything here speaks the plain OCI distribution API against $REGISTRY_HOST,
# which the caller sets (scripts/verify-image-signatures.sh takes it from each
# reference). The one registry-specific thing is where the pull token is handed
# out. GET/HEAD only: nothing in this file can push, tag or delete.

# The pull-token endpoint for one repository on $REGISTRY_HOST: quay.io serves
# it at /v2/auth, most others (ghcr.io among them) at /token. Both answer
# `{"token": "..."}`.
registry_token_url() {
	local repo="$1" path=token
	[ "$REGISTRY_HOST" = quay.io ] && path=v2/auth
	printf 'https://%s/%s?service=%s&scope=repository:%s:pull\n' \
		"$REGISTRY_HOST" "$path" "$REGISTRY_HOST" "$repo"
}

# Bearer token for pulling from one repository. Anonymous for a public
# repository, which is what lets the verifier run from a workstation with no
# credentials; REGISTRY_USERNAME + REGISTRY_PASSWORD when set. The credentials
# were issued by ONE host, REGISTRY_CREDENTIAL_HOST, and are sent to no other.
registry_token() {
	local repo="$1" url
	registry__host_ok || return 2
	url="$(registry_token_url "$repo")"
	if [ -n "${REGISTRY_PASSWORD:-}" ] && [ "${REGISTRY_CREDENTIAL_HOST:-}" = "$REGISTRY_HOST" ]; then
		curl -fsS -u "${REGISTRY_USERNAME:-x}:${REGISTRY_PASSWORD}" "$url" | registry__json_str token
	else
		curl -fsS "$url" | registry__json_str token
	fi
}

REGISTRY_MANIFEST_ACCEPT='application/vnd.oci.image.index.v1+json,application/vnd.docker.distribution.manifest.list.v2+json,application/vnd.oci.image.manifest.v1+json,application/vnd.docker.distribution.manifest.v2+json'

# Does ONE tag exist? `HEAD /v2/<repo>/manifests/<tag>`, never a listing: a
# listing is paged, and a tag can fall between pages.
#   0 present (200)   1 absent (404)   2 unknown (anything else; never "absent")
registry_tag_exists() {
	local repo="$1" tag="$2" tok="$3" code
	registry__host_ok || return 2
	code="$(curl -sS --retry 2 -o /dev/null -w '%{http_code}' -I \
		-H "Authorization: Bearer $tok" -H "Accept: ${REGISTRY_MANIFEST_ACCEPT}" \
		"https://${REGISTRY_HOST}/v2/${repo}/manifests/${tag}")" || true
	case "$code" in
	200) return 0 ;;
	404) return 1 ;;
	*)
		echo "registry_tag_exists: HEAD ${REGISTRY_HOST}/v2/${repo}/manifests/${tag} answered '${code:-nothing}'" >&2
		return 2
		;;
	esac
}

# Every child manifest digest of a multi-arch index, one per line. Fails with
# nothing printed when the digest is not an index or lists no children: a walk
# over nothing must never read as "all children verified".
registry_index_children() {
	local repo="$1" digest="$2" tok="$3" body
	registry__host_ok || return 2
	body="$(curl -fsS -H "Authorization: Bearer $tok" \
		-H 'Accept: application/vnd.oci.image.index.v1+json,application/vnd.docker.distribution.manifest.list.v2+json' \
		"https://${REGISTRY_HOST}/v2/${repo}/manifests/${digest}")" || return 1
	printf '%s' "$body" | registry__json_children
}

# The OCI 1.1 Referrers API: `GET /v2/<repo>/referrers/<digest>`, following
# `Link: rel="next"` and printing ONE merged index.
#   0  200: the merged referrers index on stdout
#   1  404: this registry has no Referrers API (a registry that has one answers
#      200 with an empty index for an unknown subject, never 404)
#   2  anything else: unknown, never "no referrers"
registry_referrers() {
	local repo="$1" digest="$2" tok="$3" url hdr body code pages="" next rc=0
	registry__host_ok || return 2
	url="https://${REGISTRY_HOST}/v2/${repo}/referrers/${digest}"
	hdr="$(mktemp)"
	body="$(mktemp)"
	while [ -n "$url" ]; do
		code="$(curl -sS --retry 2 -D "$hdr" -o "$body" -w '%{http_code}' \
			-H "Authorization: Bearer $tok" \
			-H 'Accept: application/vnd.oci.image.index.v1+json' "$url")" || true
		case "$code" in
		200) ;;
		404)
			if [ -z "$pages" ]; then rc=1; else rc=2; fi
			break
			;;
		*)
			echo "registry_referrers: GET ${url} answered '${code:-nothing}'" >&2
			rc=2
			break
			;;
		esac
		pages="${pages}$(cat "$body")"$'\n'
		next="$(tr -d '\r' <"$hdr" | sed -nE 's/^[Ll]ink:[[:space:]]*<([^>]+)>;.*rel="?next"?.*/\1/p' | head -1)"
		case "$next" in
		"") url="" ;;
		https://*) url="$next" ;;
		/*) url="https://${REGISTRY_HOST}${next}" ;;
		*)
			echo "registry_referrers: unusable Link next '${next}'" >&2
			rc=2
			break
			;;
		esac
	done
	rm -f "$hdr" "$body"
	[ "$rc" = 0 ] || return "$rc"
	printf '%s' "$pages" | registry__json_merge_index || {
		echo "registry_referrers: ${REGISTRY_HOST}/v2/${repo}/referrers/${digest} is not an OCI index" >&2
		return 2
	}
}

# What a cosign SIGNATURE looks like as a referrer. A sigstore bundle is also
# what `cosign attest` attaches, so a bundle counts as a signature only when
# its predicate type is cosign's sign predicate.
REGISTRY_SIGSTORE_BUNDLE_TYPE='application/vnd.dev.sigstore.bundle.v0.3+json'
REGISTRY_COSIGN_SIG_TYPE='application/vnd.dev.cosign.artifact.sig.v1+json'
REGISTRY_COSIGN_SIGN_PREDICATE='https://sigstore.dev/cosign/sign/v1'

# Where the signature of <digest> is published, from direct lookups: the
# referrers API, then BOTH tag shapes by name (cosign 2's `sha256-<hex>.sig`,
# cosign 3's `sha256-<hex>` fallback), so a double-write is seen.
# Echoes `referrer`, `bundle`, `legacy`, `none` or `both` (more than one).
# Returns 2 with nothing printed when any lookup goes unanswered.
registry_signature_layout() {
	local repo="$1" digest="$2" tok="$3" has_legacy has_bundle has_ref=0 refs rc=0
	refs="$(registry_referrers "$repo" "$digest" "$tok")" || rc=$?
	case "$rc" in
	0) has_ref="$(printf '%s' "$refs" | registry__json_has_signature_referrer)" || return 2 ;;
	1) has_ref=0 ;;
	*) return 2 ;;
	esac
	rc=0
	registry_tag_exists "$repo" "${digest/:/-}.sig" "$tok" || rc=$?
	case "$rc" in 0) has_legacy=1 ;; 1) has_legacy=0 ;; *) return 2 ;; esac
	rc=0
	registry_tag_exists "$repo" "${digest/:/-}" "$tok" || rc=$?
	case "$rc" in 0) has_bundle=1 ;; 1) has_bundle=0 ;; *) return 2 ;; esac
	if [ $((has_legacy + has_bundle + has_ref)) -gt 1 ]; then
		printf 'both\n'
	elif [ "$has_legacy" = 1 ]; then
		printf 'legacy\n'
	elif [ "$has_bundle" = 1 ]; then
		printf 'bundle\n'
	elif [ "$has_ref" = 1 ]; then
		printf 'referrer\n'
	else
		printf 'none\n'
	fi
}

# Does a signature found in <layout> satisfy the <expected> SIGNATURE_LAYOUT?
# `referrer` wants exactly a referrer; `tag` wants exactly one tag shape.
registry_layout_satisfies() {
	case "$2" in
	referrer) [ "$1" = referrer ] ;;
	tag) [ "$1" = legacy ] || [ "$1" = bundle ] ;;
	*) return 2 ;;
	esac
}

# --- internals -------------------------------------------------------------

registry__host_ok() {
	[ -n "${REGISTRY_HOST:-}" ] && return 0
	echo "registry-lib: REGISTRY_HOST is not set" >&2
	return 2
}

registry__json_str() {
	python3 -c 'import sys,json;print(json.load(sys.stdin)[sys.argv[1]])' "$1"
}

registry__json_children() {
	python3 -c '
import json, re, sys
doc = json.load(sys.stdin)
kids = doc.get("manifests")
if not isinstance(kids, list) or not kids:
    sys.exit(1)
digests = [k.get("digest", "") if isinstance(k, dict) else "" for k in kids]
if not all(re.fullmatch(r"sha256:[0-9a-f]{64}", d) for d in digests):
    sys.exit(1)
print("\n".join(digests))
'
}

registry__json_merge_index() {
	python3 -c '
import json, sys
dec = json.JSONDecoder()
text = sys.stdin.read()
i, out, pages = 0, [], 0
while True:
    while i < len(text) and text[i].isspace():
        i += 1
    if i >= len(text):
        break
    doc, i = dec.raw_decode(text, i)
    if not isinstance(doc, dict) or not isinstance(doc.get("manifests"), list):
        sys.exit(1)
    out.extend(doc["manifests"])
    pages += 1
if pages == 0:
    sys.exit(1)
print(json.dumps({"schemaVersion": 2, "mediaType": "application/vnd.oci.image.index.v1+json", "manifests": out}))
'
}

registry__json_has_signature_referrer() {
	python3 -c '
import json, sys
bundle, sig, pred = sys.argv[1:4]
doc = json.load(sys.stdin)
kids = doc.get("manifests") if isinstance(doc, dict) else None
if not isinstance(kids, list):
    sys.exit(1)
def is_sig(m):
    if not isinstance(m, dict):
        return False
    at = m.get("artifactType", "")
    if at == sig:
        return True
    ann = m.get("annotations") or {}
    return at == bundle and ann.get("dev.sigstore.bundle.predicateType") == pred
print(1 if any(is_sig(m) for m in kids) else 0)
' "$REGISTRY_SIGSTORE_BUNDLE_TYPE" "$REGISTRY_COSIGN_SIG_TYPE" "$REGISTRY_COSIGN_SIGN_PREDICATE"
}
