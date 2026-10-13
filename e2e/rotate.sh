#!/usr/bin/env bash
# Checks that the chart renews its own certificates and that the components
# pick the new ones up without restarting. Run by run.sh after the suite, on
# the cluster and release the suite used.
#
# Usage: rotate.sh <helm> <chart.tgz> [extra bazel arguments...]
set -euo pipefail

cd "$(dirname "$0")/.."
helm="$1" chart="$2"
shift 2

ns=chandler-system
cert() { kubectl get secret "$1" -n "${ns}" -o jsonpath='{.data.tls\.crt}' | sha256sum | cut -d' ' -f1; }
restarts() { kubectl get pods -n "${ns}" -l app.kubernetes.io/name=chandler -o jsonpath='{range .items[*]}{.metadata.uid}{"="}{range .status.containerStatuses[*]}{.restartCount}{","}{end}{" "}{end}'; }
# What every server replica presents on the wire, as curl sees it: each is
# asked by its own address, since the Service would answer for only one.
served() {
	local ip
	for ip in $(kubectl get pods -n "${ns}" -l app.kubernetes.io/component=server -o jsonpath='{.items[*].status.podIP}'); do
		kubectl exec -n "${ns}" rotation-probe -- sh -c \
			"curl -svk --max-time 5 https://${ip}:9000 2>&1 | grep -i 'expire date' || echo unreachable"
	done
}
# Whether no replica presents what it did before.
all_renewed() {
	local now line
	now="$(served)"
	[ -n "${now}" ] || return 1
	while IFS= read -r line; do
		if [ "${line}" = unreachable ] || grep -qxF -- "${line}" <<<"${served_before}"; then
			return 1
		fi
	done <<<"${now}"
}

kubectl run rotation-probe -n "${ns}" --restart=Never \
	--image=docker.io/curlimages/curl@sha256:463eaf6072688fe96ac64fa623fe73e1dbe25d8ad6c34404a669ad3ce1f104b6 \
	--command -- sleep 3600
trap 'kubectl delete pod rotation-probe -n "${ns}" --ignore-not-found --wait=false' EXIT
kubectl wait pod/rotation-probe -n "${ns}" --for=condition=Ready --timeout=120s

ca_before="$(cert chandler-ca)"
leaf_before="$(cert chandler-server-tls)"
served_before="$(served)"
restarts_before="$(restarts)"
if [ -z "${served_before}" ] || grep -q unreachable <<<"${served_before}"; then
	echo "rotate.sh: could not read every server replica's certificate" >&2
	exit 1
fi

# An upgrade that changes nothing must change no certificate.
"${helm}" upgrade chandler "${chart}" -n "${ns}" --reuse-values --wait --timeout 300s >/dev/null
if [ "$(cert chandler-server-tls)" != "${leaf_before}" ]; then
	echo "rotate.sh: an upgrade replaced a certificate that was nowhere near expiring" >&2
	exit 1
fi

# One that finds every certificate inside its renewal window must renew them,
# under the same authority. A second apart, so the new expiry differs.
sleep 2
"${helm}" upgrade chandler "${chart}" -n "${ns}" --reuse-values \
	--set tls.generated.renewBeforeDays=100000 --wait --timeout 300s >/dev/null
if [ "$(cert chandler-server-tls)" = "${leaf_before}" ]; then
	echo "rotate.sh: the certificate was not renewed inside its renewal window" >&2
	exit 1
fi
if [ "$(cert chandler-ca)" != "${ca_before}" ]; then
	echo "rotate.sh: renewing a certificate replaced the authority" >&2
	exit 1
fi

# kubelet takes about a minute to bring a changed Secret to a pod; each
# replica then presents the new certificate, with no restart.
renewed=0
for _ in $(seq 100); do
	if all_renewed; then
		renewed=1
		break
	fi
	sleep 3
done
if [ "${renewed}" != 1 ]; then
	echo "rotate.sh: a server replica still presents the old certificate:" >&2
	served >&2
	exit 1
fi
if [ "$(restarts)" != "${restarts_before}" ]; then
	echo "rotate.sh: pods were replaced or restarted to pick up the certificate:" >&2
	echo "  before: ${restarts_before}" >&2
	echo "  after:  $(restarts)" >&2
	exit 1
fi
echo "rotate.sh: the certificates were renewed and picked up without a restart"

# And everything still talks to everything, on the new certificates.
bazel test "$@" --config=e2e \
	--test_filter='TestConfigIsDeliveredAndUpdated|TestS3Provider|TestServerRefusesClientsWithoutACertificate' //e2e/...
