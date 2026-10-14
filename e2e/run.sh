#!/usr/bin/env bash
# Runs the end-to-end suite against a kind cluster.
#
# Builds the images from this checkout, loads them into the kind node and runs
# the tests tagged e2e. Creates the cluster if it does not exist; a cluster
# this script created is deleted on exit unless E2E_KEEP_CLUSTER=1.
#
# E2E_CHART=oci://<repository> with E2E_CHART_VERSION=<version> tests a
# published chart instead: nothing is built or loaded, the chart is pulled,
# and the components run the images it pins, pulled from the registry. The
# tests still come from this checkout, so run it from the commit the chart was
# built from.
#
# Extra arguments are passed to every bazel invocation, e.g. --config=ci.
set -euo pipefail

cd "$(dirname "$0")/.."

cluster="${KIND_CLUSTER:-stevedore-e2e}"
images=(stevedore-server stevedore-csi stevedore-provider-s3 stevedore-provider-file)
load_targets=(//server:image_load //csi:image_load //providers/s3:image_load //providers/file:image_load)

created=0
kubeconfig="$(mktemp)"
cleanup() {
	rm -f "${kubeconfig}"
	if [[ "${created}" == 1 && "${E2E_KEEP_CLUSTER:-0}" != 1 ]]; then
		kind delete cluster --name "${cluster}"
	fi
}
trap cleanup EXIT

if ! kind get clusters | grep -qx "${cluster}"; then
	kind create cluster --name "${cluster}" --wait 120s
	created=1
fi

# The tests use the ambient kubeconfig; point it at this cluster only.
kind get kubeconfig --name "${cluster}" >"${kubeconfig}"
export KUBECONFIG="${kubeconfig}"

published="${E2E_CHART:-}"
if [[ -n "${published}" && -z "${E2E_CHART_VERSION:-}" ]]; then
	echo "E2E_CHART needs E2E_CHART_VERSION" >&2
	exit 2
fi
if [[ -z "${published}" ]]; then
	for target in "${load_targets[@]}"; do
		bazel run "$@" "${target}"
	done
	for image in "${images[@]}"; do
		kind load docker-image --name "${cluster}" "localhost/${image}:e2e"
	done
fi

# Install the chart Bazel builds, with Bazel's pinned helm (no helm on PATH
# needed). The release is created or upgraded in place, so a kept cluster is
# reused; the image tag is fixed, so restart every workload afterwards to make
# a pod from an earlier run pick up the image just loaded.
bazel build "$@" //charts/stevedore:stevedore @helm//:helm
bin="$(bazel info "$@" bazel-bin)"
helm="$(bazel cquery "$@" @helm//:helm --output=files 2>/dev/null)"
helm="$(bazel info "$@" execution_root)/${helm}"
chart="${bin}/charts/stevedore/stevedore.tgz"
values=(--values e2e/values.yaml)
if [[ -n "${published}" ]]; then
	pulled="$(mktemp -d)"
	"${helm}" pull "${published}" --version "${E2E_CHART_VERSION}" --destination "${pulled}"
	chart="$(ls "${pulled}"/*.tgz)"
else
	values+=(--values e2e/values-local.yaml)
fi

# The origin of the chart's file provider: a ConfigMap the tests add keys to.
# Recreated, so a kept cluster starts each run without the last run's keys.
kubectl create namespace stevedore-system --dry-run=client -o yaml | kubectl apply -f -
kubectl delete configmap stevedore-e2e-config -n stevedore-system --ignore-not-found
kubectl create configmap stevedore-e2e-config -n stevedore-system

# A collector for the components to push their metrics to. It prints what it
# receives, which is what the telemetry test reads.
kubectl apply -n stevedore-system -f e2e/otel-collector.yaml

# E2E_TLS=spiffe runs the components on identities from a SPIRE agent instead
# of the chart's own certificates, and only the tests that tell whether that
# works: everything else does not depend on where a certificate comes from.
tests=()
# E2E_FILTER runs only the tests whose name matches, for working on one.
if [[ -n "${E2E_FILTER:-}" ]]; then
	tests=(--test_filter="${E2E_FILTER}")
fi
if [[ "${E2E_TLS:-generated}" == spiffe ]]; then
	e2e/spire.sh
	values+=(--values e2e/values-spiffe.yaml)
	tests=(--test_filter='TestDriverRegistersWithKubelet|TestConfigIsDeliveredAndUpdated|TestS3Provider|TestServerCacheSurvivesARollingRestart|TestServerRefusesClientsWithoutACertificate')
fi

"${helm}" upgrade --install stevedore "${chart}" \
	--namespace stevedore-system \
	"${values[@]}" \
	--wait --timeout 300s
workloads=(daemonset/stevedore-csi deployment/stevedore-server deployment/stevedore-provider-file deployment/stevedore-provider-s3)
# The provider with a small quota, for the eviction test. It runs on the
# certificate the chart issued to the file provider, so only where the chart
# issues certificates.
if [[ "${E2E_TLS:-generated}" == generated ]]; then
	if [[ -n "${published}" ]]; then
		# The chart's own file provider image, as the chart deploys it.
		file_image="$(kubectl get deployment stevedore-provider-file -n stevedore-system \
			-o jsonpath='{.spec.template.spec.containers[0].image}')"
		sed -e "s|localhost/stevedore-provider-file:e2e|${file_image}|" \
			-e 's|imagePullPolicy: Never|imagePullPolicy: IfNotPresent|' e2e/small-provider.yaml |
			kubectl apply -n stevedore-system -f -
	else
		kubectl apply -n stevedore-system -f e2e/small-provider.yaml
	fi
	workloads+=(deployment/stevedore-provider-small)
fi
for workload in "${workloads[@]}"; do
	kubectl rollout restart "${workload}" -n stevedore-system
done
# The suite checks the driver's rollout itself too.
for workload in "${workloads[@]}"; do
	kubectl rollout status "${workload}" -n stevedore-system --timeout=300s
done

# The version the images were stamped with, for the suite to look for, and
# for a published chart the images it runs.
test_env=()
if [[ -n "${published}" ]]; then
	version="$("${helm}" show chart "${chart}" | sed -n 's/^appVersion: *//p' | tr -d '"')"
	pins=""
	# <image name>=<workload>/<container>
	for deployed in stevedore-csi=daemonset/stevedore-csi/driver stevedore-server=deployment/stevedore-server/server \
		stevedore-provider-file=deployment/stevedore-provider-file/provider stevedore-provider-s3=deployment/stevedore-provider-s3/provider; do
		name="${deployed%%=*}" workload="${deployed#*=}"
		pins+=" ${name}=$(kubectl get "${workload%/*}" -n stevedore-system \
			-o jsonpath="{.spec.template.spec.containers[?(@.name=='${workload##*/}')].image}")"
	done
	test_env+=(--test_env=E2E_IMAGES="${pins# }")
else
	version="$(bazel/workspace_status.sh | sed -n 's/^STABLE_GIT_VERSION //p')"
fi
bazel test "$@" --config=e2e --test_env=E2E_VERSION="${version}" "${test_env[@]}" "${tests[@]}" //e2e/...

# The chart's own certificates: renewal on upgrade, picked up with no restart.
if [[ "${E2E_TLS:-generated}" == generated ]]; then
	e2e/rotate.sh "${helm}" "${chart}" "$@"
fi
