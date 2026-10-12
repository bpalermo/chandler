#!/usr/bin/env bash
# Runs the end-to-end suite against a kind cluster.
#
# Builds the images from this checkout, loads them into the kind node and runs
# the tests tagged e2e. Creates the cluster if it does not exist; a cluster
# this script created is deleted on exit unless E2E_KEEP_CLUSTER=1.
#
# Extra arguments are passed to every bazel invocation, e.g. --config=ci.
set -euo pipefail

cd "$(dirname "$0")/.."

cluster="${KIND_CLUSTER:-chandler-e2e}"
images=(chandler-server chandler-csi chandler-provider-s3 chandler-provider-file)
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
kind get kubeconfig --name "${cluster}" > "${kubeconfig}"
export KUBECONFIG="${kubeconfig}"

for target in "${load_targets[@]}"; do
  bazel run "$@" "${target}"
done
for image in "${images[@]}"; do
  kind load docker-image --name "${cluster}" "localhost/${image}:e2e"
done

# Install the chart Bazel builds, with Bazel's pinned helm (no helm on PATH
# needed). The release is created or upgraded in place, so a kept cluster is
# reused; the image tag is fixed, so restart every workload afterwards to make
# a pod from an earlier run pick up the image just loaded.
bazel build "$@" //charts/chandler:chandler @helm//:helm
bin="$(bazel info "$@" bazel-bin)"
helm="$(bazel cquery "$@" @helm//:helm --output=files 2>/dev/null)"
helm="$(bazel info "$@" execution_root)/${helm}"

# The origin of the chart's file provider: a ConfigMap the tests add keys to.
# Recreated, so a kept cluster starts each run without the last run's keys.
kubectl create namespace chandler-system --dry-run=client -o yaml | kubectl apply -f -
kubectl delete configmap chandler-e2e-config -n chandler-system --ignore-not-found
kubectl create configmap chandler-e2e-config -n chandler-system

# A collector for the components to push their metrics to. It prints what it
# receives, which is what the telemetry test reads.
kubectl apply -n chandler-system -f e2e/otel-collector.yaml

"${helm}" upgrade --install chandler "${bin}/charts/chandler/chandler.tgz" \
  --namespace chandler-system \
  --values e2e/values.yaml \
  --wait --timeout 300s
workloads=(daemonset/chandler-csi deployment/chandler-server deployment/chandler-provider-file deployment/chandler-provider-s3)
for workload in "${workloads[@]}"; do
  kubectl rollout restart "${workload}" -n chandler-system
done
# The suite checks the driver's rollout itself too.
for workload in "${workloads[@]}"; do
  kubectl rollout status "${workload}" -n chandler-system --timeout=300s
done

bazel test "$@" --config=e2e //e2e/...
