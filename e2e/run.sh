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
images=(chandler-server chandler-csi chandler-provider-s3)
load_targets=(//server:image_load //csi:image_load //providers/s3:image_load)

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

# The suite expects the driver to be deployed; it checks the rollout itself.
kubectl apply -f e2e/manifests/

bazel test "$@" --config=e2e //e2e/...
