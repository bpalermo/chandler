#!/usr/bin/env bash
# Deploys SPIRE into the current cluster and registers Chandler's components,
# for the end-to-end suite's SPIFFE variant (see run.sh, E2E_TLS=spiffe).
#
# Each component is identified by the pod it runs in: the chart's namespace
# and its component label. The identities are the ones e2e/values-spiffe.yaml
# tells the chart to expect.
set -euo pipefail

cd "$(dirname "$0")/.."

kubectl apply -f e2e/spire.yaml
kubectl rollout status deployment/spire-server -n spire --timeout=180s
kubectl rollout status daemonset/spire-agent -n spire --timeout=180s

server() {
  kubectl exec -n spire deployment/spire-server -- /opt/spire/bin/spire-server "$@"
}

# Entries survive a rerun on a kept cluster only as long as the server's pod
# does; creating one that exists is an error, so ask first.
existing="$(server entry show || true)"
register() {
  local id="$1"
  shift
  if grep -qF "${id}" <<<"${existing}"; then
    return
  fi
  server entry create -spiffeID "${id}" "$@"
}

# Every agent of this cluster, under one name the workloads can hang from.
register spiffe://chandler.test/node -node -selector k8s_psat:cluster:chandler-e2e
for component in csi server provider-file provider-s3; do
  register "spiffe://chandler.test/chandler/${component}" \
    -parentID spiffe://chandler.test/node \
    -selector k8s:ns:chandler-system \
    -selector "k8s:pod-label:app.kubernetes.io/component:${component}"
done
