# Chandler

Chandler delivers configuration files to Kubernetes pods through a CSI driver.
A pod names the configuration it wants with URIs; the driver mounts the files
read-only and replaces them atomically when they change.

> **Status: early development.** Configuration is delivered end to end from
> the file and S3 providers, and updated in place. Connections use mutual TLS,
> but there is no authorisation yet: any pod can read any URI the cluster's
> providers can. See
> [What works today](#what-works-today).

## How it is meant to work

```mermaid
flowchart LR
  pod["Pod volume"] --- driver["CSI driver<br/>(one per node)"]
  driver -- "gRPC stream, ACK/NACK" --> server["Config server<br/>(replicated)"]
  server -- "gRPC stream" --> s3["S3 provider"]
  server -- "gRPC stream" --> other["Other providers"]
```

- **CSI driver.** Runs on every node, subscribes to the config server for each
  volume, writes the files, and keeps the last known good version when anything
  upstream fails.
- **Config server.** Caches what providers emit and pushes it to the drivers.
  When a provider releases a new version to a share of the fleet first, the
  server decides which pods are in the share. The protocol
  acknowledges every update, in the style of Envoy xDS.
- **Providers.** Plugins behind one streaming gRPC interface. Each provider
  owns how configuration is published, validated and rolled out. This repository has a
  file provider, which serves a directory such as a mounted ConfigMap, and an
  S3 provider as a reference implementation.

Configuration content is opaque to the server and the driver. They deliver
bytes; they never parse them.

A pod asks for configuration with an inline volume:

```yaml
volumes:
  - name: config
    csi:
      driver: csi.chandler.dev
      readOnly: true
      volumeAttributes:
        sources: |
          - uri: file:///payments/app.yaml
            path: app.yaml
          - uri: file:///shared/flags/
            path: flags
```

The scheme names the provider. A URI ending in `/` is a directory; any other
is one file, placed at `path`.

The full design, including caching, staged rollouts and application
acknowledgement, is in [docs/design.md](docs/design.md). The wire contracts are
in [api/chandler/v1](api/chandler/v1).

## What works today

| Part | State |
|---|---|
| Protocol definitions | Written, for all three protocols |
| CSI driver | Mounts a volume once every source is delivered, replaces files atomically on change, and resumes after a restart. Keeps a node cache, so a pod with `failurePolicy: UseCached` can start while the server is down. With `medium: Memory` a volume never touches the node's disk, which content marked `no_store` requires |
| Config server | Shares one provider watch between subscriptions and pushes each new revision. Caches what providers emit, with quotas, and keeps serving it while a provider is down. Runs as several replicas that copy each other's cache, so it survives a rollout without a persistent volume |
| File provider | Serves files and directories from a volume, such as a ConfigMap |
| S3 provider | Serves objects and prefixes from allow-listed buckets, on AWS or an S3-compatible store |
| Helm chart | Installs the driver and the server, and optionally the file and S3 providers |
| Security | Mutual TLS between all components, with a chart-issued private authority by default, your own certificates, or identities from a SPIFFE Workload API such as SPIRE. The server verifies which pod a volume belongs to from its service account token. Not built: authorisation of which pods may read which URIs |
| Staged rollouts | A provider releases a new version to a growing share of pods and can hold it or take it back; the file and S3 providers have a time-based ramp |
| Metrics and events | Every component pushes metrics over OTLP to an OpenTelemetry collector (`telemetry.otlpEndpoint`). The driver records events on pods when their configuration is refused, rejected, late, stale or served from a cache |
| Application acknowledgement | A volume with `appAck: "true"` gets a manifest and an API on a Unix socket, as plain HTTP with JSON or as gRPC. The application reports whether it loaded each version; a rejected one is reverted at once and reported to the provider. With `appAckTimeout`, silence is reported too, and can hold a rollout |

The design document ends with a
[detailed status table](docs/design.md#13-implementation-status).

## Install

The chart and images are published to quay.io on every commit to `main`, and
signed with cosign.

| Artifact | Location |
|---|---|
| Chart | `oci://quay.io/shipchandler/chandler` |
| Images | `quay.io/shipchandler/csi`, `server`, `provider-file`, `provider-s3` |

Every chart is currently a snapshot, versioned
`0.1.0-dev.<commit time>.g<commit>`. Snapshots are pre-releases, so pass
`--devel` to take the newest one, or `--version` to pin one:

```bash
helm install chandler oci://quay.io/shipchandler/chandler \
  --namespace chandler-system --create-namespace --devel
```

That installs the driver and the server with no provider, so no URI resolves
yet.

Each chart pins its images by digest, so a chart always runs the images built
from the same commit.

To serve a ConfigMap, enable the file provider and point it at one in the
release's namespace:

```bash
kubectl create namespace chandler-system
kubectl create configmap app-config -n chandler-system --from-literal=app.yaml='replicas: 1'

helm install chandler oci://quay.io/shipchandler/chandler \
  --namespace chandler-system --devel \
  --set providers.file.enabled=true \
  --set providers.file.source.configMap.name=app-config
```

A pod then mounts `file:///app.yaml` as shown above. A change to the ConfigMap
reaches the pod's files after kubelet refreshes the provider's mount, about a
minute.

To serve S3 buckets instead, enable the S3 provider and name the buckets it
may serve. With IAM roles for service accounts:

```bash
helm install chandler oci://quay.io/shipchandler/chandler \
  --namespace chandler-system --create-namespace --devel \
  --set providers.s3.enabled=true \
  --set providers.s3.region=eu-west-1 \
  --set 'providers.s3.buckets={acme-config}' \
  --set-string 'providers.s3.serviceAccount.annotations.eks\.amazonaws\.com/role-arn=arn:aws:iam::111122223333:role/chandler-s3'
```

Pods then mount `s3://acme-config/payments/app.yaml`, or a prefix as a
directory with `s3://acme-config/shared/flags/`. Without `buckets`, the
provider serves every bucket its role can read to every pod.

To verify a signature:

```bash
cosign verify quay.io/shipchandler/csi:dev \
  --certificate-identity-regexp '^https://github\.com/bpalermo/chandler/\.github/workflows/publish\.yml@refs/heads/main$' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

## Develop

Bazel is the only build; [Bazelisk](https://github.com/bazelbuild/bazelisk)
picks the version from `.bazelversion`, and Bazel downloads the Go toolchain.

```bash
bazel test //...        # build and unit tests
bazel run //:gazelle    # after adding, removing or renaming a Go file
e2e/run.sh              # end-to-end suite in a kind cluster
```

The end-to-end suite needs `kind`, `kubectl`, `helm` and a container runtime.
It builds the images and the chart from the checkout and installs them.

| Directory | Contents |
|---|---|
| `api/chandler/v1` | Protocol definitions |
| `csi` | CSI node driver |
| `server` | Config server |
| `providers/file` | File provider |
| `providers/s3` | S3 reference provider |
| `common` | Code shared between the binaries |
| `charts/chandler` | Helm chart |
| `e2e` | End-to-end tests |
| `bazel`, `scripts` | Build rules and release tooling |

[AGENTS.md](AGENTS.md) has the conventions for working in the repository.

## License

[Apache 2.0](LICENSE)
