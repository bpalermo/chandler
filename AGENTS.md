# AGENTS.md

Guidance for agents working in this repository.

## What this is

Chandler delivers configuration files to pods through a CSI driver. Three
binaries: the CSI node driver (`csi`), the config server (`server`) and the S3
reference provider (`providers/s3`). [docs/design.md](docs/design.md) is the
design and the protos in `api/chandler/v1` are the contracts; read both before
changing behaviour, and update them in the same pull request when behaviour
changes.

The project is early. The driver mounts an empty volume; the server and the
provider are placeholders. The design describes the target, not the code.

## Build and test

```bash
bazel test //...            # builds and runs the unit tests
bazel run //:gazelle        # after adding, removing or renaming any .go file
bazel mod tidy              # after changing go.mod or a bazel_dep
e2e/run.sh                  # end-to-end in kind (needs kind, kubectl, helm, docker)
```

Bazel is the only build. The Go packages for the protos exist only as Bazel
targets, so host `go build`, `go vet` and `go mod tidy` cannot resolve them. Do
not use them, and do not "fix" go.mod with `go mod tidy`.

Change a Go dependency through rules_go's SDK:

```bash
bazel run @rules_go//go -- get github.com/some/module@v1.2.3
bazel run @rules_go//go -- get -tool github.com/some/cmd
```

Follow it with `bazel mod tidy` and `bazel run //:gazelle`.

CI runs with `--config=ci`, which fails if `MODULE.bazel.lock` is out of date,
and requires that Gazelle leaves no diff. Regenerate BUILD files rather than
hand-editing what Gazelle owns.

## Layout

- No `cmd/`. Each binary lives in a root-level directory with its `main.go`:
  `csi/`, `server/`, `providers/<name>/`.
- Code private to a binary goes under its `internal/`.
- Code shared between binaries goes under `common/`.
- `api/chandler/v1` holds the protos. The Go import path is
  `github.com/bpalermo/chandler/api/chandler/v1`.
- `bazel/` holds build rules, `scripts/` release tooling, `charts/chandler` the
  Helm chart, `e2e/` the end-to-end suite.

## Protos

- Edition 2024. The generated Go uses the opaque API: builders, getters and
  setters, never struct fields.
- Every message, field, enum value, service and RPC carries a comment. The
  comments are the reference documentation, so say what a field means, when it
  is set and what happens when it is not.
- Never reuse or renumber a field.

## Design rules that code must keep

- **Configuration is opaque.** The server and the driver never parse or
  validate file contents. They check only the digest, the size, the file count
  and that paths stay inside the volume. Validity is the provider's concern.
- **Publishing is the provider's concern.** How configuration gets into a
  provider is outside Chandler. The S3 provider is a reference implementation,
  not the only one.
- **The driver ACKs on write.** An ACK means the files were written, nothing
  more.
- **Replacement is atomic and the last known good version is kept.** All
  writes into a volume go through `csi/internal/atomic`.
- **A version is a content address** (`sha256:<hex>`, see `common/bundle`); a
  revision is the provider's own change marker. They are never interchangeable.
- **The server keeps no persistent storage.** Its cache lives on an `emptyDir`
  and replicas copy from each other.
- **The application-facing API is HTTP over a Unix socket**, not gRPC, so
  applications need no SDK.

## Tests

- Unit tests sit next to the code and run under `bazel test //...`.
- The end-to-end suite in `e2e/` is tagged `e2e` and excluded from
  `bazel test //...`. `e2e/run.sh` creates or reuses the kind cluster
  `chandler-e2e`, loads the images, installs the chart built from the checkout
  and runs the suite. Set `E2E_KEEP_CLUSTER=1` to keep the cluster.
- Every change that adds user-visible behaviour extends the end-to-end suite.
- The S3 provider is validated against a LocalStack sidecar, not a real bucket.
- Pin test images by digest.

## Images, chart and publishing

- The registry is named in one build file, `bazel/registry.bzl`. Shell and
  workflows read it through `scripts/registry.sh`;
  `scripts/check-registry-config.sh` fails CI if it is spelled out anywhere
  else. The README is the one exception, because people copy install commands
  from it.
- The publish workflow reads files from `bazel-out`. An output that is not a
  target's default output is absent there on a cache hit, so request its
  output group in the build step (see `--output_groups=+digest`).
- Image repositories are `server`, `csi` and `provider-s3`; the chart is
  `chandler`.
- Images are built with the `go_image` macro in `bazel/image/defs.bzl`.
- The chart's version and image pin are stamped at package time. Do not write a
  version or a digest into `Chart.yaml` or `values.yaml` by hand.
  `//charts/chandler:pin_test` checks the result.
- `.github/workflows/publish.yml` publishes and signs on every push to `main`.
  It cannot be exercised from a pull request, so read a change to it twice and
  watch the run after merge.

## Pull requests

- `main` is protected: squash merges only, linear history, and the `ci` check
  must pass. `ci` aggregates the Bazel job and the kind end-to-end job.
- GitHub Actions are pinned by commit SHA, with the version in a comment.
- Keep a pull request to one slice of behaviour, with its tests.
