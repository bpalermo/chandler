"""Where stevedore's images and chart are published: ONE setting.

Every published coordinate is derived from the constants below -- the
`image_push` of each `go_image` (//server:image_push, //csi:image_push,
//providers/s3:image_push, //providers/file:image_push), the chart push (//charts/stevedore:stevedore.push_registry),
the chart's default image repositories (charts/stevedore/values.yaml, which
takes each from its image_push), and -- through scripts/registry.sh, which parses THIS
file -- every workflow step and verifier. Nothing else may spell the registry
out: scripts/check-registry-config.sh fails CI on a literal anywhere outside
its allow-list.

The coordinates:

    <IMAGE_REGISTRY>/<IMAGE_NAMESPACE>/server
    <IMAGE_REGISTRY>/<IMAGE_NAMESPACE>/csi
    <IMAGE_REGISTRY>/<IMAGE_NAMESPACE>/provider-s3
    <IMAGE_REGISTRY>/<IMAGE_NAMESPACE>/provider-file
    oci://<IMAGE_REGISTRY>/<IMAGE_NAMESPACE>/stevedore        (the chart)

Repositories are not prefixed with the project name: the namespace already is
the project. The chart is `stevedore` because no image is.

scripts/registry.sh reads the assignments below with a strict, line-anchored
grep: keep each on ONE line, exactly `NAME = "value"`.
"""

# Registry host.
IMAGE_REGISTRY = "quay.io"

# Organization under the host. Quay has NO nested repositories: everything is
# `<host>/<namespace>/<name>`, one path segment after the org.
IMAGE_NAMESPACE = "stevedore"

# Where cosign's keyless signature lands on IMAGE_REGISTRY, which the publish
# workflow ASSERTS for every digest it signs: "referrer" (an OCI 1.1 referrer
# and no tag -- a registry that serves the Referrers API, as quay.io does) or
# "tag" (cosign 3's `sha256-<hex>` fallback tag -- a registry without it).
SIGNATURE_LAYOUT = "referrer"

def image_repository(component):
    """Repository path (no host) of a component's image.

    Args:
      component: "server", "csi", "provider-s3" or "provider-file".

    Returns:
      e.g. "stevedore/csi".
    """
    return "{}/{}".format(IMAGE_NAMESPACE, component)

def image_reference(component):
    """Host-qualified image repository of a component.

    Args:
      component: "server", "csi", "provider-s3" or "provider-file".

    Returns:
      e.g. "quay.io/stevedore/csi".
    """
    return "{}/{}".format(IMAGE_REGISTRY, image_repository(component))

def chart_push_url():
    """The OCI base `helm push <chart>.tgz` is given (scheme, no chart name).

    `helm push` appends the packaged chart's Chart.yaml `name:` to this base and
    uses the chart version as the tag, so the repository a chart lands in is
    chart_registry_url(<name>).

    Returns:
      "oci://quay.io/stevedore".
    """
    return "oci://{}/{}".format(IMAGE_REGISTRY, IMAGE_NAMESPACE)

def chart_registry_url(chart):
    """The host-qualified OCI repository a chart is pushed to (no scheme, no tag).

    Consumers pull it with `helm pull oci://<this> --version <X.Y.Z>`.

    Args:
      chart: the Chart.yaml name, "stevedore".

    Returns:
      e.g. "quay.io/stevedore/stevedore".
    """
    return "{}/{}/{}".format(IMAGE_REGISTRY, IMAGE_NAMESPACE, chart)
