"""A multi-arch, digest-pinned OCI image for a Go binary.

Only manifest, index and load are defined here: pushing and signing are not
wired up yet. The binary is built per image platform by the index, so the image
holds a linux binary whatever the host is.
"""

load("@rules_img//img:image.bzl", "image_index", "image_manifest")
load("@rules_img//img:layer.bzl", "file_metadata", "image_layer")
load("@rules_img//img:load.bzl", "image_load")

DEFAULT_PLATFORMS = [
    "//bazel/platforms:linux_amd64",
    "//bazel/platforms:linux_arm64",
]

def go_image(
        name,
        binary,
        path,
        base,
        load_tag,
        user = None,
        platforms = DEFAULT_PLATFORMS,
        visibility = ["//visibility:public"]):
    """Builds a multi-arch image around a pure-Go binary and a target to load it.

    Defines, for `name = "image"`:

      //pkg:image_layer     the binary, one layer, at `path`
      //pkg:image_manifest  one platform's manifest
      //pkg:image_index     the multi-platform index
      //pkg:image_load      loads the index into the local daemon as `load_tag`

    Args:
      name: prefix for the generated targets.
      binary: label of the go_binary (must be pure Go: no cgo).
      path: absolute path of the binary inside the image.
      base: digest-pinned base image.
      load_tag: the name the image gets in a local daemon.
      user: user the container runs as; None keeps the base image's user.
      platforms: platform labels, one manifest per platform.
      visibility: visibility of the index and load targets.
    """
    image_layer(
        name = name + "_layer",
        srcs = {path: binary},
        compress = "zstd",
        default_metadata = file_metadata(mode = "0755"),
        include_runfiles = False,
    )

    image_manifest(
        name = name + "_manifest",
        base = base,
        entrypoint = [path],
        layers = [name + "_layer"],
        user = user,
    )

    image_index(
        name = name + "_index",
        manifests = [name + "_manifest"],
        platforms = platforms,
        visibility = visibility,
    )

    image_load(
        name = name + "_load",
        image = name + "_index",
        tag = load_tag,
        visibility = visibility,
    )
