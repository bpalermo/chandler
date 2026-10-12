"""A multi-arch, digest-pinned OCI image for a Go binary.

Layer, manifest, index, load and push. The binary is built per image platform
by the index, so the image holds a linux binary whatever the host is. Nothing
about the commit is in the image itself, so an image whose content did not
change keeps its digest from commit to commit: the commit is on the push's
second tag and in the certificate of the image's signature.
"""

load("@bazel_skylib//rules:common_settings.bzl", "string_flag")
load("@rules_img//img:image.bzl", "image_index", "image_manifest")
load("@rules_img//img:layer.bzl", "file_metadata", "image_layer")
load("@rules_img//img:load.bzl", "image_load")
load("@rules_img//img:push.bzl", "image_push")

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
        registry,
        repository,
        user = None,
        default_tag = "dev",
        platforms = DEFAULT_PLATFORMS,
        visibility = ["//visibility:public"]):
    """Builds a multi-arch image around a pure-Go binary and a target to load it.

    Defines, for `name = "image"`:

      //pkg:image_layer     the binary, one layer, at `path`
      //pkg:image_manifest  one platform's manifest
      //pkg:image_index     the multi-platform index
      //pkg:image_load      loads the index into the local daemon as `load_tag`
      //pkg:image_tag       string_flag, the mutable tag (override with
                            --//pkg:image_tag=vX.Y.Z)
      //pkg:image_push      pushes the index under two tags

    Args:
      name: prefix for the generated targets.
      binary: label of the go_binary (must be pure Go: no cgo).
      path: absolute path of the binary inside the image.
      base: digest-pinned base image.
      load_tag: the name the image gets in a local daemon.
      registry: registry host: IMAGE_REGISTRY of //bazel:registry.bzl.
      repository: repository path within the registry: image_repository(...)
        of //bazel:registry.bzl.
      default_tag: value of the tag flag when nothing overrides it.
      user: user the container runs as; None keeps the base image's user.
      platforms: platform labels, one manifest per platform.
      visibility: visibility of the index and load targets.
    """
    string_flag(
        name = name + "_tag",
        build_setting_default = default_tag,
        visibility = visibility,
    )

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

    # Two tags deliberately. The first is mutable and is what a human pulls; the
    # second is immutable and answers "what did this commit publish?". Several
    # commits' tags name one digest when the image did not change between them.
    #
    # STABLE_, not the volatile GIT_COMMIT. A volatile status value is constant
    # metadata to Bazel, so an action embedding one is not invalidated when it
    # changes, and a cached push would republish the previous commit's tag.
    image_push(
        name = name + "_push",
        build_settings = {"tag": name + "_tag"},
        image = name + "_index",
        registry = registry,
        repository = repository,
        tag_list = [
            "{{.tag}}",
            "{{if .STABLE_GIT_COMMIT}}{{.tag}}-{{.STABLE_GIT_COMMIT}}{{end}}",
        ],
        visibility = visibility,
    )
