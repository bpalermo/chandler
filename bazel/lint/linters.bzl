"""Linters that run as aspects, with `--config=lint`.

The Go analyzers are not here: nogo runs them as part of compiling (//:nogo).
Nor is buf's lint of the protos, which is a test beside them.
"""

load("@aspect_rules_lint//lint:lint_test.bzl", "lint_test")
load("@aspect_rules_lint//lint:shellcheck.bzl", "lint_shellcheck_aspect")

shellcheck = lint_shellcheck_aspect(
    binary = Label("@aspect_rules_lint//lint:shellcheck_bin"),
    config = Label("//:.shellcheckrc"),
)

shellcheck_test = lint_test(aspect = shellcheck)
