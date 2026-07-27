"""Buildkite dynamic pipeline generator for qdb-nats-connector.

Loads step templates from `steps/*.yml`, substitutes `{placeholder}`
vars, overlays env, the Docker plugin, and qdb-artifacts options
(variant + git-ref) per platform. Produces a 7-step graph: one lint
step plus one docker step in parallel with five per-platform combined
steps; each combined step runs build, unit, integration, and e2e
scripts in sequence.

Usage:
    python3 pipeline.py [generate|check]
"""

from __future__ import annotations

import dataclasses
import sys
from pathlib import Path

from buildkite_sdk import CommandStep, Pipeline

# qdb-cicd-tools ships the qdb_pipeline library as a submodule.
# Insert the path before importing so the relative submodule is found
# regardless of the working directory the generator is called from.
sys.path.insert(0, str(Path(__file__).parent / "tools"))
from qdb_pipeline import (
    Platform,
    apply_docker,
    get_git_ref,
    load_template,
    merge_env,
    select_platforms,
    set_artifact_plugin_options,
    validate_pipeline,
)  # noqa: E402

STEPS_DIR = Path(__file__).parent / "steps"

# Pin QuasarDB artifact downloads to a specific remote ref.
# When set, the qdb-artifacts download step pulls artifacts built from
# this ref instead of the connector's own branch.  Set to None to
# follow the connector branch (the default on master / 3.15.x).
QDB_DOWNLOAD_REF = "refs/heads/3.14.x"

# Connector-specific Platform overlays. Linux platforms run inside the
# rhel7 builder container; other OSes run on bare agents (docker_image="").
# No toolchain fields are set -- Go does not need c_compiler / cxx_compiler /
# asm_compiler / ccache.
_LINUX = dict(docker_image="bureau14/builder:rhel7")
_OS_OVERLAY: dict[str, dict] = {"linux": _LINUX}

# 5-platform matrix: haswell (AVX2) baseline for all amd64 targets.
# core2 is omitted because Go's GOAMD64 distinction is not a meaningful
# performance lever here; the linked libqdb_api is built for haswell anyway.
PLATFORMS: list[Platform] = [
    dataclasses.replace(p, **_OS_OVERLAY.get(p.os, {}))
    for p in select_platforms(
        "linux-amd64-haswell",
        "linux-aarch64",
        "windows-amd64-haswell",
        "freebsd-amd64-haswell",
        "macos-aarch64",
    )
]

# Environment variable layering: global -> step -> os -> os+step -> cpu.
# Empty dicts are kept on purpose so future env knobs land in the right
# slot without refactoring the merge call.
GLOBAL_ENV: dict[str, str] = {
    "AWS_DEFAULT_REGION": "eu-west-1",
}

STEP_ENV: dict[str, dict[str, str]] = {}

# Linux builds run inside bureau14/builder:rhel7, whose default /usr/bin/gcc
# is 7.3.1 -- too old to provide the aarch64 outline-atomic libgcc helpers
# (__aarch64_*_sync) that Go 1.25+ emits from its prebuilt -race (TSan)
# runtime, so linking the race-enabled test binaries fails on linux-aarch64.
# Pin CC/CXX to gcc15 (the compiler all QuasarDB artifacts are built with;
# its libgcc has the helpers) for both linux arches -- amd64 never references
# those symbols, so this is a harmless consistency win there.  The doubled $$
# escapes Buildkite's upload-time interpolation so the literal
# $QDB_CICD_AGENT_GCC15_* reaches the agent shell, which substitutes the
# concrete path (written by qdb-cloud-deployments 36-write-agent-env.sh);
# the docker plugin then propagates the resolved CC/CXX into the container.
# Same idiom as _go_env_for_agent() below.
OS_ENV: dict[str, dict[str, str]] = {
    "linux": {
        "CC": "$$QDB_CICD_AGENT_GCC15_CC",
        "CXX": "$$QDB_CICD_AGENT_GCC15_CXX",
    },
}

OS_STEP_ENV: dict[str, dict[str, str]] = {}

# GOAMD64=v3 (AVX2/haswell) is set for all amd64 platforms.
# aarch64 platforms have no entry and receive no GOAMD64 override.
CPU_ENV: dict[str, dict[str, str]] = {
    "haswell": {"GOAMD64": "v3"},
}

# Go version slug used to form the per-OS agent env var names.
# The slug is the major+minor with no dot: 1.25 -> "125".
# Changing this constant is the single point of control for the Go version.
GO_VERSION_SLUG = "125"  # Go 1.25


def _go_env_for_agent() -> dict[str, str]:
    """Return the GOROOT / GOPATH env vars for the current Go version.

    The Buildkite agent shell substitutes $QDB_CICD_AGENT_GO<slug>_ROOT and
    $QDB_CICD_AGENT_GO<slug>_PATH at job-start time; these vars are written by
    the per-OS packer scripts in qdb-cloud-deployments (e.g.
    agents/debian/scripts/36-write-agent-env.sh).  The doubled $$ is required
    to escape Buildkite's own variable interpolation during pipeline upload so
    that the literal string "$QDB_CICD_AGENT_GO124_ROOT" reaches the agent
    shell rather than being expanded (to an empty string) by the Buildkite
    server.  This is the same idiom qdb-api-go/.buildkite/pipeline.py uses.

    macOS divergence (deferred): the macOS agent exports GO1_23 pointing at the
    binary, not a ROOT/PATH pair.  The connector pipeline emits the canonical
    _ROOT/_PATH names on macOS too; the step will fail until qdb-cloud-deployments
    adds the canonical pair to the macOS agent environment hook.
    """
    return {
        "GOROOT": f"$$QDB_CICD_AGENT_GO{GO_VERSION_SLUG}_ROOT",
        "GOPATH": f"$$QDB_CICD_AGENT_GO{GO_VERSION_SLUG}_PATH",
    }


def _env(p: Platform, step_name: str) -> dict[str, str]:
    """Compose the full environment dict for one step.

    Layers global, per-step, per-os, per-(os, step), and per-cpu env on
    top of the platform overlay applied last by `merge_env`. The connector
    has no Release/Debug axis, so there is no `build_type` parameter.
    """
    return merge_env(
        GLOBAL_ENV,
        STEP_ENV.get(step_name, {}),
        OS_ENV.get(p.os, {}),
        OS_STEP_ENV.get(f"{p.os}/{step_name}", {}),
        CPU_ENV.get(p.cpu, {}),
        platform=p,
    )


def _lint_step() -> dict:
    """Build the standalone lint step.

    Layers the global env and injects the Go-toolchain variables from
    _go_env_for_agent() so that cicd_setup_go_toolchain in 10.lint.sh
    finds GOROOT and GOPATH at job-start time.  The step is then wrapped
    with apply_docker(bureau14/builder:rhel7) so that the propagated env
    and the qdb/ volume (populated by the qdb-artifacts plugin declared in
    _lint.yml) are both visible inside the container -- the same container
    the build steps use.  Only one plugin (qdb-artifacts) is declared in
    _lint.yml; the docker plugin is injected here.  Lint runs in parallel
    with the per-platform combined steps.  Variant + git-ref for the
    qdb-artifacts download block are injected later in generate_pipeline().
    """
    step = load_template(STEPS_DIR / "_lint.yml")
    # Compose env: global baseline, then template overrides, then Go toolchain.
    # The Go env is last so the version slug in pipeline.py is authoritative.
    env = merge_env(GLOBAL_ENV, STEP_ENV.get("lint", {}))
    env.update(step.get("env") or {})
    env.update(_go_env_for_agent())
    step["env"] = env
    # Run inside rhel7 so CGO headers from qdb/ and the propagated env reach
    # golangci-lint -- same container as the build steps.
    apply_docker(step, "bureau14/builder:rhel7")
    return step


def _docker_step() -> dict:
    """Build the standalone docker-image step.

    Loads the `_docker.yml` template, layers the global env on top of any
    template-declared env using the canonical merge idiom, and returns
    the step dict.  Unlike `_lint_step` and `_per_platform_step` this
    function does NOT call `apply_docker(...)` -- the step runs on the
    bare host of the default-debian-amd64 agent so the host's docker
    daemon is available for `docker build` / `docker run` invocations
    inside `scripts/cicd/60.docker.sh`.

    Unlike the lint and per-platform steps this function does NOT inject
    `qdb-artifacts` plugin options either -- the Dockerfile downloads
    `libqdb_api` directly from `download.quasar.ai` (temporary
    scaffolding; will be superseded once QuasarDB 3.14.3 ships an
    official public artifact for the connector).

    On master builds the step's script pushes the `:latest` tag to
    Docker Hub; the branch gate and the SSM credential fetch both live
    in `scripts/cicd/60.docker.sh`, not here.
    """
    step = load_template(STEPS_DIR / "_docker.yml")
    env = merge_env(GLOBAL_ENV, STEP_ENV.get("docker", {}))
    env.update(step.get("env") or {})
    step["env"] = env
    return step


def _per_platform_step(p: Platform) -> dict:
    """Generate the combined per-platform step (build + unit +
    integration + e2e) for one platform.

    The template declares the four bash invocations in its
    `commands:` list; this function handles env composition, docker
    overlay, and template-var substitution.  The Go-toolchain env
    (GOROOT, GOPATH) is injected via _go_env_for_agent() so that
    cicd_setup_go_toolchain in 20.build.sh and 30.test-unit.sh can
    derive the correct go binary without relying on PATH or make.
    The queue template var is `"{queue_os}-{arch}"` (no prefix);
    the template spells `default-{queue}`. `apply_docker` is a
    no-op when `p.docker_image` is empty (non-linux platforms) so
    the same call works uniformly across all OSes. Variant + git-ref
    for the qdb-artifacts download block are injected later in
    `generate_pipeline()`.
    """
    tvars = {
        "slug": p.slug(),
        "queue": (
            f"{p.queue_os}-{p.arch}"
            if p.os == "macos"
            else f"default-{p.queue_os}-{p.arch}"
        ),
    }

    step = load_template(STEPS_DIR / "_build.yml", **tvars)
    env = _env(p, "build")
    env.update(step.get("env") or {})
    # Go env last so the version slug in pipeline.py is authoritative.
    env.update(_go_env_for_agent())
    step["env"] = env
    apply_docker(step, p.docker_image, p.docker_volumes)
    return step


def generate_pipeline() -> Pipeline:
    """Assemble the full pipeline and return it.

    Resulting graph (7 steps total, all running in parallel):
        lint (1)
        docker (1)
        build-{slug} x5   (each running build + unit + integration + e2e
                           in sequence, then uploading the archive and
                           promoting the LATEST_SUCCESSFUL pointer)

    The docker step is a standalone bare-host build of the
    bureau14/qdb-nats-connector image; it runs in parallel with lint
    and the per-platform combined steps and has no depends_on.

    Each per-platform step's qdb-artifacts plugin entry receives the same
    ``variant`` + ``git-ref`` defaults injected into its ``download``,
    ``upload``, and ``promote`` blocks; ``files`` for upload stays
    declared in the step template ``.buildkite/steps/_build.yml`` so the
    file pattern lives next to the step it belongs to.
    """
    git_ref = get_git_ref()
    download_ref = QDB_DOWNLOAD_REF or git_ref
    pipeline = Pipeline()

    lint = _lint_step()
    set_artifact_plugin_options(
        lint,
        {"download": {"variant": "linux-haswell-release", "git-ref": download_ref}},
    )
    pipeline.add_step(CommandStep.from_dict(lint))

    # Standalone docker step: builds the bureau14/qdb-nats-connector image
    # from the repo-root Dockerfile and runs a `--version` smoke test.
    # No depends_on, no qdb-artifacts (Dockerfile is self-contained),
    # no docker plugin wrapper (runs on the bare host).
    pipeline.add_step(CommandStep.from_dict(_docker_step()))

    variants = []
    for p in PLATFORMS:
        step = _per_platform_step(p)
        variant = p.slug("release")
        variants.append(p.slug())
        set_artifact_plugin_options(
            step,
            {
                "download": {"variant": variant, "git-ref": download_ref},
                "upload": {"variant": variant, "git-ref": git_ref},
                "promote": {"variant": variant, "git-ref": git_ref},
            },
        )
        pipeline.add_step(CommandStep.from_dict(step))

    step = load_template(STEPS_DIR / "_test_report.yml")
    step["depends_on"] = [f"build-{variant}" for variant in variants]
    pipeline.add_step(CommandStep.from_dict(step))

    return pipeline


def main() -> None:
    """Entry point.

    Commands:
        generate  -- emit the pipeline YAML to stdout (default).
        check     -- validate pipeline structure; print errors or [OK] summary.
    """
    command = sys.argv[1] if len(sys.argv) > 1 else "generate"

    try:
        pipeline = generate_pipeline()
    except Exception as e:
        print(f"[FAIL] Pipeline generation failed: {e}", file=sys.stderr)
        sys.exit(1)

    if command == "generate":
        print(pipeline.to_yaml())
    elif command == "check":
        errors = validate_pipeline(pipeline)
        if errors:
            for e in errors:
                print(f"[FAIL] {e}", file=sys.stderr)
            sys.exit(1)
        print(f"[OK] Pipeline valid: {len(pipeline.steps)} steps")
    else:
        print(f"Unknown command: {command}", file=sys.stderr)
        print("Usage: pipeline.py [generate|check]", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
