"""Buildkite dynamic pipeline generator for qdb-nats-connector.

Loads step templates from `steps/*.yml`, substitutes `{placeholder}`
vars, overlays env, the Docker plugin, and qdb-artifacts options
(variant + git-ref) per platform. Produces a 9-step graph: one
lint step in parallel with eight per-platform combined steps,
each combined step running build, unit, integration, and e2e
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

# Connector-specific Platform overlays. Linux platforms run inside the
# rhel7 builder container; other OSes run on bare agents (docker_image="").
# No toolchain fields are set -- Go does not need c_compiler / cxx_compiler /
# asm_compiler / ccache.
_LINUX = dict(docker_image="bureau14/builder:rhel7")
_OS_OVERLAY: dict[str, dict] = {"linux": _LINUX}

# Full 8-platform matrix matching the quasardb release cadence.
PLATFORMS: list[Platform] = [
    dataclasses.replace(p, **_OS_OVERLAY.get(p.os, {}))
    for p in select_platforms(
        "linux-amd64-core2",
        "linux-amd64-haswell",
        "linux-aarch64",
        "windows-amd64-core2",
        "windows-amd64-haswell",
        "freebsd-amd64-core2",
        "freebsd-amd64-haswell",
        "macos-aarch64",
    )
]

# Environment variable layering: global -> step -> os -> os+step -> cpu.
# Empty dicts are kept on purpose so future env knobs land in the right
# slot without refactoring the merge call.
GLOBAL_ENV: dict[str, str] = {}

STEP_ENV: dict[str, dict[str, str]] = {}

OS_ENV: dict[str, dict[str, str]] = {}

OS_STEP_ENV: dict[str, dict[str, str]] = {}

# GOAMD64 ties the binary to the instruction-set baseline for each CPU family.
# core2 -> v1 (SSE2 baseline); haswell -> v3 (AVX2).
# aarch64 platforms have no entry and receive no GOAMD64 override.
CPU_ENV: dict[str, dict[str, str]] = {
    "core2": {"GOAMD64": "v1"},
    "haswell": {"GOAMD64": "v3"},
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

    The step's two plugins -- `bureau14/qdb-artifacts` (CGO header
    provisioning) and `golangci-lint` (lint execution in its own
    container) -- are declared in `_lint.yml`. The step pins to
    `default-debian-amd64` via the template. Lint is no longer a
    gate: it runs in parallel with the per-platform combined
    steps. Variant + git-ref for the qdb-artifacts download block
    are injected later in `generate_pipeline()`.
    """
    step = load_template(STEPS_DIR / "_lint.yml")
    return step


def _per_platform_step(p: Platform) -> dict:
    """Generate the combined per-platform step (build + unit +
    integration + e2e) for one platform.

    The template declares the four bash invocations in its
    `commands:` list; this function only handles env composition,
    docker overlay, and template-var substitution. The queue
    template var is `"{queue_os}-{arch}"` (no prefix); the
    template spells `default-{queue}`. `apply_docker` is a no-op
    when `p.docker_image` is empty (non-linux platforms) so the
    same call works uniformly across all OSes. Variant + git-ref
    for the qdb-artifacts download block are injected later in
    `generate_pipeline()`.
    """
    tvars = {"slug": p.slug(), "queue": f"{p.queue_os}-{p.arch}"}
    step = load_template(STEPS_DIR / "_build.yml", **tvars)
    env = _env(p, "build")
    env.update(step.get("env") or {})
    step["env"] = env
    apply_docker(step, p.docker_image, p.docker_volumes)
    return step


def generate_pipeline() -> Pipeline:
    """Assemble the full pipeline and return it.

    Resulting graph (9 steps total, all running in parallel):
        lint (1)
        build-{slug} x8   (each running build + unit +
                            integration + e2e in sequence)
    """
    git_ref = get_git_ref()
    pipeline = Pipeline()

    lint = _lint_step()
    set_artifact_plugin_options(
        lint,
        {"download": {"variant": "linux-core2-release", "git-ref": git_ref}},
    )
    pipeline.add_step(CommandStep.from_dict(lint))

    for p in PLATFORMS:
        step = _per_platform_step(p)
        set_artifact_plugin_options(
            step,
            {"download": {"variant": p.slug("release"), "git-ref": git_ref}},
        )
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
