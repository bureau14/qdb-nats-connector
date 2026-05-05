"""Buildkite dynamic pipeline generator for qdb-nats-connector.

Loads step templates from `steps/*.yml`, substitutes `{placeholder}` vars,
overlays env and the Docker plugin per platform. Produces a 33-step graph:
one lint gate, then per-platform build/unit/integration-stub/e2e-stub chains.

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
    load_template,
    merge_env,
    select_platforms,
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
    """Build the single lint step that gates the whole pipeline.

    The step's two plugins -- `bureau14/qdb-artifacts` (CGO header
    provisioning) and `golangci-lint` (lint execution in its own container)
    -- are declared in `_lint.yml`; no docker wrapper is applied here.
    The step pins to `default-debian-amd64` via the template.
    """
    step = load_template(STEPS_DIR / "_lint.yml")
    return step


def _per_platform_steps(p: Platform) -> list[dict]:
    """Generate the four-step chain (build, unit, integration-stub, e2e-stub)
    for one platform.

    Each template declares its own `depends_on`. The queue template var is
    `"{queue_os}-{arch}"` (no prefix) -- templates spell `default-{queue}`.
    Per-step env is composed via `_env(p, step_name)`; template env wins
    over computed env via the canonical `env.update + assign` idiom.
    `apply_docker` is a no-op when `p.docker_image` is empty (non-linux
    platforms), so the same call works uniformly across all OSes.
    """
    tvars = {"slug": p.slug(), "queue": f"{p.queue_os}-{p.arch}"}
    steps: list[dict] = []
    for template_name, step_name in (
        ("_build.yml", "build"),
        ("_test_unit.yml", "test-unit"),
        ("_test_integration.yml", "test-integration"),
        ("_test_e2e.yml", "test-e2e"),
    ):
        step = load_template(STEPS_DIR / template_name, **tvars)
        env = _env(p, step_name)
        env.update(step.get("env") or {})
        step["env"] = env
        apply_docker(step, p.docker_image, p.docker_volumes)
        steps.append(step)
    return steps


def generate_pipeline() -> Pipeline:
    """Assemble the full pipeline and return it.

    Resulting graph (33 steps total):
        lint (1)
            -> build-{slug} x8
                -> test-unit-{slug} x8
                    -> test-integration-{slug} x8
                        -> test-e2e-{slug} x8
    """
    pipeline = Pipeline()
    pipeline.add_step(CommandStep.from_dict(_lint_step()))
    for p in PLATFORMS:
        for step in _per_platform_steps(p):
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
