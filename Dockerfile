# Dockerfile for bureau14/qdb-nats-connector.
#
# Multi-stage build that produces a Debian-trixie-based image carrying
# the connector binary plus libqdb_api.so.  Interim scaffolding -- will
# be superseded once QuasarDB 3.14.3 ships an official public artifact
# for this connector.
#
# Design principle: SIMPLE over EASY.  Both stages use vanilla Debian
# base images (debian:trixie / debian:trixie-slim) rather than the
# composite golang:* image so we retain full control over the stack --
# which Go version is installed, which apt packages are present, which
# CA bundle is shipped.  Do NOT switch the builder to a golang:* image
# as a "one-line simplification": that regresses the principle.
#
# Build-time ARGs (top-of-file defaults; CI overrides via --build-arg):
#   QDB_VERSION  -- QuasarDB ecosystem version (currently 3.14.2;
#                   bump in one place when 3.14.3 ships).
#   GO_VERSION   -- Go toolchain version, fetched from the QuasarDB CI
#                   build-deps mirror (the same bucket the Buildkite agents
#                   are provisioned from, see qdb-cloud-deployments
#                   cloud/aws/cicd/packer/buildkite/agents/common/include.sh);
#                   keep aligned with the `go` directive in go.mod and with
#                   the versions mirrored there.
#   GO_MIRROR    -- Base URL of that mirror; never fetch toolchains from
#                   go.dev or other third-party hosts, builds must not
#                   depend on external availability and must stay
#                   deterministic.
#   VERSION      -- Connector version string, from the repo VERSION
#                   file; injected into the binary via -ldflags.
#   GIT_SHA      -- Full 40-char git SHA of the source commit;
#                   injected into the binary via -ldflags.
#   BUILD_TIME   -- RFC3339 UTC timestamp of the build; injected via
#                   -ldflags.
#
# Runtime layout (mirrors host dev layout under qdb/lib):
#   /opt/qdb/lib/libqdb_api.so              -- dynamic library
#   /usr/local/bin/qdb-nats-connector       -- connector binary
# The CGO_LDFLAGS rpath baked at build time points at /opt/qdb/lib,
# so the loader finds libqdb_api.so without LD_LIBRARY_PATH or
# ldconfig.
#
# Scope: ships ONLY qdb-nats-connector.  qdb-data-gen and qdb-data-loader
# are internal dev tools and are not built or included.
#
# glibc compat note: a binary built on trixie (glibc 2.41) will not run
# on older Debian releases.  If you change the runtime stage base
# image, the builder base must match (or be older).

ARG QDB_VERSION=3.14.2
ARG GO_VERSION=1.25.10
ARG GO_MIRROR=https://qdb-cicd-builddeps-20260226074339625300000001.s3.eu-west-1.amazonaws.com/golang

# ---------------------------------------------------------------------
# Builder stage: vanilla debian:trixie + explicit Go + libqdb_api.
# ---------------------------------------------------------------------
FROM debian:trixie AS builder

ARG QDB_VERSION
ARG GO_VERSION
ARG GO_MIRROR
ARG VERSION
ARG GIT_SHA
ARG BUILD_TIME

# Explicit apt prerequisites:
#   ca-certificates -- HTTPS to go.dev for the Go tarball.
#   curl            -- fetch the Go tarball.
#   gcc, libc6-dev  -- cgo invokes the system C toolchain to compile
#                      the libqdb_api bindings.
#   tar, gzip       -- explicit even though they are usually present,
#                      so the dependency is documented.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        gcc \
        libc6-dev \
        tar \
        gzip \
    && rm -rf /var/lib/apt/lists/*

# Install Go from the CI build-deps mirror (the explicit, "simple" way --
# no apt drift, no dependency on go.dev being up or still serving a given
# release). Verifies via `go version` in the build log for diagnostic
# visibility.
RUN curl -fsSL "${GO_MIRROR}/go${GO_VERSION}.linux-amd64.tar.gz" \
        -o /tmp/go.tar.gz && \
    tar -C /usr/local -xzf /tmp/go.tar.gz && \
    rm /tmp/go.tar.gz
ENV PATH="/usr/local/go/bin:${PATH}"
RUN go version

# Download and extract libqdb_api.  Mirrors the qdb-docker
# `ADD http://download.quasar.ai/...` pattern; the URL is ARG-templated
# so the eventual 3.14.3 bump is a single edit.
#
# Post-extraction layout under /opt/qdb:
#   include/qdb/*.h, *.hpp
#   lib/libqdb_api.so
#   examples/  (unused at build/runtime; left in place for the builder
#               stage but NOT copied into the runtime stage)
ADD http://download.quasar.ai/quasardb/3.14/${QDB_VERSION}/api/c/qdb-${QDB_VERSION}-linux-64bit-c-api.tar.gz /tmp/qdb-c-api.tar.gz
RUN mkdir -p /opt/qdb && \
    tar -xzf /tmp/qdb-c-api.tar.gz -C /opt/qdb && \
    rm /tmp/qdb-c-api.tar.gz

WORKDIR /src
COPY . /src

# Build the connector binary.  Flag composition mirrors
# scripts/cicd/20.build.sh (BUILD_MODE=release path):
#   -trimpath              -- reproducible build paths.
#   -buildvcs=false        -- avoid git-stamping noise (commit SHA is
#                             injected via -ldflags below).
#   GOAMD64=v3             -- haswell baseline (matches CI default).
#   CGO_LDFLAGS rpath      -- bake the runtime path /opt/qdb/lib into
#                             the binary so the loader finds the .so.
# ONLY ./cmd/qdb-nats-connector is built; the data-gen / data-loader
# binaries are internal dev tools and are explicitly NOT shipped.
RUN CGO_ENABLED=1 \
    CGO_CFLAGS="-I/opt/qdb/include" \
    CGO_LDFLAGS="-L/opt/qdb/lib -Wl,-rpath -Wl,/opt/qdb/lib" \
    GOFLAGS="-trimpath" \
    GOAMD64="v3" \
        go build -buildvcs=false \
            -ldflags "-X main.version=${VERSION} \
                      -X main.commit=${GIT_SHA} \
                      -X main.buildTime=${BUILD_TIME} \
                      -X main.buildMode=release \
                      -X main.goamd64=v3 \
                      -X main.kernelVersion=docker" \
            -o /out/qdb-nats-connector ./cmd/qdb-nats-connector

# ---------------------------------------------------------------------
# Runtime stage: vanilla debian:trixie-slim + connector + libqdb_api.
# ---------------------------------------------------------------------
FROM debian:trixie-slim AS runtime

ARG QDB_VERSION
ARG VERSION

LABEL maintainer="support@quasar.ai"
LABEL org.opencontainers.image.title="qdb-nats-connector"
LABEL org.opencontainers.image.version="${VERSION}"
LABEL org.opencontainers.image.description="NATS JetStream to QuasarDB connector (built against libqdb_api ${QDB_VERSION})"

# Runtime prerequisites:
#   ca-certificates -- for any HTTPS the connector may dial.
# libqdb_api.so's transitive runtime deps (libstdc++6, libc6, libm6)
# are already in debian:trixie-slim.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /opt/qdb/lib/libqdb_api.so /opt/qdb/lib/libqdb_api.so
COPY --from=builder /out/qdb-nats-connector /usr/local/bin/qdb-nats-connector

ENTRYPOINT ["/usr/local/bin/qdb-nats-connector"]
