# Dockerfile for bureau14/qdb-nats-connector.
#
# Runtime-only image: wraps the statically linked Linux connector binary
# produced by the CI build (scripts/cicd/20.build.sh, linux-haswell) into
# a vanilla debian:trixie-slim base. Nothing is compiled or downloaded
# here -- no Go toolchain, no gcc, no C API -- so the image ships exactly
# the binary CI tested, and the build depends on no external source.
#
# Design principle: SIMPLE over EASY. Vanilla Debian base image, explicit
# apt packages, explicit CA bundle. Do NOT switch to a composite image as
# a "one-line simplification".
#
# Build context: a directory holding bin/qdb-nats-connector.
#   CI:    the qdb-artifacts plugin extracts this build's
#          *-linux-64bit-nats-connector.tar.zst into docker-context/
#          (see .buildkite/steps/_docker.yml) and scripts/cicd/60.docker.sh
#          builds with that directory as context.
#   Local: `make build` (on a Linux host with qdb/ populated) then
#          `docker build --build-arg VERSION=$(cat VERSION) -f Dockerfile .`
#          -- bin/qdb-nats-connector is the same relative path.
#
# Build-time ARGs (CI overrides via --build-arg):
#   VERSION  -- Connector version string, from the repo VERSION file; used
#               for the image label. The binary itself is already stamped
#               with version/commit/build time by 20.build.sh.
#
# glibc compat note: the CI binary is linked on rhel7 (glibc 2.17) with
# libqdb_api, libstdc++ and libgcc static, so any glibc-based runtime base
# newer than that works; trixie-slim is chosen for its currency.

FROM debian:trixie-slim AS runtime

ARG VERSION

LABEL maintainer="support@quasar.ai"
LABEL org.opencontainers.image.title="qdb-nats-connector"
LABEL org.opencontainers.image.version="${VERSION}"
LABEL org.opencontainers.image.description="NATS JetStream to QuasarDB connector"

# Runtime prerequisites:
#   ca-certificates -- for any HTTPS the connector may dial (NATS TLS).
# The connector's only shared-library dependencies are libc and libm,
# already present in debian:trixie-slim.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY bin/qdb-nats-connector /usr/local/bin/qdb-nats-connector

ENTRYPOINT ["/usr/local/bin/qdb-nats-connector"]
