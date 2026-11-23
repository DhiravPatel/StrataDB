# StrataDB server image (spec §29.2).
# Build from the repository root:  docker build -f deployments/docker/Dockerfile -t stratadb:dev .
# NOTE: this Dockerfile has not yet been built in CI or on the development machine
# (Docker daemon unavailable there); treat it as unverified until the CI job runs it.
# Production deployments should pin both base images by digest.

FROM rust:1.95-slim-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --release --locked -p stratadb-server -p stratadb-cli \
 && strip target/release/strata-server target/release/strata

FROM debian:bookworm-slim
# Non-root user with a fixed UID/GID; no shell login.
RUN groupadd --system --gid 10001 stratadb \
 && useradd --system --uid 10001 --gid stratadb --home-dir /data --shell /usr/sbin/nologin stratadb \
 && mkdir -p /data /etc/stratadb && chown stratadb:stratadb /data && chmod 0700 /data
COPY --from=build /src/target/release/strata-server /usr/local/bin/strata-server
COPY --from=build /src/target/release/strata /usr/local/bin/strata
USER 10001:10001
VOLUME ["/data"]
EXPOSE 7070
ENV STRATADB_DATA_DIR=/data
# No secrets are baked in: tokens are created at runtime (`strata-server token create`),
# TLS material is mounted read-only into /etc/stratadb/tls.
HEALTHCHECK --interval=15s --timeout=5s --start-period=30s --retries=3 \
  CMD ["strata-server", "healthcheck", "--addr", "127.0.0.1:7070", "--tcp-only"]
ENTRYPOINT ["strata-server", "serve"]
CMD ["--listen", "0.0.0.0:7070", "--tls-cert", "/etc/stratadb/tls/cert.pem", "--tls-key", "/etc/stratadb/tls/key.pem"]
