# syntax=docker/dockerfile:1

# ---------------------------------------------------------------------------
# Builder: complete release toolchain, never copied into the final image.
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim AS builder

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        libssl-dev \
        make \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

COPY Makefile ./
COPY include/ ./include/
COPY src/ ./src/

RUN make MODE=release all \
    && strip \
        build/release/bin/netmirror-node \
        build/release/bin/netmirror-gateway \
        build/release/bin/netmirror-cli

# ---------------------------------------------------------------------------
# Runtime: only libssl3 (provided by libssl3) and the three binaries.
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim AS runtime

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends libssl3 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 netmirror \
    && useradd --system --uid 10001 --gid 10001 \
        --home-dir /app --create-home --shell /usr/sbin/nologin netmirror

COPY --from=builder /src/build/release/bin/netmirror-node /usr/local/bin/
COPY --from=builder /src/build/release/bin/netmirror-gateway /usr/local/bin/
COPY --from=builder /src/build/release/bin/netmirror-cli /usr/local/bin/
COPY docker/entrypoint.sh /usr/local/bin/netmirror-entrypoint

RUN chmod 0755 \
        /usr/local/bin/netmirror-node \
        /usr/local/bin/netmirror-gateway \
        /usr/local/bin/netmirror-cli \
        /usr/local/bin/netmirror-entrypoint \
    && mkdir -p /app/data \
    && chown -R netmirror:netmirror /app

LABEL org.opencontainers.image.title="netmirror" \
      org.opencontainers.image.description="Replicated content-addressable object store in C" \
      org.opencontainers.image.licenses="MIT"

EXPOSE 9001 9002 9003 8080
STOPSIGNAL SIGTERM

ENTRYPOINT ["/usr/local/bin/netmirror-entrypoint"]
CMD ["netmirror-node", "--help"]
