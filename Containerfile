FROM docker.io/library/rust:alpine as builder
WORKDIR /work
COPY . .
RUN \
    apk add musl-dev curl xz && \
    cargo update && \
    cargo build --target=x86_64-unknown-linux-musl --release && \
    xz -k -6 target/x86_64-unknown-linux-musl/release/fragtale && \
    xz -k -9 target/x86_64-unknown-linux-musl/release/fragtale-cli && \
    mv target/x86_64-unknown-linux-musl/release/fragtale.xz fragtale.xz && \
    mv target/x86_64-unknown-linux-musl/release/fragtale-cli.xz fragtale-cli.xz && \
    mkdir -p /work/data && \
    ./bin/extract-third-party-licenses.sh && \
    XZ_OPT='-9' tar cJf licenses.tar.xz licenses/

FROM ghcr.io/mydriatech/the-ground-up:1.0.0 as tgu

FROM scratch

LABEL org.opencontainers.image.source="https://github.com/mydriatech/fragtale.git"
LABEL org.opencontainers.image.description="Turn your planet scale DB into a trustworthy message queue (MQ) with event sourcing capabilities."
LABEL org.opencontainers.image.licenses="Apache-2.0 WITH FWM-Exception-1.0.0 AND ..."
LABEL org.opencontainers.image.vendor="MydriaTech AB"

COPY --from=tgu  --chown=10001:0 /licenses-tgu.tar.xz /licenses-tgu.tar.xz
COPY --from=tgu  --chown=10001:0 /the-ground-up /fragtale
COPY --from=tgu  --chown=10001:0 /the-ground-up /fragtale-cli
COPY --from=tgu  --chown=10001:0 /the-ground-up-bin /fragtale-bin
COPY --from=tgu  --chown=10001:0 /the-ground-up-bin /fragtale-cli-bin
COPY --from=builder --chown=10001:0 /work/fragtale.xz /fragtale.xz
COPY --from=builder --chown=10001:0 /work/fragtale-cli.xz /fragtale-cli.xz
COPY --from=builder --chown=10001:0 --chmod=770 /work/data /data
COPY --from=builder --chown=10001:0 --chmod=770 /work/licenses.tar.xz /licenses.tar.xz

WORKDIR /

USER 10001:0

EXPOSE 8081

#ENV LOG_LEVEL "DEBUG"
ENV PATH "/"

CMD ["/fragtale"]
