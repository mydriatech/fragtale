FROM docker.io/library/rust:alpine as builder
WORKDIR /work
COPY . .
RUN \
    apk add musl-dev curl xz && \
    cargo update && \
    cargo build --target=x86_64-unknown-linux-musl --release && \
    xz -k -6 target/x86_64-unknown-linux-musl/release/fragtale && \
    mv target/x86_64-unknown-linux-musl/release/fragtale.xz app.xz && \
    mkdir -p /work/data
    #./bin/extract-third-party-licenses.sh && \
    #tar cJf licenses.tar.xz licenses/

FROM ghcr.io/mydriatech/the-ground-up:latest as runner

FROM scratch

LABEL org.opencontainers.image.source="https://github.com/mydriatech/fragtale.git"
LABEL org.opencontainers.image.description="...."
LABEL org.opencontainers.image.licenses="Apache-2.0 WITH FWM-Exception-1.0.0 AND ..."
LABEL org.opencontainers.image.vendor="MydriaTech AB"

COPY --from=builder --chown=10001:0 --chmod=770 /work/data /data
COPY --from=runner  --chown=10001:0 /the-ground-up /fragtale
COPY --from=runner  --chown=10001:0 /app /app
COPY --from=runner  --chown=10001:0 /licenses-the-ground-up.tar.xz /licenses-tgu.tar.xz
COPY --from=builder --chown=10001:0 /work/app.xz /app.xz
#COPY --from=builder --chown=10001:0 --chmod=770 /work/licenses.tar.xz /licenses.tar.xz
COPY --from=builder --chown=10001:0 /work/target/x86_64-unknown-linux-musl/release/fragtale-cli /fragtale-cli


WORKDIR /

USER 10001:0

EXPOSE 8081

#ENV LOG_LEVEL "DEBUG"
ENV PATH "/"

CMD ["/fragtale"]
