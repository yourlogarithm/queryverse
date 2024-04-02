ARG RUST_VERSION=1.77.0
ARG APP_NAME=crawler
FROM rust:${RUST_VERSION}-bullseye AS build
ARG APP_NAME
WORKDIR /app
RUN --mount=type=bind,source=crawler/src,target=./src \
    --mount=type=bind,source=crawler/Cargo.toml,target=./Cargo.toml \
    --mount=type=bind,source=./Cargo.lock,target=./Cargo.lock \
    --mount=type=cache,target=/app/target/ \
    --mount=type=cache,target=/usr/local/cargo/registry/ \
    cargo build --locked --release && \
    cp ./target/release/$APP_NAME /bin/server

FROM debian:bullseye-slim AS final
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
ARG UID=10001
RUN adduser \
    --disabled-password \
    --gecos "" \
    --home "/nonexistent" \
    --shell "/sbin/nologin" \
    --no-create-home \
    --uid "${UID}" \
    appuser
WORKDIR /app
COPY crawler/crt/certificate.pem ./crt/certificate.pem
COPY crawler/crt/private.key ./crt/private.key
RUN chmod 644 ./crt/certificate.pem ./crt/private.key
COPY --from=build /bin/server /bin/
USER appuser
EXPOSE 8443
CMD ["/bin/server"]
