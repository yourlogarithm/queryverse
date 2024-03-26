ARG RUST_VERSION=1.77.0
ARG APP_NAME=crawler

FROM rust:${RUST_VERSION}-bookworm AS builder
ARG APP_NAME
WORKDIR /app
COPY . .
RUN \
    --mount=type=cache,target=/app/target/ \
    --mount=type=cache,target=/usr/local/cargo/registry/ \
    cargo build --release && \
    cp ./target/release/${APP_NAME} /

FROM debian:bookworm-slim AS final
ARG APP_NAME
RUN adduser \
    --disabled-password \
    --gecos "" \
    --home "/nonexistent" \
    --shell "/sbin/nologin" \
    --no-create-home \
    --uid "10001" \
    appuser
COPY --from=builder /${APP_NAME} /app
RUN chown appuser /app
USER appuser
ENTRYPOINT ["/app"]
EXPOSE 8080/tcp