ARG RUST_VERSION=1.78
ARG APP_NAME=messaging-service

FROM rust:${RUST_VERSION}-bookworm AS build
ARG APP_NAME
RUN apt-get update && apt-get install -y \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY . .
RUN \
    --mount=type=cache,target=/app/target/ \
    --mount=type=cache,target=/usr/local/cargo/registry/ \
    cargo build --release --locked --bin ${APP_NAME} && \
    cp target/release/${APP_NAME} /bin/app

FROM debian:bookworm-slim AS final
ARG APP_NAME
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
RUN adduser \
    --disabled-password \
    --gecos "" \
    --home "/nonexistent" \
    --shell "/sbin/nologin" \
    --no-create-home \
    --uid "10001" \
    appuser
WORKDIR /app
USER appuser
COPY --from=build /bin/app /app/server
EXPOSE 8000
CMD ["/app/server"]
