ARG RUST_VERSION=1.78
ARG APP_NAME=crawler-service
FROM rust:${RUST_VERSION}-bookworm AS build
ARG APP_NAME
ENV LANGUAGE_PROCESSOR_API http://language-processor:8000
WORKDIR /app
COPY . .
RUN apt-get update && apt-get install -y protobuf-compiler && rm -rf /var/lib/apt/lists/*
RUN cargo build --locked --release -p ${APP_NAME} && cp ./target/release/${APP_NAME} /bin/server

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
COPY --from=build /bin/server /app/
EXPOSE 8000
CMD ["/app/server"]
