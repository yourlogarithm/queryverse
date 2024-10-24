use opentelemetry::{global, KeyValue};
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    logs::LoggerProvider,
    trace::{Config, TracerProvider},
    Resource,
};
use tracing_subscriber::{
    field::MakeExt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter,
};

#[cfg(feature = "redis")]
pub mod redis;

#[cfg(feature = "database")]
pub mod database;

fn get_resource(pkg: &str) -> Resource {
    Resource::new(vec![KeyValue::new(
        opentelemetry_semantic_conventions::resource::SERVICE_NAME,
        pkg.to_owned(),
    )])
}

fn init_tracer_provider(pkg: &str, endpoint: &str) -> TracerProvider {
    opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(
            opentelemetry_otlp::new_exporter()
                .tonic()
                .with_endpoint(endpoint),
        )
        .with_trace_config(Config::default().with_resource(get_resource(pkg)))
        .install_batch(opentelemetry_sdk::runtime::Tokio)
        .unwrap()
}

fn init_logs(pkg: &str, endpoint: &str) -> LoggerProvider {
    opentelemetry_otlp::new_pipeline()
        .logging()
        .with_resource(get_resource(pkg))
        .with_exporter(
            opentelemetry_otlp::new_exporter()
                .tonic()
                .with_endpoint(endpoint),
        )
        .install_batch(opentelemetry_sdk::runtime::Tokio)
        .unwrap()
}

pub async fn start(pkg: &str, f: std::pin::Pin<Box<dyn futures::Future<Output = ()>>>) {
    let otel_endpoint = std::env::var("OTEL_COLLECTOR").unwrap();

    let tracer_provider = init_tracer_provider(pkg, &otel_endpoint);
    global::set_tracer_provider(tracer_provider);

    let logger_provider = init_logs(pkg, &otel_endpoint);
    let layer = OpenTelemetryTracingBridge::new(&logger_provider);

    let filter = EnvFilter::from_default_env()
        .add_directive("hyper=error".parse().unwrap())
        .add_directive("tonic=error".parse().unwrap())
        .add_directive("reqwest=error".parse().unwrap());

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .without_time()
                .map_fmt_fields(|f| f.debug_alt()),
        )
        .with(filter)
        .with(layer)
        .init();

    f.await;

    global::shutdown_tracer_provider();
    logger_provider.shutdown().unwrap();
}
