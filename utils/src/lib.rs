use opentelemetry::global;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use prometheus::{Encoder, TextEncoder};
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

#[cfg(feature = "redis")]
pub mod redis;

#[tracing::instrument]
pub async fn metrics_handler() -> String {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    String::from_utf8(buffer).unwrap()
}

pub async fn start(pkg: &str, f: std::pin::Pin<Box<dyn futures::Future<Output = ()>>>) {
    global::set_text_map_propagator(opentelemetry_jaeger_propagator::Propagator::new());
    let tracer = opentelemetry_jaeger::new_agent_pipeline()
        .with_endpoint(env!("JAEGER_HOST"))
        .with_service_name(pkg)
        .install_simple()
        .unwrap();
    let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
    let registry = prometheus::Registry::new();
    let exporter = opentelemetry_prometheus::exporter()
        .with_registry(registry.clone())
        .build()
        .unwrap();
    let provider = SdkMeterProvider::builder().with_reader(exporter).build();
    global::set_meter_provider(provider);
    let filter = EnvFilter::from_default_env();
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .without_time()
                .with_filter(filter),
        )
        .with(telemetry)
        .init();
    f.await;
    opentelemetry::global::shutdown_tracer_provider();
}
