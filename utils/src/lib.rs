use opentelemetry::{global, trace::TraceError, KeyValue};
use opentelemetry_sdk::{
    runtime,
    trace::{Config, TracerProvider},
    Resource,
};
use tracing_loki::url::Url;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

#[cfg(feature = "redis")]
pub mod redis;

#[cfg(feature = "database")]
pub mod database;

fn init_tracer_provider(pkg: &str) -> Result<TracerProvider, TraceError> {
    let resource = Resource::new(vec![KeyValue::new("service.name", pkg.to_owned())]);
    opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(opentelemetry_otlp::new_exporter().tonic())
        .with_trace_config(Config::default().with_resource(resource))
        .install_batch(runtime::Tokio)
}

pub async fn start(pkg: &str, f: std::pin::Pin<Box<dyn futures::Future<Output = ()>>>) {
    let tracer = init_tracer_provider(pkg).unwrap();
    global::set_tracer_provider(tracer);
    let loki_url = std::env::var("LOKI_URL").unwrap();
    let (layer, task) = tracing_loki::builder()
        .label("service", pkg)
        .unwrap()
        .extra_field("pid", format!("{}", std::process::id()))
        .unwrap()
        .build_url(Url::parse(&loki_url).unwrap())
        .unwrap();
    tokio::spawn(task);
    let filter = EnvFilter::from_default_env();
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .without_time()
                .with_filter(filter),
        )
        .with(layer)
        .init();
    f.await;
}
