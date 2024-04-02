use axum::{
    extract::Host,
    handler::HandlerWithoutStateExt,
    http::{StatusCode, Uri},
    response::Redirect,
    BoxError,
};
use axum_server::tls_rustls::RustlsConfig;
use tracing::{debug, warn};
use std::{net::SocketAddr, path::PathBuf};

#[derive(Clone, Copy)]
pub struct Ports {
    pub http: u16,
    pub https: u16,
}

async fn redirect_http_to_https(ports: Ports) {
    fn make_https(host: String, uri: Uri, ports: Ports) -> Result<Uri, BoxError> {
        let mut parts = uri.into_parts();

        parts.scheme = Some(axum::http::uri::Scheme::HTTPS);

        if parts.path_and_query.is_none() {
            parts.path_and_query = Some("/".parse().unwrap());
        }

        let https_host = host.replace(&ports.http.to_string(), &ports.https.to_string());
        parts.authority = Some(https_host.parse()?);

        Ok(Uri::from_parts(parts)?)
    }

    let redirect = move |Host(host): Host, uri: Uri| async move {
        match make_https(host, uri, ports) {
            Ok(uri) => {
                Ok(Redirect::permanent(&uri.to_string()))
            },
            Err(error) => {
                warn!(%error, "failed to convert URI to HTTPS");
                Err(StatusCode::BAD_REQUEST)
            }
        }
    };

    let addr = SocketAddr::from(([0, 0, 0, 0], ports.http));
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    debug!("Listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, redirect.into_make_service())
        .await
        .unwrap();
}

pub async fn tls_config(ports: Ports) -> RustlsConfig {
    let config = RustlsConfig::from_pem_file(
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("crt/certificate.pem"),
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("crt/private.key"),
    )
    .await
    .unwrap();
    tokio::spawn(redirect_http_to_https(ports));
    config
}
