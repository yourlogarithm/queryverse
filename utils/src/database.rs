use bson::Uuid;
use chrono::{DateTime, Utc};
use mongodm::f;
use mongodm::mongo::{
    bson::doc, error::Error as MongoError, options::ClientOptions, Client as MongoClient,
};
use mongodm::{sync_indexes, CollectionConfig, Index, IndexOption, Indexes, Model};
use qdrant_client::qdrant::CreateCollectionBuilder;
use qdrant_client::{
    qdrant::{vectors_config::Config as QConfig, Distance, VectorParams, VectorsConfig},
    Qdrant,
};
use serde::{Deserialize, Serialize};

pub const DATABASE: &str = "crawler";

pub struct PagesCollConf;

impl CollectionConfig for PagesCollConf {
    fn collection_name() -> &'static str {
        "pages"
    }

    fn indexes() -> Indexes {
        Indexes::new()
            .with(Index::new(f!(url in Page)).with_option(IndexOption::Unique))
            .with(Index::new(f!(first in Page)))
            .with(Index::new(f!(last in Page)))
            .with(Index::new(f!(sha256 in Page)))
    }
}

#[derive(Serialize, Deserialize)]
pub struct Page {
    pub url: String,
    #[serde(with = "bson::serde_helpers::chrono_datetime_as_bson_datetime")]
    pub first: DateTime<Utc>,
    #[serde(with = "bson::serde_helpers::chrono_datetime_as_bson_datetime")]
    pub last: DateTime<Utc>,
    pub sha256: String,
    pub uuid: Uuid,
}

#[derive(Serialize, Deserialize)]
pub struct UuidProjection {
    pub uuid: Uuid,
}

impl Model for UuidProjection {
    type CollConf = PagesCollConf;
}

impl Model for Page {
    type CollConf = PagesCollConf;
}

pub async fn init_mongo(uri: &str) -> Result<MongoClient, MongoError> {
    tracing::info!("Initializing MongoDB client");
    let client_options = ClientOptions::parse(uri).await?;
    let client = MongoClient::with_options(client_options)?;
    let db = client.database(DATABASE);
    sync_indexes::<PagesCollConf>(&db).await?;
    Ok(client)
}

pub async fn init_qdrant(uri: &str) -> Qdrant {
    tracing::info!("Initializing Qdrant client");
    let qdrant_client = Qdrant::from_url(uri).build().unwrap();
    if !qdrant_client.collection_exists("pages").await.unwrap() {
        tracing::info!("Creating Qdrant collection");
        qdrant_client
            .create_collection(
                CreateCollectionBuilder::new("pages")
                    .vectors_config(VectorsConfig {
                        config: Some(QConfig::Params(VectorParams {
                            size: 384,
                            distance: Distance::Cosine.into(),
                            ..Default::default()
                        })),
                    })
                    .build(),
            )
            .await
            .unwrap();
    }
    qdrant_client
}
