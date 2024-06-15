use bson::Uuid;
use chrono::{DateTime, Utc};
use mongodm::f;
use mongodm::mongo::{
    bson::doc, error::Error as MongoError, options::ClientOptions, Client as MongoClient,
};
use mongodm::{sync_indexes, CollectionConfig, Index, IndexOption, Indexes, Model};
use qdrant_client::{
    client::QdrantClient,
    qdrant::{
        vectors_config::Config as QConfig, CreateCollection, Distance, VectorParams, VectorsConfig,
    },
};
use serde::{Deserialize, Serialize};

pub const DATABASE: &str = "crawler";

pub struct DocumentsCollConf;

impl CollectionConfig for DocumentsCollConf {
    fn collection_name() -> &'static str {
        "documents"
    }

    fn indexes() -> Indexes {
        Indexes::new()
            .with(Index::new(f!(url in Document)).with_option(IndexOption::Unique))
            .with(Index::new(f!(first in Document)))
            .with(Index::new(f!(last in Document)))
            .with(Index::new(f!(sha256 in Document)))
    }
}

#[derive(Serialize, Deserialize)]
pub struct Document {
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
    type CollConf = DocumentsCollConf;
}

impl Model for Document {
    type CollConf = DocumentsCollConf;
}

pub async fn init_mongo() -> Result<MongoClient, MongoError> {
    let client_options = ClientOptions::parse(env!("MONGO_URI")).await?;
    let client = MongoClient::with_options(client_options)?;
    let db = client.database(DATABASE);
    sync_indexes::<DocumentsCollConf>(&db).await?;
    Ok(client)
}

pub async fn init_qdrant() -> QdrantClient {
    let qdrant_client = QdrantClient::from_url(env!("QDRANT_URI")).build().unwrap();
    if !qdrant_client.collection_exists("documents").await.unwrap() {
        qdrant_client
            .create_collection(&CreateCollection {
                collection_name: "documents".to_string(),
                vectors_config: Some(VectorsConfig {
                    config: Some(QConfig::Params(VectorParams {
                        size: 384,
                        distance: Distance::Cosine.into(),
                        ..Default::default()
                    })),
                }),
                ..Default::default()
            })
            .await
            .unwrap();
    }
    qdrant_client
}
