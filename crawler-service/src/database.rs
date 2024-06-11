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

pub struct DocumentsCollConf;

impl CollectionConfig for DocumentsCollConf {
    fn collection_name() -> &'static str {
        "documents"
    }

    fn indexes() -> Indexes {
        Indexes::new()
            .with(Index::new(f!(url in Document)).with_option(IndexOption::Unique))
            .with(Index::new(f!(date in Document)))
            .with(Index::new(f!(visited in Document)))
            .with(Index::new(f!(sha256 in Document)))
    }
}

#[derive(Serialize, Deserialize, PartialEq)]
pub struct Document {
    pub url: String,
    #[serde(with = "bson::serde_helpers::chrono_datetime_as_bson_datetime")]
    pub date: DateTime<Utc>,
    #[serde(with = "bson::serde_helpers::chrono_datetime_as_bson_datetime")]
    pub visited: DateTime<Utc>,
    pub sha256: String,
}

impl Model for Document {
    type CollConf = DocumentsCollConf;
}

pub async fn init_mongo() -> Result<MongoClient, MongoError> {
    let client_options = ClientOptions::parse(env!("MONGO_URI")).await?;
    let client = MongoClient::with_options(client_options)?;
    let db = client.database("documents");
    sync_indexes::<DocumentsCollConf>(&db).await?;
    Ok(client)
}

pub async fn init_qdrant() -> anyhow::Result<QdrantClient> {
    let qdrant_client = QdrantClient::from_url(env!("QDRANT_URI")).build()?;
    qdrant_client
        .create_collection(&CreateCollection {
            collection_name: "documents".to_string(),
            vectors_config: Some(VectorsConfig {
                config: Some(QConfig::Params(VectorParams {
                    size: 768,
                    distance: Distance::Cosine.into(),
                    ..Default::default()
                })),
            }),
            ..Default::default()
        })
        .await?;
    Ok(qdrant_client)
}
