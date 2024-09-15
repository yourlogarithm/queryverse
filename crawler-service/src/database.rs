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

pub async fn init_mongo(uri: &str) -> Result<MongoClient, MongoError> {
    tracing::info!("Initializing MongoDB client");
    let client_options = ClientOptions::parse(uri).await?;
    let client = MongoClient::with_options(client_options)?;
    let db = client.database(DATABASE);
    sync_indexes::<DocumentsCollConf>(&db).await?;
    Ok(client)
}

pub async fn init_qdrant(uri: &str) -> Qdrant {
    tracing::info!("Initializing Qdrant client");
    let qdrant_client = Qdrant::from_url(uri).build().unwrap();
    if !qdrant_client.collection_exists("documents").await.unwrap() {
        tracing::info!("Creating Qdrant collection");
        qdrant_client
            .create_collection(
                CreateCollectionBuilder::new("documents")
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
