//! Shared REST catalog connection setup — used by both the Bronze poll loop
//! (`main.rs`) and the Gold refresh loop (`bin/gold.rs`), so the same
//! `ICEBRIDGE_CATALOG_*`/`ICEBRIDGE_S3_*`/`ICEBRIDGE_STORAGE` env vars work
//! identically for both.

use std::collections::HashMap;
use std::sync::Arc;

use iceberg::CatalogBuilder;
use iceberg_catalog_rest::{RestCatalog, RestCatalogBuilder, REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE};
use iceberg_storage_opendal::OpenDalStorageFactory;

pub fn env_var(name: &str) -> anyhow::Result<String> {
    std::env::var(name).map_err(|_| anyhow::anyhow!("missing required env var {name}"))
}

pub fn env_var_opt(name: &str) -> Option<String> {
    std::env::var(name).ok()
}

/// Connects to the REST catalog configured via `ICEBRIDGE_CATALOG_URI`
/// (required), `ICEBRIDGE_CATALOG_WAREHOUSE`/`ICEBRIDGE_CATALOG_TOKEN`
/// (optional), and `ICEBRIDGE_S3_*` (forwarded to the storage layer).
/// `ICEBRIDGE_STORAGE=fs` selects the local-filesystem storage factory for
/// dev/verification; anything else (including unset) selects S3.
pub async fn connect(client_name: &str) -> anyhow::Result<RestCatalog> {
    let mut catalog_props = HashMap::new();
    catalog_props.insert(REST_CATALOG_PROP_URI.to_string(), env_var("ICEBRIDGE_CATALOG_URI")?);
    if let Some(warehouse) = env_var_opt("ICEBRIDGE_CATALOG_WAREHOUSE") {
        catalog_props.insert(REST_CATALOG_PROP_WAREHOUSE.to_string(), warehouse);
    }
    if let Some(token) = env_var_opt("ICEBRIDGE_CATALOG_TOKEN") {
        catalog_props.insert("token".to_string(), token);
    }
    for (env_name, prop_name) in [
        ("ICEBRIDGE_S3_ENDPOINT", "s3.endpoint"),
        ("ICEBRIDGE_S3_REGION", "s3.region"),
        ("ICEBRIDGE_S3_ACCESS_KEY_ID", "s3.access-key-id"),
        ("ICEBRIDGE_S3_SECRET_ACCESS_KEY", "s3.secret-access-key"),
    ] {
        if let Some(value) = env_var_opt(env_name) {
            catalog_props.insert(prop_name.to_string(), value);
        }
    }

    // "fs" is for local/dev verification against a filesystem warehouse; "s3"
    // (default) is the real deployment target (Supabase Storage's Iceberg REST
    // catalog + S3-compatible bucket).
    let storage_factory = match env_var_opt("ICEBRIDGE_STORAGE").as_deref() {
        Some("fs") => OpenDalStorageFactory::Fs,
        _ => OpenDalStorageFactory::S3 { customized_credential_load: None },
    };
    Ok(RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(storage_factory))
        .load(client_name, catalog_props)
        .await?)
}
