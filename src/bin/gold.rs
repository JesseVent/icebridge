//! icebridge-gold — Day 2 of the LTAP round trip. Reads the Bronze Iceberg
//! table `icebridge`'s poll loop (`main.rs`) writes, runs an aggregation
//! query over it via DuckDB, lands the result as a freshly recomputed Gold
//! Iceberg table, and syncs that same result into a plain table in the
//! operational SQLite database for the app to read with normal SQL.
//!
//! Deliberately a separate binary with its own poll loop: aggregation is
//! heavier and runs on a coarser cadence than Bronze's tight CDC loop, and
//! fusing them would let a slow query stall ingestion.

use std::process::Stdio;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow_array::{ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use icebridge::catalog::{connect, env_var, env_var_opt};
use icebridge::iceberg_sink::{append_batch, recreate_table};
use serde_json::Value as Json;
use tokio::process::Command;

fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
}

/// Runs `query` against DuckDB with the `bronze` view pointed at the Bronze
/// table's current metadata location, and returns the parsed JSON rows.
/// `duckdb`, not a Rust query engine, for the same reason `restore.js` shells
/// out to Litestream: it already reads our Iceberg tables correctly, so this
/// is a subprocess call, not a query-engine integration.
async fn run_gold_query(bronze_metadata_location: &str, query: &str) -> anyhow::Result<Vec<Json>> {
    let mut sql = String::from("INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs;\n");
    if env_var_opt("ICEBRIDGE_STORAGE").as_deref() != Some("fs") {
        for (env_name, setting) in [
            ("ICEBRIDGE_S3_ENDPOINT", "s3_endpoint"),
            ("ICEBRIDGE_S3_REGION", "s3_region"),
            ("ICEBRIDGE_S3_ACCESS_KEY_ID", "s3_access_key_id"),
            ("ICEBRIDGE_S3_SECRET_ACCESS_KEY", "s3_secret_access_key"),
        ] {
            if let Some(value) = env_var_opt(env_name) {
                sql.push_str(&format!("SET {setting}='{}';\n", value.replace('\'', "''")));
            }
        }
    }
    sql.push_str(&format!(
        "CREATE OR REPLACE VIEW bronze AS SELECT * FROM iceberg_scan('{}');\n",
        bronze_metadata_location.replace('\'', "''")
    ));
    sql.push_str(query);

    let output = Command::new("duckdb")
        .arg("-json")
        .arg("-c")
        .arg(&sql)
        .stdin(Stdio::null())
        .output()
        .await?;
    if !output.status.success() {
        anyhow::bail!("duckdb query failed: {}", String::from_utf8_lossy(&output.stderr));
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let trimmed = stdout.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }
    Ok(serde_json::from_str(trimmed)?)
}

/// Iceberg field type for one column, inferred from the JSON values seen for
/// it across all rows (first non-null value decides; an all-null column
/// falls back to String — a safe, lossless default).
fn infer_primitive(rows: &[Json], column: &str) -> PrimitiveType {
    for row in rows {
        match row.get(column) {
            Some(Json::Number(n)) if n.is_i64() || n.is_u64() => return PrimitiveType::Long,
            Some(Json::Number(_)) => return PrimitiveType::Double,
            Some(Json::String(_)) => return PrimitiveType::String,
            Some(Json::Bool(_)) => return PrimitiveType::Boolean,
            _ => {}
        }
    }
    PrimitiveType::String
}

fn build_gold_schema(rows: &[Json], columns: &[String]) -> anyhow::Result<Schema> {
    let fields = columns
        .iter()
        .enumerate()
        .map(|(i, name)| {
            std::sync::Arc::new(NestedField::new(
                (i + 1) as i32,
                name,
                Type::Primitive(infer_primitive(rows, name)),
                false,
            ))
        })
        .collect::<Vec<_>>();
    Ok(Schema::builder().with_schema_id(0).with_fields(fields).build()?)
}

fn build_gold_batch(
    rows: &[Json],
    columns: &[String],
    arrow_schema: arrow_schema::SchemaRef,
) -> anyhow::Result<RecordBatch> {
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(columns.len());
    for (col_index, column) in columns.iter().enumerate() {
        let field_type = arrow_schema.field(col_index).data_type();
        let array: ArrayRef = match field_type {
            arrow_schema::DataType::Int64 => std::sync::Arc::new(Int64Array::from(
                rows.iter().map(|r| r.get(column).and_then(Json::as_i64)).collect::<Vec<_>>(),
            )),
            arrow_schema::DataType::Float64 => std::sync::Arc::new(Float64Array::from(
                rows.iter().map(|r| r.get(column).and_then(Json::as_f64)).collect::<Vec<_>>(),
            )),
            arrow_schema::DataType::Boolean => std::sync::Arc::new(BooleanArray::from(
                rows.iter().map(|r| r.get(column).and_then(Json::as_bool)).collect::<Vec<_>>(),
            )),
            _ => std::sync::Arc::new(StringArray::from(
                rows.iter()
                    .map(|r| match r.get(column) {
                        Some(Json::String(s)) => Some(s.clone()),
                        Some(other) if !other.is_null() => Some(other.to_string()),
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
            )),
        };
        arrays.push(array);
    }
    Ok(RecordBatch::try_new(arrow_schema, arrays)?)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let bronze_namespace = env_var_opt("ICEBRIDGE_NAMESPACE").unwrap_or_else(|| "default".to_string());
    let bronze_table = env_var("ICEBRIDGE_ICEBERG_TABLE")?;
    let gold_namespace = env_var_opt("ICEBRIDGE_GOLD_NAMESPACE").unwrap_or_else(|| bronze_namespace.clone());
    let gold_table_name = env_var("ICEBRIDGE_GOLD_ICEBERG_TABLE")?;
    let query = env_var("ICEBRIDGE_GOLD_QUERY")?;

    let sync_database_file = env_var("ICEBRIDGE_SYNC_DATABASE_FILE")?;
    let sync_table = env_var("ICEBRIDGE_SYNC_TABLE")?;
    let sync_script = env_var_opt("ICEBRIDGE_SYNC_SCRIPT")
        .unwrap_or_else(|| format!("{}/restore/sync-back.js", env!("CARGO_MANIFEST_DIR")));

    let poll_interval_ms: u64 = env_var_opt("ICEBRIDGE_GOLD_POLL_INTERVAL_MS")
        .and_then(|v| v.parse().ok())
        .unwrap_or(30_000);
    let max_cycles: Option<u64> = env_var_opt("ICEBRIDGE_GOLD_MAX_CYCLES").and_then(|v| v.parse().ok());
    let timing_enabled = env_var_opt("ICEBRIDGE_TIMING").is_some();

    let catalog = connect("icebridge-gold").await?;
    let bronze_ident = TableIdent::new(NamespaceIdent::new(bronze_namespace), bronze_table);

    let mut cycle: u64 = 0;
    loop {
        cycle += 1;
        let cycle_start = Instant::now();

        let bronze = catalog.load_table(&bronze_ident).await?;
        let metadata_location = bronze
            .metadata_location()
            .ok_or_else(|| anyhow::anyhow!("bronze table has no metadata location yet — has icebridge written to it?"))?;

        let query_start = Instant::now();
        let rows = run_gold_query(metadata_location, &query).await?;
        let query_ms = query_start.elapsed().as_millis();

        eprintln!("gold cycle {cycle}: {} rows", rows.len());

        let write_start = Instant::now();
        if !rows.is_empty() {
            let columns: Vec<String> = match &rows[0] {
                Json::Object(map) => map.keys().cloned().collect(),
                _ => anyhow::bail!("gold query must return rows of objects"),
            };
            let schema = build_gold_schema(&rows, &columns)?;
            let table = recreate_table(&catalog, &gold_namespace, &gold_table_name, &schema).await?;
            let arrow_schema = std::sync::Arc::new(iceberg::arrow::schema_to_arrow_schema(&schema)?);
            let batch = build_gold_batch(&rows, &columns, arrow_schema)?;
            append_batch(&catalog, table, batch, &format!("gold-{}", now_ms())).await?;

            let sync_rows: Vec<serde_json::Map<String, Json>> = rows
                .iter()
                .map(|r| match r {
                    Json::Object(map) => map.clone(),
                    _ => unreachable!(),
                })
                .collect();
            let sync_args = serde_json::json!({
                "databaseFile": sync_database_file,
                "table": sync_table,
                "rows": sync_rows,
            });
            let output = Command::new("node").arg(&sync_script).arg(sync_args.to_string()).output().await?;
            if !output.status.success() {
                anyhow::bail!("sync-back.js failed: {}", String::from_utf8_lossy(&output.stderr));
            }
        }
        let write_ms = write_start.elapsed().as_millis();

        if timing_enabled {
            eprintln!(
                "gold cycle {cycle} timing: query_ms={query_ms} write_ms={write_ms} total_ms={}",
                cycle_start.elapsed().as_millis()
            );
        }

        if max_cycles.is_some_and(|max| cycle >= max) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(poll_interval_ms)).await;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Array;
    use serde_json::json;

    #[test]
    fn infer_primitive_reads_the_first_non_null_value() {
        let rows = vec![json!({"n": null}), json!({"n": 5}), json!({"n": 6})];
        assert_eq!(infer_primitive(&rows, "n"), PrimitiveType::Long);

        let rows = vec![json!({"n": 1.5})];
        assert_eq!(infer_primitive(&rows, "n"), PrimitiveType::Double);

        let rows = vec![json!({"n": "cat_1"})];
        assert_eq!(infer_primitive(&rows, "n"), PrimitiveType::String);

        let rows = vec![json!({"n": true})];
        assert_eq!(infer_primitive(&rows, "n"), PrimitiveType::Boolean);

        // all-null column: safe fallback, not a panic.
        let rows = vec![json!({"n": null}), json!({"n": null})];
        assert_eq!(infer_primitive(&rows, "n"), PrimitiveType::String);

        // column absent from every row: same safe fallback.
        let rows = vec![json!({"other": 1})];
        assert_eq!(infer_primitive(&rows, "n"), PrimitiveType::String);
    }

    #[test]
    fn build_gold_schema_infers_one_field_per_column() {
        let rows = vec![json!({"category": "cat_1", "n": 5, "total": 12.5})];
        let columns = vec!["category".to_string(), "n".to_string(), "total".to_string()];

        let schema = build_gold_schema(&rows, &columns).unwrap();
        let fields = schema.as_struct().fields();

        assert_eq!(fields.len(), 3);
        assert_eq!(fields[0].name, "category");
        assert!(!fields[0].required, "gold schema fields are all optional — aggregation output shape isn't guaranteed row-complete");
    }

    #[test]
    fn build_gold_batch_round_trips_row_values() {
        let rows = vec![json!({"category": "cat_1", "n": 5}), json!({"category": "cat_2", "n": 3})];
        let columns = vec!["category".to_string(), "n".to_string()];
        let schema = build_gold_schema(&rows, &columns).unwrap();
        let arrow_schema = std::sync::Arc::new(iceberg::arrow::schema_to_arrow_schema(&schema).unwrap());

        let batch = build_gold_batch(&rows, &columns, arrow_schema).unwrap();

        assert_eq!(batch.num_rows(), 2);
        let n = batch.column_by_name("n").unwrap().as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(n.value(0), 5);
        assert_eq!(n.value(1), 3);
    }
}
