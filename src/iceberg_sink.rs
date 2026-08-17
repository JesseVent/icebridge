//! Writes a `Diff` (see diff.rs) into an Iceberg table via a REST catalog —
//! Supabase Storage's hosted Iceberg REST catalog in the target deployment,
//! but any spec-compliant REST catalog works.
//!
//! ponytail: append-only log, not true row-level deletes. iceberg-rust 0.10.1's
//! `Transaction` only exposes `fast_append` (see transaction/mod.rs — no
//! delete/overwrite action yet), so a removed primary key is recorded as a new
//! row with `_deleted = true` rather than an equality-delete file. Downstream
//! reads should pick the latest `_ingested_at` per primary key and filter
//! `_deleted = false`. Move to real equality deletes once the crate exposes a
//! delete/rewrite transaction action.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, BinaryArray, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::{DataFileFormat, NestedField, PrimitiveType, Schema, Type};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{DefaultFileNameGenerator, DefaultLocationGenerator};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use parquet::file::properties::WriterProperties;
use rusqlite::types::Value as SqlValue;

use crate::diff::{ColumnInfo, Diff};

const DELETED_COLUMN: &str = "_deleted";
const INGESTED_AT_COLUMN: &str = "_ingested_at";

fn primitive_for(sqlite_type: &str) -> PrimitiveType {
    let upper = sqlite_type.to_ascii_uppercase();
    if upper.contains("INT") {
        PrimitiveType::Long
    } else if upper.contains("REAL") || upper.contains("FLOA") || upper.contains("DOUB") {
        PrimitiveType::Double
    } else if upper.contains("BLOB") {
        PrimitiveType::Binary
    } else {
        // TEXT and anything else (NUMERIC, etc.) — safe, lossless fallback.
        PrimitiveType::String
    }
}

/// Builds the Iceberg schema: source columns (primary key required, the rest
/// optional) plus the two synthetic append-log columns.
pub fn build_schema(columns: &[ColumnInfo]) -> anyhow::Result<Schema> {
    let mut fields = Vec::new();
    let mut id = 1;
    for column in columns {
        fields.push(Arc::new(NestedField::new(
            id,
            &column.name,
            Type::Primitive(primitive_for(&column.sqlite_type)),
            column.is_pk,
        )));
        id += 1;
    }
    fields.push(Arc::new(NestedField::new(
        id,
        DELETED_COLUMN,
        Type::Primitive(PrimitiveType::Boolean),
        true,
    )));
    id += 1;
    fields.push(Arc::new(NestedField::new(
        id,
        INGESTED_AT_COLUMN,
        Type::Primitive(PrimitiveType::Long),
        true,
    )));
    Ok(Schema::builder().with_schema_id(0).with_fields(fields).build()?)
}

async fn create_table_in(
    catalog: &dyn Catalog,
    namespace_ident: &NamespaceIdent,
    table_name: &str,
    schema: &Schema,
) -> anyhow::Result<Table> {
    if let Err(err) = catalog.create_namespace(namespace_ident, HashMap::new()).await
        && err.kind() != iceberg::ErrorKind::NamespaceAlreadyExists {
            return Err(err.into());
        }

    let creation = TableCreation::builder()
        .name(table_name.to_string())
        .schema(schema.clone())
        .build();
    Ok(catalog.create_table(namespace_ident, creation).await?)
}

/// Loads the table if it exists, else creates the namespace (ignoring
/// already-exists) and the table with `schema`. Bronze's append-only path —
/// the table's existing data (and schema) is never disturbed.
pub async fn ensure_table(
    catalog: &dyn Catalog,
    namespace: &str,
    table_name: &str,
    schema: &Schema,
) -> anyhow::Result<Table> {
    let namespace_ident = NamespaceIdent::new(namespace.to_string());
    let table_ident = TableIdent::new(namespace_ident.clone(), table_name.to_string());

    if catalog.table_exists(&table_ident).await? {
        return Ok(catalog.load_table(&table_ident).await?);
    }
    create_table_in(catalog, &namespace_ident, table_name, schema).await
}

/// Drops the table if it exists (ignoring not-found) and creates it fresh
/// with `schema`. For tables that are fully recomputed each cycle (a rollup,
/// not an incremental log) rather than appended to — recreating avoids
/// needing an overwrite/replace `Transaction` action, which `iceberg-rust`
/// does not yet expose (only `fast_append`).
pub async fn recreate_table(
    catalog: &dyn Catalog,
    namespace: &str,
    table_name: &str,
    schema: &Schema,
) -> anyhow::Result<Table> {
    let namespace_ident = NamespaceIdent::new(namespace.to_string());
    let table_ident = TableIdent::new(namespace_ident.clone(), table_name.to_string());

    if let Err(err) = catalog.drop_table(&table_ident).await
        && err.kind() != iceberg::ErrorKind::TableNotFound {
            return Err(err.into());
        }
    create_table_in(catalog, &namespace_ident, table_name, schema).await
}

fn value_to_i64(value: &SqlValue) -> Option<i64> {
    match value {
        SqlValue::Integer(i) => Some(*i),
        SqlValue::Real(f) => Some(*f as i64),
        _ => None,
    }
}

fn value_to_f64(value: &SqlValue) -> Option<f64> {
    match value {
        SqlValue::Real(f) => Some(*f),
        SqlValue::Integer(i) => Some(*i as f64),
        _ => None,
    }
}

fn value_to_string(value: &SqlValue) -> Option<String> {
    match value {
        SqlValue::Text(s) => Some(s.clone()),
        SqlValue::Integer(i) => Some(i.to_string()),
        SqlValue::Real(f) => Some(f.to_string()),
        _ => None,
    }
}

fn value_to_bytes(value: &SqlValue) -> Option<Vec<u8>> {
    match value {
        SqlValue::Blob(b) => Some(b.clone()),
        _ => None,
    }
}

/// Builds one Arrow RecordBatch: `upserted` rows as-is (`_deleted = false`)
/// plus one synthetic row per `deleted_pks` entry (`_deleted = true`, all
/// non-key columns null).
fn build_record_batch(
    diff: &Diff,
    ingested_at_ms: i64,
    arrow_schema: arrow_schema::SchemaRef,
) -> anyhow::Result<RecordBatch> {
    let row_count = diff.upserted.len() + diff.deleted_pks.len();
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(diff.columns.len() + 2);

    for (col_index, column) in diff.columns.iter().enumerate() {
        let is_pk = col_index == diff.pk_index;
        let values_for_row = |row_index: usize| -> Option<SqlValue> {
            if row_index < diff.upserted.len() {
                Some(diff.upserted[row_index][col_index].clone())
            } else if is_pk {
                Some(diff.deleted_pks[row_index - diff.upserted.len()].clone())
            } else {
                None
            }
        };
        let primitive = primitive_for(&column.sqlite_type);
        let array: ArrayRef = match primitive {
            PrimitiveType::Long => Arc::new(Int64Array::from(
                (0..row_count).map(|i| values_for_row(i).and_then(|v| value_to_i64(&v))).collect::<Vec<_>>(),
            )),
            PrimitiveType::Double => Arc::new(Float64Array::from(
                (0..row_count).map(|i| values_for_row(i).and_then(|v| value_to_f64(&v))).collect::<Vec<_>>(),
            )),
            PrimitiveType::Binary => Arc::new(BinaryArray::from_opt_vec(
                (0..row_count).map(|i| values_for_row(i).and_then(|v| value_to_bytes(&v))).collect::<Vec<_>>()
                    .iter().map(|v: &Option<Vec<u8>>| v.as_deref()).collect::<Vec<_>>(),
            )),
            _ => Arc::new(StringArray::from(
                (0..row_count).map(|i| values_for_row(i).and_then(|v| value_to_string(&v))).collect::<Vec<_>>(),
            )),
        };
        arrays.push(array);
    }

    let deleted_flags: Vec<bool> = (0..row_count).map(|i| i >= diff.upserted.len()).collect();
    arrays.push(Arc::new(BooleanArray::from(deleted_flags)));
    arrays.push(Arc::new(Int64Array::from(vec![ingested_at_ms; row_count])));

    Ok(RecordBatch::try_new(arrow_schema, arrays)?)
}

/// Writes the diff as one Parquet data file and commits a fast-append
/// transaction. Returns the updated table (carry it into the next poll cycle).
pub async fn write_diff(
    catalog: &dyn Catalog,
    table: Table,
    diff: &Diff,
    ingested_at_ms: i64,
) -> anyhow::Result<Table> {
    if diff.upserted.is_empty() && diff.deleted_pks.is_empty() {
        return Ok(table);
    }
    let timing = std::env::var("ICEBRIDGE_TIMING").is_ok();
    let t0 = std::time::Instant::now();

    let arrow_schema = Arc::new(schema_to_arrow_schema(table.metadata().current_schema())?);
    let batch = build_record_batch(diff, ingested_at_ms, arrow_schema)?;
    let t1 = std::time::Instant::now();

    let result = append_batch(catalog, table, batch, &format!("icebridge-{ingested_at_ms}")).await?;

    if timing {
        eprintln!(
            "  write: build_batch_ms={} append_ms={}",
            t1.duration_since(t0).as_millis(),
            t1.elapsed().as_millis(),
        );
    }
    Ok(result)
}

/// Writes `batch` as one Parquet data file and commits a fast-append
/// transaction against `table`. `file_prefix` must be unique per call —
/// `DefaultFileNameGenerator`'s internal counter restarts at 0 for every new
/// instance, so a constant prefix collides across cycles that reuse the same
/// generator lineage. Shared by the Bronze poll loop (`write_diff`, above)
/// and the Gold refresh loop (`bin/gold.rs`), which builds its own RecordBatch
/// from an aggregation query result.
pub async fn append_batch(
    catalog: &dyn Catalog,
    table: Table,
    batch: RecordBatch,
    file_prefix: &str,
) -> anyhow::Result<Table> {
    let location_generator = DefaultLocationGenerator::new(table.metadata())?;
    let file_name_generator =
        DefaultFileNameGenerator::new(file_prefix.to_string(), None, DataFileFormat::Parquet);
    let parquet_writer_builder = ParquetWriterBuilder::new(
        WriterProperties::default(),
        table.metadata().current_schema().clone(),
    );
    let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_writer_builder,
        table.file_io().clone(),
        location_generator,
        file_name_generator,
    );
    let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder);
    let mut writer = data_file_writer_builder.build(None).await?;
    writer.write(batch).await?;
    let data_files = writer.close().await?;

    let tx = Transaction::new(&table);
    let tx = tx.fast_append().add_data_files(data_files).apply(tx)?;
    Ok(tx.commit(catalog).await?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Array;

    #[test]
    fn primitive_for_maps_sqlite_affinities() {
        assert_eq!(primitive_for("INTEGER"), PrimitiveType::Long);
        assert_eq!(primitive_for("integer"), PrimitiveType::Long); // case-insensitive
        assert_eq!(primitive_for("REAL"), PrimitiveType::Double);
        assert_eq!(primitive_for("FLOAT"), PrimitiveType::Double);
        assert_eq!(primitive_for("DOUBLE"), PrimitiveType::Double);
        assert_eq!(primitive_for("BLOB"), PrimitiveType::Binary);
        assert_eq!(primitive_for("TEXT"), PrimitiveType::String);
        assert_eq!(primitive_for("NUMERIC"), PrimitiveType::String); // safe fallback
    }

    fn sample_columns() -> Vec<ColumnInfo> {
        vec![
            ColumnInfo { name: "id".to_string(), sqlite_type: "INTEGER".to_string(), is_pk: true },
            ColumnInfo { name: "payload".to_string(), sqlite_type: "TEXT".to_string(), is_pk: false },
        ]
    }

    #[test]
    fn build_schema_adds_the_two_synthetic_columns() {
        let schema = build_schema(&sample_columns()).unwrap();
        let fields = schema.as_struct().fields();

        assert_eq!(fields.len(), 4); // id, payload, _deleted, _ingested_at
        assert_eq!(fields[2].name, DELETED_COLUMN);
        assert_eq!(fields[3].name, INGESTED_AT_COLUMN);
        assert!(fields[0].required, "primary key column must be required");
        assert!(!fields[1].required, "non-pk source column must be optional");
        assert!(fields[2].required, "_deleted is always populated by build_record_batch, never absent");
        assert!(fields[3].required, "_ingested_at is always populated by build_record_batch, never absent");
    }

    #[test]
    fn build_record_batch_marks_upserts_and_deletes_correctly() {
        let columns = sample_columns();
        let diff = Diff {
            columns: columns.clone(),
            pk_index: 0,
            upserted: vec![
                vec![SqlValue::Integer(1), SqlValue::Text("a".to_string())],
                vec![SqlValue::Integer(2), SqlValue::Text("b".to_string())],
            ],
            deleted_pks: vec![SqlValue::Integer(3)],
        };
        let schema = build_schema(&columns).unwrap();
        let arrow_schema = Arc::new(schema_to_arrow_schema(&schema).unwrap());

        let batch = build_record_batch(&diff, 1_000, arrow_schema).unwrap();

        assert_eq!(batch.num_rows(), 3); // 2 upserts + 1 delete marker

        let deleted = batch.column_by_name(DELETED_COLUMN).unwrap().as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(deleted.value(0), false);
        assert_eq!(deleted.value(1), false);
        assert_eq!(deleted.value(2), true);

        let ids = batch.column_by_name("id").unwrap().as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(ids.value(2), 3, "delete marker row still carries the deleted primary key");

        let payload = batch.column_by_name("payload").unwrap().as_any().downcast_ref::<StringArray>().unwrap();
        assert!(payload.is_null(2), "non-pk columns on a delete marker row must be null");

        let ingested_at = batch.column_by_name(INGESTED_AT_COLUMN).unwrap().as_any().downcast_ref::<Int64Array>().unwrap();
        assert!((0..3).all(|i| ingested_at.value(i) == 1_000));
    }
}
