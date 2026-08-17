//! icebridge — poll loop that restores a Litestream replica (via the Node
//! `restore/restore.js` wrapper around `@jessevent/supalite-replica`'s
//! restoreReplica()), diffs it against the previous poll's snapshot, and
//! writes the delta into an Iceberg table through a REST catalog.
//!
//! First slice of the plan: prove tmpfs SQLite -> Litestream replica ->
//! Iceberg-on-S3 works end to end. See diff.rs / iceberg_sink.rs for the two
//! deliberate simplifications (snapshot-diff CDC, append-only delete log).

use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use icebridge::catalog::{env_var, env_var_opt};
use icebridge::{diff, iceberg_sink};
use tokio::process::Command;

fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
}

/// Shells out to restore.js (see its header for why this isn't reimplemented
/// in Rust) and returns the restored snapshot path.
async fn restore_snapshot(
    restore_script: &str,
    database_file: &str,
    replica_json: &serde_json::Value,
    target: &std::path::Path,
) -> anyhow::Result<PathBuf> {
    let options = serde_json::json!({
        "databaseFile": database_file,
        "replica": replica_json,
        "target": target.to_string_lossy(),
    });
    let output = Command::new("node")
        .arg(restore_script)
        .arg(options.to_string())
        .output()
        .await?;
    if !output.status.success() {
        anyhow::bail!(
            "restore.js failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let parsed: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    let target = parsed["target"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("restore.js did not report a target path"))?;
    Ok(PathBuf::from(target))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let database_file = env_var("ICEBRIDGE_DATABASE_FILE")?;
    let replica_json: serde_json::Value = serde_json::from_str(&env_var("ICEBRIDGE_REPLICA_JSON")?)?;
    let table_name = env_var("ICEBRIDGE_TABLE")?;
    let namespace = env_var_opt("ICEBRIDGE_NAMESPACE").unwrap_or_else(|| "default".to_string());
    let iceberg_table = env_var_opt("ICEBRIDGE_ICEBERG_TABLE").unwrap_or_else(|| table_name.clone());
    let poll_interval_ms: u64 = env_var_opt("ICEBRIDGE_POLL_INTERVAL_MS")
        .and_then(|v| v.parse().ok())
        .unwrap_or(5_000);
    let max_cycles: Option<u64> = env_var_opt("ICEBRIDGE_MAX_CYCLES").and_then(|v| v.parse().ok());
    let restore_script = env_var_opt("ICEBRIDGE_RESTORE_SCRIPT")
        .unwrap_or_else(|| format!("{}/restore/restore.js", env!("CARGO_MANIFEST_DIR")));
    let workdir = env_var_opt("ICEBRIDGE_WORKDIR").map(PathBuf::from).unwrap_or_else(std::env::temp_dir);

    let catalog = icebridge::catalog::connect("icebridge").await?;

    // Stable path, not a timestamped one: this is icebridge's persisted diff
    // cursor. Surviving a process restart (not just a poll-loop iteration) means
    // the "previous snapshot" has to be findable on disk by convention, not held
    // only in memory. Existing file at startup = resuming after a restart.
    let cursor_path = workdir.join("icebridge-previous.db");
    let mut previous_snapshot: Option<PathBuf> = cursor_path.exists().then(|| cursor_path.clone());
    let mut table: Option<iceberg::table::Table> = None;
    let mut cycle: u64 = 0;

    let timing_enabled = env_var_opt("ICEBRIDGE_TIMING").is_some();

    loop {
        cycle += 1;
        let cycle_start = Instant::now();

        let target = workdir.join(format!("icebridge-snap-{}.db", now_ms()));
        let restore_start = Instant::now();
        let snapshot = restore_snapshot(&restore_script, &database_file, &replica_json, &target).await?;
        let restore_ms = restore_start.elapsed().as_millis();

        let diff_start = Instant::now();
        let diff = diff::diff_snapshots(previous_snapshot.as_deref(), &snapshot, &table_name)?;
        let diff_ms = diff_start.elapsed().as_millis();
        eprintln!(
            "cycle {cycle}: {} upserted, {} deleted",
            diff.upserted.len(),
            diff.deleted_pks.len()
        );

        let write_start = Instant::now();
        let current_table = match table.take() {
            Some(t) => t,
            None => {
                let schema = iceberg_sink::build_schema(&diff.columns)?;
                iceberg_sink::ensure_table(&catalog, &namespace, &iceberg_table, &schema).await?
            }
        };
        table = Some(iceberg_sink::write_diff(&catalog, current_table, &diff, now_ms()).await?);
        let write_ms = write_start.elapsed().as_millis();

        // Advance the cursor only after a successful commit. A crash between
        // commit and this rename replays that cycle's rows on the next start —
        // safe because write_diff's log is append-only and idempotent under a
        // latest-_ingested_at-per-key read (see iceberg_sink.rs), not because
        // this is exactly-once.
        let rename_start = Instant::now();
        std::fs::rename(&snapshot, &cursor_path)?;
        let rename_ms = rename_start.elapsed().as_millis();
        previous_snapshot = Some(cursor_path.clone());

        if timing_enabled {
            eprintln!(
                "cycle {cycle} timing: restore_ms={restore_ms} diff_ms={diff_ms} write_ms={write_ms} rename_ms={rename_ms} total_ms={}",
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
