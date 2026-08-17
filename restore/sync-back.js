/**
 * icebridge Gold sync-back step — writes a freshly computed Gold result set
 * into a table in the *live* operational SQLite database, through
 * @supabase/lite's connection layer (correct pragmas, same path the app's
 * own writes use) rather than a second unmanaged writer touching the file
 * directly. Mirrors restore.js's "thin Node wrapper, JSON in, JSON out"
 * shape. Plain JS — no TS toolchain needed for a wrapper this thin.
 *
 * The destination table must already exist (created by the app's own
 * migration) — this script never creates or alters tables, only replaces
 * their contents, so schema ownership stays with the app.
 *
 * Usage: node sync-back.js '<{"databaseFile","table","rows"} JSON>'
 * On success prints {"synced": N} to stdout and exits 0.
 * On failure prints the error to stderr and exits 1.
 */
import { createConnection } from "@supabase/lite/sqlite";

const IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/;

function quoteIdent(name) {
  if (!IDENTIFIER.test(name)) {
    throw new Error(`unsafe identifier: ${JSON.stringify(name)}`);
  }
  return `"${name}"`;
}

function quoteValue(value) {
  if (value === null || value === undefined) return "NULL";
  if (typeof value === "number" && Number.isFinite(value)) return String(value);
  if (typeof value === "boolean") return value ? "1" : "0";
  return `'${String(value).replace(/'/g, "''")}'`;
}

async function main() {
  const argJson = process.argv[2];
  if (argJson === undefined) {
    throw new Error("Usage: node sync-back.js '<{\"databaseFile\",\"table\",\"rows\"} JSON>'");
  }
  const { databaseFile, table, rows } = JSON.parse(argJson);
  const quotedTable = quoteIdent(table);

  const connection = await createConnection({ url: `file:${databaseFile}`, ddlDialect: "postgres" });
  try {
    await connection.exec("BEGIN");
    await connection.exec(`DELETE FROM ${quotedTable}`);
    if (rows.length > 0) {
      const columns = Object.keys(rows[0]);
      const quotedColumns = columns.map(quoteIdent).join(", ");
      const BATCH = 500;
      for (let start = 0; start < rows.length; start += BATCH) {
        const batch = rows.slice(start, start + BATCH);
        const values = batch
          .map((row) => `(${columns.map((c) => quoteValue(row[c])).join(", ")})`)
          .join(",");
        await connection.exec(`INSERT INTO ${quotedTable} (${quotedColumns}) VALUES ${values}`);
      }
    }
    await connection.exec("COMMIT");
  } catch (error) {
    await connection.exec("ROLLBACK").catch(() => undefined);
    throw error;
  } finally {
    await connection.close();
  }

  process.stdout.write(`${JSON.stringify({ synced: rows.length })}\n`);
}

main().catch((error) => {
  process.stderr.write(`${error instanceof Error ? error.stack : String(error)}\n`);
  process.exitCode = 1;
});
