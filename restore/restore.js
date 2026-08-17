/**
 * icebridge restore step — a thin CLI wrapper around @jessevent/supalite-replica's
 * restoreReplica(). Not reimplementing Litestream restore/config-generation in Rust:
 * this is the one code path with a passing integration test against a real binary
 * (../../supalite-replica/tests/kill-and-restore.test.ts), so the Rust poll loop
 * shells out here instead of duplicating it. Plain JS — no TS toolchain needed for
 * a wrapper this thin.
 *
 * Usage: node restore.js '<RestoreOptions JSON>'
 * On success prints {"target": "..."} to stdout and exits 0.
 * On failure prints the error to stderr and exits 1.
 */
import { restoreReplica } from "../../supalite-replica/dist/index.js";

async function main() {
  const argJson = process.argv[2];
  if (argJson === undefined) {
    throw new Error("Usage: node restore.js '<RestoreOptions JSON>'");
  }
  const options = JSON.parse(argJson);
  const result = await restoreReplica(options);
  process.stdout.write(`${JSON.stringify({ target: result.target })}\n`);
}

main().catch((error) => {
  process.stderr.write(`${error instanceof Error ? error.stack : String(error)}\n`);
  process.exitCode = 1;
});
