"use strict";

/**
 * ShardDB performance benchmark (Node + mock Drive).
 *
 * Log-scale dataset sizes (default: 100, 1000, 10000). Optional 100000 via --full (slow).
 * Outputs: console table, benchmark-results.csv (includes master INDEX size: index_json_bytes,
 * key_to_fragment_count, routing_ok from validateRoutingConsistency), optional Mermaid xychart.
 *
 * Usage:
 *   node tests/sharddb-benchmark-performance.js
 *   node tests/sharddb-benchmark-performance.js --sizes=100,1000
 *   node tests/sharddb-benchmark-performance.js --full
 *   node tests/sharddb-benchmark-performance.js --csv --mermaid
 */

const fs = require("fs");
const path = require("path");
const { loadShardDbUmd } = require("./helpers/load-sharddb");
const { createMockDrive } = require("./helpers/mock-drive");

const SHARD_DB = loadShardDbUmd();
const INDEX_ID = "BENCH_INDEX";

function parseArgs() {
  const argv = process.argv.slice(2);
  const out = { sizes: null, csv: false, mermaid: false, full: false };
  for (const a of argv) {
    if (a === "--csv") out.csv = true;
    else if (a === "--mermaid") out.mermaid = true;
    else if (a === "--full") out.full = true;
    else if (a.startsWith("--sizes=")) {
      out.sizes = a
        .slice("--sizes=".length)
        .split(",")
        .map((s) => parseInt(s.trim(), 10))
        .filter((n) => n > 0);
    }
  }
  if (!out.sizes || out.sizes.length === 0) {
    out.sizes = out.full ? [100, 1000, 10000, 100000] : [100, 1000, 10000];
  }
  return out;
}

function timeMs(fn) {
  const t0 = Date.now();
  const ret = fn();
  return { ms: Date.now() - t0, ret: ret };
}

function runForSize(n) {
  const mock = createMockDrive({ dbDir: path.join(__dirname, ".mock_drive", "bench_" + n) });
  mock.wipe();

  const adapter = mock.adapter;
  const DB = SHARD_DB.init(INDEX_ID, adapter);
  const ctx = { dbMain: "USERS" };
  const row = (i) => ({
    key: "k_" + i,
    id: i,
    email: "u" + i + "@b.io",
    tag: i % 5,
    profile: {
      status: i % 2 === 0 ? "Active" : "Archived",
      metrics: [{ clearance: i > n / 2 ? "L2" : "L1", score: i % 100 }]
    }
  });

  const out = { size: n };

  out.seed_ms = timeMs(() => {
    for (let i = 1; i <= n; i++) DB.addToDB(row(i), ctx);
  }).ms;

  out.saveToDBFiles_ms = timeMs(() => DB.saveToDBFiles()).ms;

  const fp = DB.getIndexFootprint({ dbMain: "USERS" });
  out.index_json_bytes = fp.indexJsonBytes;
  out.key_to_fragment_count = fp.keyToFragmentCount;
  out.fragments_count = fp.fragmentsCount;
  out.legacy_key_query_array_entries = fp.legacyKeyQueryArrayEntries;
  const vr = DB.validateRoutingConsistency({ dbMain: "USERS" });
  out.routing_ok = vr.ok ? 1 : 0;
  if (!vr.ok) {
    console.warn("validateRoutingConsistency failed n=" + n + ": " + vr.errors.join(" | "));
  }

  const mid = Math.max(1, Math.floor(n / 2));
  out.lookUpById_ms = timeMs(() => DB.lookUpById(mid, ctx)).ms;
  out.lookUpByKey_ms = timeMs(() => DB.lookUpByKey("k_" + mid, ctx)).ms;

  out.lookupByCriteria_id_ms = timeMs(() =>
    DB.lookupByCriteria([{ param: "id", criterion: mid }], ctx)
  ).ms;

  out.lookupByCriteria_complex_ms = timeMs(() =>
    DB.lookupByCriteria(
      [
        { path: ["profile"], param: "status", criterion: "Active" },
        { path: ["profile", "metrics"], param: "clearance", criterion: "L2" }
      ],
      ctx
    )
  ).ms;

  out.addToDB_update_ms = timeMs(() =>
    DB.addToDB(
      Object.assign({}, row(mid), { note: "updated", profile: row(mid).profile }),
      ctx
    )
  ).ms;

  out.saveIndex_ms = timeMs(() => DB.saveIndex()).ms;

  out.addExternalConfig_ms = timeMs(() =>
    DB.addExternalConfig("bench", { n: n }, { dbMain: "USERS", dbFragment: "USERS_1" })
  ).ms;
  out.getExternalConfig_ms = timeMs(() =>
    DB.getExternalConfig("bench", { dbMain: "USERS", dbFragment: "USERS_1" })
  ).ms;

  out.closeDB_then_lookUpById_ms = timeMs(() => {
    DB.closeDB({ dbMain: "USERS" });
    DB.lookUpById(mid, ctx);
  }).ms;

  out.deleteFromDBByKey_ms = timeMs(() => DB.deleteFromDBByKey("k_1", ctx)).ms;
  out.deleteFromDBById_ms = timeMs(() => DB.deleteFromDBById(2, ctx)).ms;

  out.saveToDBFiles_after_delete_ms = timeMs(() => DB.saveToDBFiles()).ms;

  out.destroyDB_ms = timeMs(() => DB.destroyDB({ dbMain: "USERS" })).ms;

  mock.wipe();
  return out;
}

function main() {
  const args = parseArgs();
  const rows = [];
  const keys = [
    "size",
    "index_json_bytes",
    "key_to_fragment_count",
    "fragments_count",
    "legacy_key_query_array_entries",
    "routing_ok",
    "seed_ms",
    "saveToDBFiles_ms",
    "lookUpById_ms",
    "lookUpByKey_ms",
    "lookupByCriteria_id_ms",
    "lookupByCriteria_complex_ms",
    "addToDB_update_ms",
    "saveIndex_ms",
    "addExternalConfig_ms",
    "getExternalConfig_ms",
    "closeDB_then_lookUpById_ms",
    "deleteFromDBByKey_ms",
    "deleteFromDBById_ms",
    "saveToDBFiles_after_delete_ms",
    "destroyDB_ms"
  ];

  console.log("ShardDB benchmark (mock Drive). Sizes: " + args.sizes.join(", "));
  console.log("— clearDB is not timed here (would wipe dataset mid-matrix); destroyDB cleans up per size.\n");

  for (const n of args.sizes) {
    if (n >= 100000) {
      console.warn("Warning: size " + n + " may take several minutes on slow machines.");
    }
    const r = runForSize(n);
    rows.push(r);
    console.log("size=" + n + " seed_ms=" + r.seed_ms + " criteria_complex_ms=" + r.lookupByCriteria_complex_ms);
  }

  console.log("\n--- Table (ms) ---");
  console.table(rows);

  const csvPath = path.join(__dirname, "benchmark-results.csv");
  const header = keys.join(",");
  const csvLines = [header].concat(
    rows.map((r) => keys.map((k) => (r[k] !== undefined ? r[k] : "")).join(","))
  );
  if (args.csv || true) {
    fs.writeFileSync(csvPath, csvLines.join("\n"), "utf8");
    console.log("\nWrote " + csvPath + " (open in Google Sheets / Excel → insert chart).");
  }

  const clearMs = (function () {
    const mock = createMockDrive({ dbDir: path.join(__dirname, ".mock_drive", "bench_clear") });
    mock.wipe();
    const DB = SHARD_DB.init(INDEX_ID, mock.adapter);
    DB.addToDB({ key: "a", id: 1, v: 1 }, { dbMain: "USERS" });
    DB.saveToDBFiles();
    const ms = timeMs(() => DB.clearDB({ dbMain: "USERS" })).ms;
    mock.wipe();
    return ms;
  })();
  console.log("\nclearDB (micro, 1 row dataset) ms: " + clearMs);
  fs.appendFileSync(csvPath, "\n# clearDB_micro_ms," + clearMs + "\n", "utf8");

  if (args.mermaid || process.stdout.isTTY) {
    const ops = [
      "seed_ms",
      "lookUpById_ms",
      "lookupByCriteria_complex_ms",
      "saveToDBFiles_ms",
      "closeDB_then_lookUpById_ms"
    ];
    const labels = rows.map((r) => String(r.size));
    let chart = "xychart-beta\n  title \"ShardDB benchmark (ms) by dataset size\"\n  x-axis [ " + labels.join(", ") + " ]\n  y-axis \"ms (log-scale workloads)\" 0 --> " + Math.max(1, ...rows.map((r) => Math.max(...ops.map((o) => r[o] || 0)))) + "\n";
    for (const op of ops) {
      const vals = rows.map((r) => r[op] || 0).join(", ");
      chart += "  line \"" + op + "\" [ " + vals + " ]\n";
    }
    const mdPath = path.join(__dirname, "benchmark-results.mmd");
    fs.writeFileSync(mdPath, chart, "utf8");
    console.log("Wrote " + mdPath + " (paste into https://mermaid.live or Mermaid-capable Markdown).");
    console.log("\n--- Mermaid preview ---\n" + chart);
  }
}

main();
