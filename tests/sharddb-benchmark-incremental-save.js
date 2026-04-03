"use strict";

/**
 * Measures saveToDBFiles() after small edits at a fixed dataset size N.
 * Always persists dirty fragment JSON(s). Master INDEX is skipped when indexRoutingDirty
 * stays false (pure payload update; same id+key routing).
 *
 * Usage:
 *   node tests/sharddb-benchmark-incremental-save.js
 *   node tests/sharddb-benchmark-incremental-save.js --n=50000
 */

const path = require("path");
const { loadShardDbUmd } = require("./helpers/load-sharddb");
const { createMockDrive, wrapAdapterWithWriteCounts } = require("./helpers/mock-drive");

const SHARD_DB = loadShardDbUmd();
const INDEX_ID = "INCR_BENCH_INDEX";

function parseN() {
  const a = process.argv.find((x) => x.startsWith("--n="));
  if (!a) return 10000;
  const n = parseInt(a.slice("--n=".length), 10);
  return Number.isFinite(n) && n > 0 ? n : 10000;
}

function timeMs(fn) {
  const t0 = Date.now();
  fn();
  return Date.now() - t0;
}

function row(i) {
  return {
    key: "k_" + i,
    id: i,
    email: "u" + i + "@x.io",
    profile: { status: i % 2 === 0 ? "Active" : "Archived" }
  };
}

function main() {
  const n = parseN();
  const mock = createMockDrive({
    dbDir: path.join(__dirname, ".mock_drive", "bench_incremental_" + n)
  });
  mock.wipe();

  const wrapped = wrapAdapterWithWriteCounts(mock.adapter, { indexFileId: INDEX_ID });
  const adapter = wrapped.adapter;
  const DB = SHARD_DB.init(INDEX_ID, adapter);
  const ctx = { dbMain: "USERS" };

  console.log("Incremental save benchmark (mock Drive). N=" + n + " rows seeded.\n");

  console.log("Seeding…");
  const seedMs = timeMs(() => {
    for (let i = 1; i <= n; i++) {
      DB.addToDB(row(i), ctx);
    }
  });
  const initialSaveMs = timeMs(() => DB.saveToDBFiles());
  const fpAfterSeed = DB.getIndexFootprint({ dbMain: "USERS" });
  console.log("seed_ms=" + seedMs + " initial_saveToDBFiles_ms=" + initialSaveMs);
  console.log(
    "index_json_bytes=" +
      fpAfterSeed.indexJsonBytes +
      " fragments=" +
      fpAfterSeed.fragmentsCount +
      "\n"
  );

  DB.closeDB({ dbMain: "USERS" });

  function runCase(label, mutateFn) {
    wrapped.reset();
    mutateFn();
    const saveMs = timeMs(() => DB.saveToDBFiles());
    const c = wrapped.counts();
    const fp = DB.getIndexFootprint({ dbMain: "USERS" });
    const v = DB.validateRoutingConsistency({ dbMain: "USERS" });
    console.log(label);
    console.log(
      "  saveToDBFiles_ms=" +
        saveMs +
        " | writes: fragment=" +
        c.fragmentWriteCount +
        " index=" +
        c.indexWriteCount +
        " indexRoutingDirty=" +
        fp.indexRoutingDirty +
        " (index_json_bytes=" +
        fp.indexJsonBytes +
        ") routing_ok=" +
        v.ok
    );
    DB.closeDB({ dbMain: "USERS" });
  }

  const mid = Math.max(1, Math.floor(n / 2));

  runCase("A) Update 1 row (same id/key, one fragment)", function () {
    DB.addToDB(Object.assign({}, row(mid), { note: "touch1" }), ctx);
  });

  runCase("B) Update 2 rows in the SAME fragment", function () {
    const a = Math.max(1, mid - 1);
    const b = Math.min(n, mid);
    DB.addToDB(Object.assign({}, row(a), { note: "a" }), ctx);
    DB.addToDB(Object.assign({}, row(b), { note: "b" }), ctx);
  });

  const maxPerFrag = SHARD_DB.MAX_ENTRIES_COUNT;
  const idFrag1 = 1;
  const idFrag2 = Math.min(n, maxPerFrag + 1);
  runCase("C) Update 1 row in EACH of TWO fragments (ids " + idFrag1 + " and " + idFrag2 + ")", function () {
    DB.addToDB(Object.assign({}, row(idFrag1), { note: "f1" }), ctx);
    DB.addToDB(Object.assign({}, row(idFrag2), { note: "f2" }), ctx);
  });

  console.log("\nInterpretation:");
  console.log(
    "- fragment/index = adapter write counts. Master INDEX write is skipped when indexRoutingDirty stayed false (pure payload update)."
  );
  console.log("- Compare save_ms to full-bench seed saves (flush entire dataset).");
  console.log("");

  mock.wipe();
}

main();
