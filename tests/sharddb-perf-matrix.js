"use strict";

/**
 * ShardDB — performance matrix (standalone, not part of npm test).
 *
 * Measures raw throughput for the five core write operations across a range of
 * dataset sizes.  No correctness assertions are made; the goal is to provide a
 * stable performance baseline and detect regressions between runs.
 *
 * Operations measured per size N:
 *   1. SEED        — N × addToDB (in-memory, before any save)
 *   2. SAVE        — saveToDBFiles after seeding
 *   3. RELOAD      — init() from persisted state
 *   4. UPDATE      — N/10 in-place value updates + 1 key-change
 *   5. DELETE      — N/10 deleteFromDBById calls
 *   6. SAVE-DELTA  — saveToDBFiles after mutations
 *
 * Reported metrics per cell: total ms, ops/sec (where applicable).
 *
 * Usage:
 *   node tests/sharddb-perf-matrix.js
 *   node tests/sharddb-perf-matrix.js --sizes=1000,5000,10000
 *   node tests/sharddb-perf-matrix.js --full
 *   node tests/sharddb-perf-matrix.js --csv
 *
 * Flags:
 *   --sizes=N,N,…   Override dataset sizes (comma-separated integers)
 *   --full          Add 50 000 and 100 000 to the default set (slow)
 *   --csv           Write results to perf-matrix-results.csv alongside the console table
 */

const fs   = require("fs");
const path = require("path");
const { loadShardDbUmd }  = require("./helpers/load-sharddb");
const { createMockDrive } = require("./helpers/mock-drive");

const SHARD_DB = loadShardDbUmd();
const INDEX_ID = "PERF_MATRIX_INDEX";
const DB_MAIN  = "USERS";
const CTX      = { dbMain: DB_MAIN };

// ─── CLI args ─────────────────────────────────────────────────────────────────

function parseArgs() {
  const argv = process.argv.slice(2);
  const out = { sizes: null, full: false, csv: false };
  for (const a of argv) {
    if (a === "--full") out.full = true;
    else if (a === "--csv") out.csv = true;
    else if (a.startsWith("--sizes=")) {
      out.sizes = a.slice("--sizes=".length)
        .split(",")
        .map(s => parseInt(s.trim(), 10))
        .filter(n => Number.isFinite(n) && n > 0);
    }
  }
  if (!out.sizes || out.sizes.length === 0) {
    out.sizes = out.full
      ? [1_000, 5_000, 10_000, 50_000, 100_000]
      : [1_000, 5_000, 10_000];
  }
  return out;
}

// ─── helpers ─────────────────────────────────────────────────────────────────

/** High-resolution elapsed milliseconds */
function elapsed(t0) {
  const [s, ns] = process.hrtime(t0);
  return +(s * 1e3 + ns / 1e6).toFixed(2);
}

/** Build a deterministic row for index i */
function row(i) {
  return {
    id:    i,
    key:   "key_" + i,
    value: "val_" + i,
    score: i % 97,
    profile: {
      status: i % 2 === 0 ? "active" : "inactive",
      tags: [
        { label: "primary",   rank: i % 3 },
        { label: "secondary", rank: (i + 1) % 3 }
      ]
    }
  };
}

/** Format a number as "1 234.56" with thousand separators */
function fmt(n, decimals = 0) {
  return n.toLocaleString("en-US", {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals
  });
}

/** Format ops/sec: "123 456 ops/s" */
function opsPerSec(ops, ms) {
  if (ms <= 0) return "–";
  return fmt(Math.round(ops / (ms / 1000))) + " ops/s";
}

// ─── matrix runner ────────────────────────────────────────────────────────────

/**
 * @typedef {{ op: string, ms: number, tps: string }} Cell
 * @param {number} N
 * @returns {Cell[]}
 */
function runForSize(N) {
  const mockDir = path.join(__dirname, ".mock_drive", "perf_matrix_" + N);
  const mock    = createMockDrive({ dbDir: mockDir });
  mock.wipe();

  const cells = [];

  // ── 1. SEED ────────────────────────────────────────────────────────────────
  const DB = SHARD_DB.init(INDEX_ID, mock.adapter);
  let t0 = process.hrtime();
  for (let i = 1; i <= N; i++) {
    DB.addToDB(row(i), CTX);
  }
  const seedMs = elapsed(t0);
  cells.push({ op: "SEED", ms: seedMs, tps: opsPerSec(N, seedMs) });

  // ── 2. SAVE ────────────────────────────────────────────────────────────────
  t0 = process.hrtime();
  DB.saveToDBFiles();
  const saveMs = elapsed(t0);
  cells.push({ op: "SAVE", ms: saveMs, tps: "–" });

  // ── 3. RELOAD ──────────────────────────────────────────────────────────────
  t0 = process.hrtime();
  const DB2 = SHARD_DB.init(INDEX_ID, mock.adapter);
  const reloadMs = elapsed(t0);
  cells.push({ op: "RELOAD", ms: reloadMs, tps: "–" });

  // ── 4. UPDATE (in-place + 1 key-change) ───────────────────────────────────
  const updateCount = Math.max(1, Math.floor(N / 10));
  t0 = process.hrtime();
  for (let i = 1; i <= updateCount; i++) {
    DB2.addToDB({ id: i, key: "key_" + i, value: "upd_" + i }, CTX);
  }
  // one key-change
  const kcId = Math.ceil(N / 2);
  DB2.addToDB({ id: kcId, key: "changed_key_" + kcId, value: "kc_" + kcId }, CTX);
  const updateMs = elapsed(t0);
  cells.push({ op: "UPDATE", ms: updateMs, tps: opsPerSec(updateCount + 1, updateMs) });

  // ── 5. DELETE ──────────────────────────────────────────────────────────────
  const deleteCount = Math.max(1, Math.floor(N / 10));
  const deleteStart = updateCount + 1; // avoid deleting ids we just updated
  t0 = process.hrtime();
  for (let i = deleteStart; i < deleteStart + deleteCount; i++) {
    DB2.deleteFromDBById(i, CTX);
  }
  const deleteMs = elapsed(t0);
  cells.push({ op: "DELETE", ms: deleteMs, tps: opsPerSec(deleteCount, deleteMs) });

  // ── 6. SAVE-DELTA ──────────────────────────────────────────────────────────
  t0 = process.hrtime();
  DB2.saveToDBFiles();
  const saveDeltaMs = elapsed(t0);
  cells.push({ op: "SAVE-DELTA", ms: saveDeltaMs, tps: "–" });

  return cells;
}

// ─── table renderer ───────────────────────────────────────────────────────────

const OPS = ["SEED", "SAVE", "RELOAD", "UPDATE", "DELETE", "SAVE-DELTA"];

function renderTable(sizes, matrix) {
  // matrix[sizeIndex][opIndex] = Cell

  // Column widths
  const COL_OP  = 11;
  const COL_N   = sizes.map(n => Math.max(String(n).length + 2, 14));

  const hr = "─".repeat(COL_OP + 1 + COL_N.reduce((a, w) => a + w + 1, 0));

  const pad = (s, w) => String(s).padStart(w);
  const padL = (s, w) => String(s).padEnd(w);

  let out = "\n";
  out += "ShardDB Performance Matrix\n";
  out += hr + "\n";

  // Header row
  out += padL("Operation", COL_OP) + " ";
  sizes.forEach((n, si) => { out += pad("N=" + fmt(n), COL_N[si]) + " "; });
  out += "\n";
  out += "           "; // 11 chars for OP column
  sizes.forEach((n, si) => {
    out += pad("(ms / ops/s)", COL_N[si]) + " ";
  });
  out += "\n";
  out += hr + "\n";

  for (const op of OPS) {
    out += padL(op, COL_OP) + " ";
    sizes.forEach((n, si) => {
      const cell = matrix[si].find(c => c.op === op);
      const val  = cell
        ? (cell.tps === "–"
            ? fmt(cell.ms, 2) + " ms"
            : fmt(cell.ms, 2) + " / " + cell.tps)
        : "–";
      out += pad(val, COL_N[si]) + " ";
    });
    out += "\n";
  }

  out += hr + "\n";
  return out;
}

function renderCsv(sizes, matrix) {
  const rows = ["size," + OPS.map(op => op + "_ms," + op + "_tps").join(",")];
  sizes.forEach((n, si) => {
    const parts = [n];
    for (const op of OPS) {
      const cell = matrix[si].find(c => c.op === op);
      parts.push(cell ? cell.ms : "");
      parts.push(cell ? cell.tps.replace(" ops/s", "").replace(",", "") : "");
    }
    rows.push(parts.join(","));
  });
  return rows.join("\n") + "\n";
}

// ─── main ─────────────────────────────────────────────────────────────────────

function main() {
  const args = parseArgs();
  const { sizes, csv } = args;

  console.log("\nRunning ShardDB perf matrix for sizes: " + sizes.join(", ") + "\n");

  const matrix = sizes.map((n, idx) => {
    process.stdout.write("  [" + (idx + 1) + "/" + sizes.length + "] N=" + fmt(n) + " … ");
    const cells = runForSize(n);
    const seedCell = cells.find(c => c.op === "SEED");
    console.log("seed " + fmt(seedCell.ms, 1) + " ms");
    return cells;
  });

  console.log(renderTable(sizes, matrix));

  if (csv) {
    const csvPath = path.join(__dirname, "perf-matrix-results.csv");
    fs.writeFileSync(csvPath, renderCsv(sizes, matrix));
    console.log("CSV written to " + csvPath);
  }
}

main();
