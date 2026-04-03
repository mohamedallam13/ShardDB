"use strict";

/**
 * ShardDB — real Google Drive integration + performance tests
 *
 * Prerequisites (one-time setup):
 *   1. Copy .env.example → .env and fill in GCP_PROJECT_ID + CLIENT_CREDENTIAL_FILE
 *   2. gas-fakes auth          (OAuth consent, stores token)
 *   3. gas-fakes enableApis    (enables Drive API on your GCP project)
 *   4. Create a "ShardDB Tests" folder on Drive, paste its ID into SHARDDB_TEST_FOLDER_ID
 *
 * Run:
 *   npm run test:drive          — correctness + light perf (default 50 rows)
 *   npm run test:drive:perf     — heavier perf run (SHARDDB_PERF_ROWS=200)
 *
 * All Drive files created during the tests are deleted in afterEach cleanup.
 * If a test fails mid-run, files may be left behind in SHARDDB_TEST_FOLDER_ID —
 * delete them manually or re-run; the test always creates a fresh sub-folder.
 *
 * What this tests that the mock suite cannot:
 *   - Real DriveApp.createFile / getFileById / setContent / getBlob round-trips
 *   - Actual JSON serialization/deserialization across Drive I/O
 *   - Network-realistic latency for performance measurements
 *   - SHARD_DB_TOOLKIT.createDriveToolkitAdapter() end-to-end
 *   - wrapWithBackupRestore creates sibling .backup.json files correctly
 */

const { describe, it, before, after, afterEach } = require("node:test");
const assert = require("node:assert/strict");
const { performance } = require("node:perf_hooks");
const path = require("path");
const { bootstrapGasFakes } = require("./helpers/bootstrap-gas-fakes");
const { loadShardDbUmd } = require("./helpers/load-sharddb");

// ── Config ────────────────────────────────────────────────────────────────────
const FOLDER_ID = process.env.SHARDDB_TEST_FOLDER_ID || "";
const PERF_ROWS = parseInt(process.env.SHARDDB_PERF_ROWS || "50", 10);
const RUN_DRIVE = !!FOLDER_ID;

// ── Load ShardDB + Toolkit ────────────────────────────────────────────────────
const SHARD_DB = loadShardDbUmd();

// SHARD_DB_TOOLKIT is a UMD module attached to `this` — load it the same way.
function loadToolkitUmd() {
  const fs = require("fs");
  const toolkitPath = path.join(__dirname, "../src/ShardDB/ShardDBToolkitHelpers.js");
  const code = fs.readFileSync(toolkitPath, "utf8");
  const ctx = {};
  const fn = new Function("ctx", "code", "(function(){ eval(code); }).call(ctx); return ctx.SHARD_DB_TOOLKIT;");
  return fn(ctx, code);
}
const TOOLKIT = loadToolkitUmd();

// ── Helpers ───────────────────────────────────────────────────────────────────

/**
 * Create a fresh sub-folder inside FOLDER_ID for one test run.
 * Returns { folder, folderId }.
 */
function createTestFolder(label) {
  const parent = DriveApp.getFolderById(FOLDER_ID);
  const name = "ShardDB-test-" + label + "-" + Date.now();
  const folder = parent.createFolder(name);
  return { folder, folderId: folder.getId() };
}

/**
 * Create the master INDEX file for a new DB inside testFolderId.
 * Returns the fileId of the INDEX.
 */
function createIndexFile(testFolderId, payload) {
  const folder = DriveApp.getFolderById(testFolderId);
  const defaultPayload = payload || {
    USERS: {
      properties: {
        cumulative: true,
        rootFolder: testFolderId,
        filesPrefix: "chk",
        fragmentsList: [],
        keyToFragment: {},
        idRangesSorted: []
      },
      dbFragments: {}
    }
  };
  const file = folder.createFile(
    "SHARDDB_INDEX.json",
    JSON.stringify(defaultPayload),
    "application/json"
  );
  return file.getId();
}

/**
 * Delete an entire Drive folder (and all its contents) by ID.
 */
function deleteFolder(folderId) {
  try {
    DriveApp.getFolderById(folderId).setTrashed(true);
  } catch (e) {
    // best-effort cleanup
  }
}

/**
 * Time a synchronous callback. Returns { result, ms }.
 */
function timed(fn) {
  const t0 = performance.now();
  const result = fn();
  return { result, ms: performance.now() - t0 };
}

// ── Suite ─────────────────────────────────────────────────────────────────────

describe("ShardDB Drive integration", () => {
  before(async () => {
    await bootstrapGasFakes();
    if (!RUN_DRIVE) {
      console.log(
        "\n  ⚠  SHARDDB_TEST_FOLDER_ID not set — Drive integration tests are skipped.\n" +
          "     See .env.example for setup instructions.\n"
      );
    }
  });

  // ── 1. Adapter smoke test ────────────────────────────────────────────────────
  describe("DriveToolkitAdapter", () => {
    let testFolderId;

    afterEach(() => {
      if (testFolderId) deleteFolder(testFolderId);
      testFolderId = null;
    });

    it("createDriveToolkitAdapter creates, writes, reads, and deletes a JSON file", (t) => {
      if (!RUN_DRIVE) return t.skip("SHARDDB_TEST_FOLDER_ID not configured");

      const { folderId } = createTestFolder("adapter");
      testFolderId = folderId;

      const adapter = TOOLKIT.createDriveToolkitAdapter();
      const payload = { hello: "drive", n: 42 };

      const { result: fileId, ms: createMs } = timed(() =>
        adapter.createJSON("smoke-test", folderId, payload)
      );
      console.log(`    createJSON:      ${createMs.toFixed(1)} ms`);

      const { result: read1, ms: read1Ms } = timed(() => adapter.readFromJSON(fileId));
      console.log(`    readFromJSON:    ${read1Ms.toFixed(1)} ms`);
      assert.deepEqual(read1, payload);

      const updated = { hello: "updated", n: 99 };
      const { ms: writeMs } = timed(() => adapter.writeToJSON(fileId, updated));
      console.log(`    writeToJSON:     ${writeMs.toFixed(1)} ms`);

      const { result: read2, ms: read2Ms } = timed(() => adapter.readFromJSON(fileId));
      console.log(`    readFromJSON(2): ${read2Ms.toFixed(1)} ms`);
      assert.deepEqual(read2, updated);

      const { ms: deleteMs } = timed(() => adapter.deleteFile(fileId));
      console.log(`    deleteFile:      ${deleteMs.toFixed(1)} ms`);

      // Verify deleted (read should return null)
      const afterDelete = adapter.readFromJSON(fileId);
      assert.equal(afterDelete, null);
    });

    it("wrapWithBackupRestore creates a sibling .backup.json on write", (t) => {
      if (!RUN_DRIVE) return t.skip("SHARDDB_TEST_FOLDER_ID not configured");

      const { folderId } = createTestFolder("backup");
      testFolderId = folderId;

      const inner = TOOLKIT.createDriveToolkitAdapter();
      const wrapped = TOOLKIT.wrapWithBackupRestore(inner);

      const fileId = inner.createJSON("main-file", folderId, { v: 1 });
      wrapped.writeToJSON(fileId, { v: 2 });

      // Backup sibling should exist
      const folder = DriveApp.getFolderById(folderId);
      const it2 = folder.getFilesByName("main-file.backup.json");
      assert.ok(it2.hasNext(), "backup sibling file must exist after wrapped write");
      const backupContent = JSON.parse(it2.next().getBlob().getDataAsString());
      assert.deepEqual(backupContent, { v: 2 });
    });
  });

  // ── 2. Core correctness on real Drive ────────────────────────────────────────
  describe("Core correctness", () => {
    let testFolderId;
    let indexFileId;
    let DB;
    let adapter;

    before(() => {
      if (!RUN_DRIVE) return;
      const { folderId } = createTestFolder("core");
      testFolderId = folderId;
      adapter = TOOLKIT.createDriveToolkitAdapter();
      indexFileId = createIndexFile(testFolderId);
      DB = SHARD_DB.init(indexFileId, adapter);
    });

    after(() => {
      if (testFolderId) deleteFolder(testFolderId);
    });

    it("addToDB + saveToDBFiles + lookUpByKey round-trips through Drive", (t) => {
      if (!RUN_DRIVE) return t.skip("SHARDDB_TEST_FOLDER_ID not configured");

      DB.addToDB({ key: "alice", id: 1, email: "alice@test.com" }, { dbMain: "USERS" });
      DB.addToDB({ key: "bob", id: 2, email: "bob@test.com" }, { dbMain: "USERS" });
      DB.saveToDBFiles();

      // Re-init to prove data is persisted (not just in OPEN_DB)
      DB.closeDB({ dbMain: "USERS" });
      const DB2 = SHARD_DB.init(indexFileId, adapter);
      assert.equal(DB2.lookUpByKey("alice", { dbMain: "USERS" }).email, "alice@test.com");
      assert.equal(DB2.lookUpById(2, { dbMain: "USERS" }).key, "bob");

      const v = DB2.validateRoutingConsistency({ dbMain: "USERS" });
      assert.equal(v.ok, true, v.errors.join("; "));
    });

    it("lookupByCriteria with field filter works on Drive-persisted data", (t) => {
      if (!RUN_DRIVE) return t.skip("SHARDDB_TEST_FOLDER_ID not configured");

      DB.addToDB({ key: "carol", id: 3, role: "admin" }, { dbMain: "USERS" });
      DB.addToDB({ key: "dave", id: 4, role: "user" }, { dbMain: "USERS" });
      DB.addToDB({ key: "eve", id: 5 /* no role */ }, { dbMain: "USERS" });
      DB.saveToDBFiles();

      const admins = DB.lookupByCriteria([{ param: "role", criterion: "admin" }], {
        dbMain: "USERS"
      });
      assert.ok(
        admins.every((r) => r.role === "admin"),
        "only admin rows returned"
      );
      assert.ok(
        !admins.find((r) => r.key === "eve"),
        "row missing the field must not appear"
      );
    });

    it("deleteFromDBById removes row from Drive fragment on next save", (t) => {
      if (!RUN_DRIVE) return t.skip("SHARDDB_TEST_FOLDER_ID not configured");

      DB.addToDB({ key: "del-target", id: 99, v: "delete-me" }, { dbMain: "USERS" });
      DB.saveToDBFiles();
      DB.deleteFromDBById(99, { dbMain: "USERS" });
      DB.saveToDBFiles();
      DB.closeDB({ dbMain: "USERS" });

      const DB3 = SHARD_DB.init(indexFileId, adapter);
      assert.equal(DB3.lookUpById(99, { dbMain: "USERS" }), null);
      assert.equal(DB3.lookUpByKey("del-target", { dbMain: "USERS" }), null);
    });

    it("multi-fragment across Drive: idRangesSorted routes correctly after reload", (t) => {
      if (!RUN_DRIVE) return t.skip("SHARDDB_TEST_FOLDER_ID not configured");

      const cap = SHARD_DB.MAX_ENTRIES_COUNT;
      // Insert just enough to create a second fragment
      // (existing rows from prior tests count toward cap)
      const existing = Object.keys(DB.INDEX.USERS.properties.keyToFragment).length;
      const needed = cap - existing + 1;

      for (let i = 100; i < 100 + needed; i++) {
        DB.addToDB({ key: "bulk_" + i, id: 1000 + i, n: i }, { dbMain: "USERS" });
      }
      DB.saveToDBFiles();
      DB.closeDB({ dbMain: "USERS" });

      const DB4 = SHARD_DB.init(indexFileId, adapter);
      assert.ok(DB4.INDEX.USERS.properties.fragmentsList.length >= 2, "should have 2+ fragments");

      const v = DB4.validateRoutingConsistency({ dbMain: "USERS" });
      assert.equal(v.ok, true, v.errors.join("; "));
    });
  });

  // ── 3. Performance benchmarks ─────────────────────────────────────────────────
  describe("Performance benchmarks", () => {
    let testFolderId;

    afterEach(() => {
      if (testFolderId) deleteFolder(testFolderId);
      testFolderId = null;
    });

    it(
      `in-memory addToDB: ${PERF_ROWS} rows — no Drive writes until save`,
      { timeout: 120000 },
      (t) => {
        if (!RUN_DRIVE) return t.skip("SHARDDB_TEST_FOLDER_ID not configured");

        const { folderId } = createTestFolder("perf-add");
        testFolderId = folderId;
        const adapter = TOOLKIT.createDriveToolkitAdapter();
        const indexFileId = createIndexFile(folderId);
        const DB = SHARD_DB.init(indexFileId, adapter);

        const { ms: seedMs } = timed(() => {
          for (let i = 1; i <= PERF_ROWS; i++) {
            DB.addToDB({ key: "u" + i, id: i, email: "u" + i + "@perf.test", tag: i % 5 }, {
              dbMain: "USERS"
            });
          }
        });
        const perRow = seedMs / PERF_ROWS;
        console.log(`\n    addToDB ×${PERF_ROWS}:   ${seedMs.toFixed(1)} ms total  /  ${perRow.toFixed(3)} ms per row  (in-memory)`);
        assert.ok(perRow < 5, `addToDB should be < 5ms/row in-memory (got ${perRow.toFixed(3)} ms)`);
      }
    );

    it(
      `saveToDBFiles: flush ${PERF_ROWS} rows to real Drive — measures Drive write latency`,
      { timeout: 300000 },
      (t) => {
        if (!RUN_DRIVE) return t.skip("SHARDDB_TEST_FOLDER_ID not configured");

        const { folderId } = createTestFolder("perf-save");
        testFolderId = folderId;
        const adapter = TOOLKIT.createDriveToolkitAdapter();
        const indexFileId = createIndexFile(folderId);
        const DB = SHARD_DB.init(indexFileId, adapter);

        for (let i = 1; i <= PERF_ROWS; i++) {
          DB.addToDB({ key: "s" + i, id: i, v: i }, { dbMain: "USERS" });
        }

        const fragCount = DB.INDEX.USERS.properties.fragmentsList.length;
        const { ms: saveMs } = timed(() => DB.saveToDBFiles());

        const perFrag = saveMs / (fragCount + 1); // +1 for INDEX
        console.log(
          `\n    saveToDBFiles (${PERF_ROWS} rows, ${fragCount} fragments):` +
            `\n      total: ${saveMs.toFixed(1)} ms` +
            `\n      per Drive write: ~${perFrag.toFixed(0)} ms  (${fragCount + 1} writes)`
        );
        assert.ok(saveMs > 0, "save should take non-zero time");
      }
    );

    it(
      "lookUpById / lookUpByKey: cold read (fragment not in OPEN_DB) vs warm (cached)",
      { timeout: 120000 },
      (t) => {
        if (!RUN_DRIVE) return t.skip("SHARDDB_TEST_FOLDER_ID not configured");

        const { folderId } = createTestFolder("perf-lookup");
        testFolderId = folderId;
        const adapter = TOOLKIT.createDriveToolkitAdapter();
        const indexFileId = createIndexFile(folderId);
        const DB = SHARD_DB.init(indexFileId, adapter);

        for (let i = 1; i <= PERF_ROWS; i++) {
          DB.addToDB({ key: "lk" + i, id: i, v: i }, { dbMain: "USERS" });
        }
        DB.saveToDBFiles();
        DB.closeDB({ dbMain: "USERS" }); // evict from OPEN_DB

        // Cold read — fragment must be loaded from Drive
        const { ms: coldIdMs } = timed(() => DB.lookUpById(1, { dbMain: "USERS" }));
        const { ms: coldKeyMs } = timed(() => {
          DB.closeDB({ dbMain: "USERS" });
          return DB.lookUpByKey("lk1", { dbMain: "USERS" });
        });

        // Warm read — fragment already in OPEN_DB
        const { ms: warmIdMs } = timed(() => DB.lookUpById(1, { dbMain: "USERS" }));
        const { ms: warmKeyMs } = timed(() => DB.lookUpByKey("lk1", { dbMain: "USERS" }));

        console.log(
          `\n    lookUpById  — cold: ${coldIdMs.toFixed(1)} ms   warm: ${warmIdMs.toFixed(2)} ms` +
            `\n    lookUpByKey — cold: ${coldKeyMs.toFixed(1)} ms   warm: ${warmKeyMs.toFixed(2)} ms`
        );

        // Warm reads should be dramatically faster (pure in-memory)
        assert.ok(warmIdMs < coldIdMs, "warm lookUpById must be faster than cold");
        assert.ok(warmKeyMs < coldKeyMs, "warm lookUpByKey must be faster than cold");
        assert.ok(warmIdMs < 5, `warm lookUpById must be < 5ms (got ${warmIdMs.toFixed(2)} ms)`);
        assert.ok(warmKeyMs < 5, `warm lookUpByKey must be < 5ms (got ${warmKeyMs.toFixed(2)} ms)`);
      }
    );

    it(
      "saveToDBFiles: pure payload update skips INDEX write (indexRoutingDirty=false)",
      { timeout: 120000 },
      (t) => {
        if (!RUN_DRIVE) return t.skip("SHARDDB_TEST_FOLDER_ID not configured");

        const { folderId } = createTestFolder("perf-routing-dirty");
        testFolderId = folderId;
        const adapter = TOOLKIT.createDriveToolkitAdapter();
        const indexFileId = createIndexFile(folderId);
        const DB = SHARD_DB.init(indexFileId, adapter);

        DB.addToDB({ key: "x", id: 1, v: 1 }, { dbMain: "USERS" });
        const { ms: saveWithIndex } = timed(() => DB.saveToDBFiles()); // routing dirty → INDEX write
        assert.equal(DB.INDEX.USERS.properties.indexRoutingDirty, false);

        // In-place update — same id+key, no routing change
        DB.addToDB({ key: "x", id: 1, v: 2 }, { dbMain: "USERS" });
        assert.equal(DB.INDEX.USERS.properties.indexRoutingDirty, false);
        const { ms: saveNoIndex } = timed(() => DB.saveToDBFiles()); // should skip INDEX

        console.log(
          `\n    save WITH index write:    ${saveWithIndex.toFixed(1)} ms` +
            `\n    save WITHOUT index write: ${saveNoIndex.toFixed(1)} ms` +
            `\n    saved: ${(saveWithIndex - saveNoIndex).toFixed(1)} ms per update`
        );

        assert.equal(DB.lookUpById(1, { dbMain: "USERS" }).v, 2);
        assert.ok(
          saveNoIndex <= saveWithIndex,
          "skipping INDEX write should be faster or equal"
        );
      }
    );

    it(
      "lookupByCriteria full scan across all fragments (Drive-loaded)",
      { timeout: 300000 },
      (t) => {
        if (!RUN_DRIVE) return t.skip("SHARDDB_TEST_FOLDER_ID not configured");

        const { folderId } = createTestFolder("perf-scan");
        testFolderId = folderId;
        const adapter = TOOLKIT.createDriveToolkitAdapter();
        const indexFileId = createIndexFile(folderId);
        const DB = SHARD_DB.init(indexFileId, adapter);

        for (let i = 1; i <= PERF_ROWS; i++) {
          DB.addToDB({ key: "sc" + i, id: i, tag: i % 3 }, { dbMain: "USERS" });
        }
        DB.saveToDBFiles();
        DB.closeDB({ dbMain: "USERS" }); // force Drive reads during scan

        const fragCount = DB.INDEX.USERS.properties.fragmentsList.length;
        const { result: rows, ms: scanMs } = timed(() =>
          DB.lookupByCriteria([{ param: "tag", criterion: 0 }], { dbMain: "USERS" })
        );

        const expected = Math.floor(PERF_ROWS / 3);
        console.log(
          `\n    lookupByCriteria full scan (${PERF_ROWS} rows, ${fragCount} fragments):` +
            `\n      found: ${rows.length} rows  |  scan time: ${scanMs.toFixed(1)} ms` +
            `\n      (includes Drive reads for cold fragments)`
        );
        assert.ok(rows.length > 0, "should find rows matching tag=0");
        assert.ok(rows.every((r) => r.tag === 0), "all returned rows must match criterion");
      }
    );
  });

  // ── 4. Backup / restore adapter on Drive ──────────────────────────────────────
  describe("wrapWithBackupRestore on Drive", () => {
    let testFolderId;

    afterEach(() => {
      if (testFolderId) deleteFolder(testFolderId);
      testFolderId = null;
    });

    it("restore falls back to backup when primary read returns null", (t) => {
      if (!RUN_DRIVE) return t.skip("SHARDDB_TEST_FOLDER_ID not configured");

      const { folderId } = createTestFolder("backup-restore");
      testFolderId = folderId;

      const inner = TOOLKIT.createDriveToolkitAdapter();
      const wrapped = TOOLKIT.wrapWithBackupRestore(inner);

      // Write through wrapped adapter (creates primary + backup)
      const fileId = inner.createJSON("restore-test", folderId, { v: 1 });
      wrapped.writeToJSON(fileId, { v: 42 });

      // Corrupt primary by writing garbage
      inner.writeToJSON(fileId, "not-valid-json-{{{{");

      // Wrapped read should fall back to backup and return correct data
      const restored = wrapped.readFromJSON(fileId);
      assert.deepEqual(restored, { v: 42 }, "backup restore must return last good write");
    });
  });
});
