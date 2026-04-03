;(function (root, factory) {
  root.SHARD_DB_TESTS = factory();
})(this, function () {
  /**
   * TEST_FOLDER_ID — read from Script Properties first so this file can be
   * checked in without a personal Drive ID baked in.
   *
   * To configure: Apps Script editor → Project Settings → Script Properties
   * → add key "SHARDDB_TEST_FOLDER_ID" with your Drive folder ID as the value.
   *
   * The hardcoded fallback is used when the property is absent (local dev).
   */
  const FALLBACK_TEST_FOLDER_ID = "1E_7mgRa6Pub901rpR-BescRuita0Gkb_";
  const TEST_FOLDER_ID = (function () {
    try {
      var prop = PropertiesService.getScriptProperties().getProperty("SHARDDB_TEST_FOLDER_ID");
      return prop || FALLBACK_TEST_FOLDER_ID;
    } catch (e) {
      return FALLBACK_TEST_FOLDER_ID;
    }
  })();

  // ─── Assertion helpers ─────────────────────────────────────────────────────

  /**
   * Hard assertion — throws immediately on failure.
   * @param {boolean} condition
   * @param {string}  message
   */
  function assert(condition, message) {
    if (!condition) throw new Error("ASSERTION FAILED: " + message);
  }

  function assertEqual(actual, expected, message) {
    if (actual !== expected) {
      throw new Error(
        "ASSERTION FAILED: " + message +
        " | expected=" + JSON.stringify(expected) +
        " actual=" + JSON.stringify(actual)
      );
    }
  }

  function assertNull(actual, message) {
    if (actual !== null && actual !== undefined) {
      throw new Error(
        "ASSERTION FAILED: " + message + " | expected null/undefined, got " + JSON.stringify(actual)
      );
    }
  }

  function assertNotNull(actual, message) {
    if (actual === null || actual === undefined) {
      throw new Error("ASSERTION FAILED: " + message + " | value was null/undefined");
    }
  }

  function assertDeepEqual(actual, expected, message) {
    var a = JSON.stringify(actual);
    var e = JSON.stringify(expected);
    if (a !== e) {
      throw new Error(
        "ASSERTION FAILED: " + message + " | expected=" + e + " actual=" + a
      );
    }
  }

  function emptyDbMain(filesPrefix) {
    return {
      properties: {
        cumulative: true,
        rootFolder: TEST_FOLDER_ID,
        filesPrefix: filesPrefix,
        fragmentsList: [],
        keyToFragment: {},
        idRangesSorted: []
      },
      dbFragments: {}
    };
  }

  function getRealDriveAdapter() {
    return {
      indexFileId: null,
      readFromJSON: function (fileId) {
        if (!fileId) return null;
        try {
          return JSON.parse(DriveApp.getFileById(fileId).getBlob().getDataAsString());
        } catch (e) {
          return null;
        }
      },
      writeToJSON: function (fileId, payload) {
        try {
          DriveApp.getFileById(fileId).setContent(JSON.stringify(payload));
        } catch (e) {
          Logger.log("Sync Error: " + e);
        }
      },
      createJSON: function (name, rootId, payload) {
        const folder = DriveApp.getFolderById(rootId);
        const file = folder.createFile(name + ".json", JSON.stringify(payload), "application/json");
        return file.getId();
      },
      deleteFile: function (fileId) {
        try {
          DriveApp.getFileById(fileId).setTrashed(true);
        } catch (e) {}
      }
    };
  }

  /** @returns {{ ms: number, result: * }} */
  function timed(label, fn) {
    const t0 = Date.now();
    const result = fn();
    const ms = Date.now() - t0;
    Logger.log("[ms=" + ms + "] " + label);
    return { ms: ms, result: result };
  }

  function driveFileUrl(fileId) {
    return "https://drive.google.com/file/d/" + fileId + "/view";
  }

  /** Right-pad a value to width w for fixed-width table columns. */
  function pad(val, w) {
    var s = String(val);
    while (s.length < w) s = s + " ";
    return s;
  }

  function trashDriveFileById(fileId) {
    try {
      DriveApp.getFileById(fileId).setTrashed(true);
    } catch (e) {}
  }

  function buildPlainTextSummary(perfRuns) {
    var lines = [
      "ShardDB — paste this block to your reviewer / AI",
      "Columns: size | index_json_bytes (master INDEX size, keys dominate) | routing_ok | seed_ms | save_ms | lookup_id_ms | criteria_ms | close_ms",
      ""
    ];
    for (var i = 0; i < perfRuns.length; i++) {
      var r = perfRuns[i];
      lines.push(
        [
          "n=" + r.size,
          "idxB=" + r.index_json_bytes,
          "keys=" + r.key_to_fragment_count,
          "frags=" + r.fragments_count,
          "route=" + (r.routing_ok ? "OK" : "FAIL"),
          "seed=" + r.seed_ms,
          "save=" + r.save_ms,
          "byId=" + r.lookup_id_ms,
          "crit=" + r.criteria_complex_ms,
          "close=" + r.close_lookup_ms
        ].join(" | ")
      );
    }
    return lines.join("\n");
  }

  /**
   * Touches every method on the object returned by SHARD_DB.init (plus INDEX/OPEN_DB reads).
   * Uses dbMain "USERS" and a separate "ORDERS" for isolated clearDB.
   */
  function runFullApiCoverage(DB) {
    Logger.log("\n========== FULL API COVERAGE (all public methods) ==========");
    const U = { dbMain: "USERS" };
    const O = { dbMain: "ORDERS" };

    timed("addToDB seed row 1", function () {
      DB.addToDB({ key: "cov_1", id: 1, email: "a@t.com", profile: { status: "Active" } }, U);
    });
    timed("addToDB seed row 2", function () {
      DB.addToDB({ key: "cov_2", id: 2, email: "b@t.com", profile: { status: "Archived" } }, U);
    });
    timed("saveToDBFiles", function () {
      DB.saveToDBFiles();
    });

    timed("lookUpById(1)", function () {
      return DB.lookUpById(1, U);
    });
    timed("lookUpByKey(cov_2)", function () {
      return DB.lookUpByKey("cov_2", U);
    });
    timed("lookupByCriteria [id]", function () {
      return DB.lookupByCriteria([{ param: "id", criterion: 2 }], U);
    });
    timed("lookupByCriteria nested", function () {
      return DB.lookupByCriteria(
        [{ path: ["profile"], param: "status", criterion: "Active" }],
        U
      );
    });

    timed("addToDB update same id (replace row 1)", function () {
      DB.addToDB(
        { key: "cov_1", id: 1, email: "updated@t.com", profile: { status: "Active" } },
        U
      );
    });
    timed("saveIndex", function () {
      DB.saveIndex();
    });

    timed("addExternalConfig on USERS_1", function () {
      DB.addExternalConfig("suite", { v: 1 }, { dbMain: "USERS", dbFragment: "USERS_1" });
    });
    timed("getExternalConfig", function () {
      return DB.getExternalConfig("suite", { dbMain: "USERS", dbFragment: "USERS_1" });
    });

    timed("closeDB(USERS) then lookUpById cold", function () {
      DB.closeDB({ dbMain: "USERS" });
      return DB.lookUpById(1, U);
    });

    timed("deleteFromDBByKey(cov_2)", function () {
      DB.deleteFromDBByKey("cov_2", U);
    });
    timed("deleteFromDBById(1)", function () {
      DB.deleteFromDBById(1, U);
    });
    timed("saveToDBFiles after deletes", function () {
      DB.saveToDBFiles();
    });

    timed("ORDERS addToDB one row (for clearDB isolation)", function () {
      DB.addToDB({ key: "ord_1", id: 1, x: 1 }, O);
    });
    timed("saveToDBFiles ORDERS", function () {
      DB.saveToDBFiles();
    });
    timed("clearDB(ORDERS) only", function () {
      DB.clearDB({ dbMain: "ORDERS" });
    });
    timed("destroyDB(ORDERS)", function () {
      DB.destroyDB({ dbMain: "ORDERS" });
    });

    Logger.log("INDEX.USERS keys: " + (DB.INDEX.USERS ? "ok" : "missing"));
    Logger.log("========== END FULL API COVERAGE ==========\n");
  }

  /**
   * Different orderings: open/close/save single/small churn.
   */
  function runSequenceScenarios(DB) {
    Logger.log("\n========== SEQUENCE SCENARIOS ==========");
    const M = { dbMain: "USERS" };

    timed("seq: save 1 new entry", function () {
      DB.addToDB({ key: "seq_a", id: 9001, n: 1 }, M);
      DB.saveToDBFiles();
    });
    timed("seq: close → open via lookUpByKey", function () {
      DB.closeDB(M);
      return DB.lookUpByKey("seq_a", M);
    });
    timed("seq: close → open via lookUpById", function () {
      DB.closeDB(M);
      return DB.lookUpById(9001, M);
    });
    timed("seq: update 1 entry (same id)", function () {
      DB.addToDB({ key: "seq_a", id: 9001, n: 2, note: "v2" }, M);
      DB.saveToDBFiles();
    });
    timed("seq: retrieve by id / key / criteria after update", function () {
      const a = DB.lookUpById(9001, M);
      const b = DB.lookUpByKey("seq_a", M);
      const c = DB.lookupByCriteria([{ param: "note", criterion: "v2" }], M);
      return { a: a, b: b, cLen: c.length };
    });
    timed("seq: remove 1 by key", function () {
      DB.deleteFromDBByKey("seq_a", M);
      DB.saveToDBFiles();
    });

    Logger.log("========== END SEQUENCE SCENARIOS ==========\n");
  }

  /**
   * Log-scale sizes. Returns rows + temp INDEX file ids (caller trashes those after writing the single report file).
   * destroyDB unchanged — removes shard fragment files only; master INDEX files are trashed separately.
   */
  function runPerfMatrix(adapter, sizes, benchIndexFileId) {
    var restoreAdapterIndex =
      benchIndexFileId != null && benchIndexFileId !== ""
        ? benchIndexFileId
        : adapter.indexFileId;

    Logger.log("\n========== PERF MATRIX (metrics also go into SHARD_PERF_REPORT_*.json) ==========");
    Logger.log(
      "DECODE_CSV: size=dataset rows; index_json_bytes=serialized USERS subtree length (keyToFragment ~ O(rows)); routing_ok=1 if validateRoutingConsistency passed; *_ms columns are timings; legacy_kqa should be 0 after migration."
    );
    Logger.log(
      "csv_header,size,index_json_bytes,key_to_fragment_count,fragments_count,routing_ok,legacy_kqa,seed_ms,save_ms,lookup_id_ms,criteria_complex_ms,close_lookup_ms"
    );
    const folder = DriveApp.getFolderById(TEST_FOLDER_ID);
    var perfRuns = [];
    var perfIndexIdsToTrash = [];

    for (let si = 0; si < sizes.length; si++) {
      const n = sizes[si];
      const name = "SHARD_PERF_" + n + "_" + Date.now() + ".json";
      const initial = {
        USERS: emptyDbMain("chk")
      };
      const indexId = folder.createFile(name, JSON.stringify(initial), "application/json").getId();
      perfIndexIdsToTrash.push(indexId);
      adapter.indexFileId = indexId;
      const DB = SHARD_DB.init(indexId, adapter);

      const ctx = { dbMain: "USERS" };
      const tSeed = timed("perf seed n=" + n, function () {
        for (let i = 1; i <= n; i++) {
          DB.addToDB(
            {
              key: "p_" + i,
              id: i,
              email: "e" + i + "@p.io",
              profile: {
                status: i % 2 === 0 ? "Active" : "Archived",
                metrics: [{ clearance: i > n / 2 ? "L2" : "L1", score: i % 99 }]
              }
            },
            ctx
          );
        }
      });
      const tSave = timed("perf saveToDBFiles n=" + n, function () {
        DB.saveToDBFiles();
      });
      var fp = DB.getIndexFootprint({ dbMain: "USERS" });
      var vr = DB.validateRoutingConsistency({ dbMain: "USERS" });
      Logger.log(
        "index_growth n=" +
          n +
          " master_INDEX_json_bytes≈" +
          fp.indexJsonBytes +
          " keys=" +
          fp.keyToFragmentCount +
          " fragments=" +
          fp.fragmentsCount +
          " legacy_keyQueryArray_entries=" +
          fp.legacyKeyQueryArrayEntries
      );
      Logger.log("routing_consistency n=" + n + " " + (vr.ok ? "OK" : "FAIL " + vr.errors.join(" | ")));
      const mid = Math.max(1, Math.floor(n / 2));
      const tLid = timed("perf lookUpById n=" + n, function () {
        return DB.lookUpById(mid, ctx);
      });
      const tCrit = timed("perf lookupByCriteria complex n=" + n, function () {
        return DB.lookupByCriteria(
          [
            { path: ["profile"], param: "status", criterion: "Active" },
            { path: ["profile", "metrics"], param: "clearance", criterion: "L2" }
          ],
          ctx
        );
      });
      const tClose = timed("perf close+lookUpById n=" + n, function () {
        DB.closeDB({ dbMain: "USERS" });
        return DB.lookUpById(mid, ctx);
      });

      Logger.log(
        "csv_row," +
          n +
          "," +
          fp.indexJsonBytes +
          "," +
          fp.keyToFragmentCount +
          "," +
          fp.fragmentsCount +
          "," +
          (vr.ok ? 1 : 0) +
          "," +
          fp.legacyKeyQueryArrayEntries +
          "," +
          tSeed.ms +
          "," +
          tSave.ms +
          "," +
          tLid.ms +
          "," +
          tCrit.ms +
          "," +
          tClose.ms
      );

      perfRuns.push({
        size: n,
        temp_index_file_name: name,
        temp_index_file_id: indexId,
        index_json_bytes: fp.indexJsonBytes,
        key_to_fragment_count: fp.keyToFragmentCount,
        fragments_count: fp.fragmentsCount,
        legacy_key_query_array_entries: fp.legacyKeyQueryArrayEntries,
        routing_ok: vr.ok,
        routing_errors: vr.ok ? [] : vr.errors,
        seed_ms: tSeed.ms,
        save_ms: tSave.ms,
        lookup_id_ms: tLid.ms,
        criteria_complex_ms: tCrit.ms,
        close_lookup_ms: tClose.ms
      });

      timed("perf destroyDB n=" + n, function () {
        DB.destroyDB({ dbMain: "USERS" });
      });
      adapter.indexFileId = restoreAdapterIndex;
    }
    Logger.log("========== END PERF MATRIX ==========\n");
    return { perfRuns: perfRuns, perfIndexIdsToTrash: perfIndexIdsToTrash };
  }

  function testMassiveDataInsertion(DB) {
    Logger.log("==================================================");
    Logger.log("GENERATING 1,500 DEEPLY NESTED RECORDS...");
    for (let i = 1; i <= 1500; i++) {
      const payloadObj = {
        key: "u_" + i,
        id: i,
        profile: {
          contact: { email: "tester_" + i + "@sharddb.io" },
          status: i % 2 === 0 ? "Active" : "Archived",
          metrics: [{ clearance: i > 1000 ? "Level_2" : "Level_1", score: 90 }]
        }
      };
      DB.addToDB(payloadObj, { dbMain: "USERS" });
    }
    timed("saveToDBFiles (1500 rows)", function () {
      DB.saveToDBFiles();
    });
  }

  function testLookupsAndFilters(DB) {
    Logger.log("==================================================");
    const entryHot = timed("lookUpById(1250)", function () {
      return DB.lookUpById(1250, { dbMain: "USERS" });
    }).result;
    if (!entryHot || !entryHot.profile) {
      Logger.log("ERROR: lookUpById(1250) missing profile. " + JSON.stringify(entryHot));
      if (DB.INDEX && DB.INDEX.USERS && DB.INDEX.USERS.properties) {
        Logger.log("idRangesSorted: " + JSON.stringify(DB.INDEX.USERS.properties.idRangesSorted));
      }
      return;
    }
    Logger.log("contact: " + JSON.stringify(entryHot.profile.contact));
    timed("lookupByCriteria nested", function () {
      return DB.lookupByCriteria(
        [
          { path: ["profile"], param: "status", criterion: "Active" },
          { path: ["profile", "metrics"], param: "clearance", criterion: "Level_2" }
        ],
        { dbMain: "USERS" }
      );
    });
  }

  function testDeletionsAndCleanups(DB) {
    Logger.log("==================================================");
    timed("deleteFromDBByKey u_1250", function () {
      DB.deleteFromDBByKey("u_1250", { dbMain: "USERS" });
      DB.saveToDBFiles();
    });
    const fetchCheck = DB.lookUpByKey("u_1250", { dbMain: "USERS" });
    Logger.log(fetchCheck ? "ERROR: u_1250 still exists" : "OK: u_1250 deleted");
  }

  /**
   * Original visual flow: one index file, 1500 rows, lookups, one delete.
   */
  function runFlow() {
    Logger.log("======== SHARD_DB VISUAL REAL-DRIVE (legacy) ========");
    const realDrive = getRealDriveAdapter();
    const folder = DriveApp.getFolderById(TEST_FOLDER_ID);
    const MASTER_INDEX_NAME = "MASTER_INDEX_VISUAL.json";
    const existing = folder.getFilesByName(MASTER_INDEX_NAME);
    let indexId;
    if (existing.hasNext()) {
      indexId = existing.next().getId();
    } else {
      const initialPayload = { USERS: emptyDbMain("chk") };
      indexId = folder.createFile(MASTER_INDEX_NAME, JSON.stringify(initialPayload), "application/json").getId();
    }
    realDrive.indexFileId = indexId;
    const DBEngine = SHARD_DB.init(indexId, realDrive);
    testMassiveDataInsertion(DBEngine);
    testLookupsAndFilters(DBEngine);
    testDeletionsAndCleanups(DBEngine);
  }

  /**
   * Full benchmark: API coverage + sequences + perf matrix.
   * Writes one SHARD_PERF_REPORT_<timestamp>.json (share this for feedback). Trashes all transient INDEX JSONs after.
   */
  function runFullBenchmarkSuite(options) {
    options = options || {};
    const realDrive = getRealDriveAdapter();
    const folder = DriveApp.getFolderById(TEST_FOLDER_ID);
    const name = "MASTER_INDEX_BENCH_" + Date.now() + ".json";
    const initialPayload = {
      USERS: emptyDbMain("chk"),
      ORDERS: emptyDbMain("ord")
    };
    const indexId = folder.createFile(name, JSON.stringify(initialPayload), "application/json").getId();
    realDrive.indexFileId = indexId;
    const DB = SHARD_DB.init(indexId, realDrive);

    runFullApiCoverage(DB);
    runSequenceScenarios(DB);
    // Destroy USERS fragments created by the two suites above so no orphan shard files remain
    timed("destroyDB(USERS) — bench cleanup", function () {
      DB.destroyDB({ dbMain: "USERS" });
    });

    const sizes = options.perfSizes || [100, 1000, 10000];
    const perfResult = runPerfMatrix(realDrive, sizes, indexId);

    var decodeCsv =
      "size=dataset rows; index_json_bytes=master INDEX JSON size for USERS subtree (keyToFragment grows ~linearly with row count); routing_ok=validateRoutingConsistency; legacy_kqa should be 0; *_ms=timings measured at that N before destroyDB.";

    var report = {
      schemaVersion: 1,
      title: "ShardDB performance report — share this file to compare vs Sheets / a plain JSON file / Firestore / etc.",
      purpose:
        "Decide whether ShardDB routing + Drive shards is worth the complexity. Only this file is kept; bench and perf INDEX files are moved to trash after the run.",
      generatedAt: new Date().toISOString(),
      testFolderId: TEST_FOLDER_ID,
      perfSizesRequested: sizes,
      decodeCsv: decodeCsv,
      csvHeader:
        "size,index_json_bytes,key_to_fragment_count,fragments_count,routing_ok,legacy_kqa,seed_ms,save_ms,lookup_id_ms,criteria_complex_ms,close_lookup_ms",
      bench: {
        temp_master_index_file_name: name,
        temp_master_index_file_id: indexId,
        note: "Used for API coverage + sequence scenarios; trashed after report (destroyDB already removed shard fragments)."
      },
      perfRuns: perfResult.perfRuns,
      plainTextSummary: buildPlainTextSummary(perfResult.perfRuns)
    };

    var reportName = "SHARD_PERF_REPORT_" + Date.now() + ".json";
    var reportId = folder
      .createFile(reportName, JSON.stringify(report, null, 2), "application/json")
      .getId();

    Logger.log("========== ONLY FILE TO KEEP / SHARE ==========");
    Logger.log("SHARD_PERF_REPORT name=" + reportName + " fileId=" + reportId);
    Logger.log("url=" + driveFileUrl(reportId));
    Logger.log("plainTextSummary:\n" + report.plainTextSummary);
    Logger.log("========== trashing transient INDEX files (shard data already gone via destroyDB) ==========");

    var i;
    for (i = 0; i < perfResult.perfIndexIdsToTrash.length; i++) {
      trashDriveFileById(perfResult.perfIndexIdsToTrash[i]);
    }
    trashDriveFileById(indexId);

    Logger.log("Done. Open the report JSON in Drive and share it — everything else from this run is in trash.");
  }

  // ─── Assertion suite ───────────────────────────────────────────────────────

  /**
   * Creates a fresh ephemeral INDEX file in TEST_FOLDER_ID and returns
   * { db, indexFileId, adapter } for the given dbMains config.
   * Caller is responsible for trashing indexFileId when done.
   *
   * @param {object} adapter        — Drive adapter
   * @param {object} initialPayload — INDEX JSON payload
   * @param {object} [options]      — forwarded to SHARD_DB.init (e.g. { partitionBy: {...} })
   */
  function makeAssertDB(adapter, initialPayload, options) {
    var folder = DriveApp.getFolderById(TEST_FOLDER_ID);
    var name = "ASSERT_INDEX_" + Date.now() + ".json";
    var indexId = folder.createFile(name, JSON.stringify(initialPayload), "application/json").getId();
    var db = SHARD_DB.init(indexId, adapter, options || {});
    return { db: db, indexFileId: indexId, adapter: adapter };
  }

  function emptyAssertIndex(filesPrefix) {
    return {
      USERS: emptyDbMain(filesPrefix || "assert")
    };
  }

  /**
   * Full assertion suite that runs on real Drive.
   * Call via runShardDbAssertionSuite() top-level entry.
   *
   * Groups:
   *   A. getValueFromPath array fix
   *   B. OPEN_DB composite key (no collision across dbMains)
   *   C. createNewFile rollback guard (best-effort; verified structurally)
   *   D. lookupByCriteria id + extra criteria warning
   *   E. maxEntriesCount per-instance override
   *   F. Full API round-trip on real Drive
   *   G. wrapWithBackupRestore — backup mirrors writes, restores on primary failure
   */
  function runAssertionSuite() {
    var realDrive = getRealDriveAdapter();
    var indexIdsToTrash = [];
    var passed = 0;

    function run(label, fn) {
      Logger.log("  RUN: " + label);
      fn();
      passed++;
      Logger.log("  PASS: " + label);
    }

    Logger.log("\n========== ShardDB ASSERTION SUITE ==========");
    Logger.log("TEST_FOLDER_ID = " + TEST_FOLDER_ID);

    // ── A. getValueFromPath array fix ────────────────────────────────────────

    run("A1: array element at index > 0 is found", function () {
      var ctx = makeAssertDB(realDrive, emptyAssertIndex("a1"));
      indexIdsToTrash.push(ctx.indexFileId);
      var db = ctx.db;
      db.addToDB({
        key: "row1", id: 1,
        profile: {
          metrics: [
            { clearance: "L1", score: 10 },
            { clearance: "L2", score: 20 }   // index 1 — was broken before fix
          ]
        }
      }, { dbMain: "USERS" });
      db.addToDB({
        key: "row2", id: 2,
        profile: { metrics: [{ clearance: "L1", score: 5 }] }
      }, { dbMain: "USERS" });

      var rows = db.lookupByCriteria(
        [{ path: ["profile", "metrics"], param: "clearance", criterion: "L2" }],
        { dbMain: "USERS" }
      );
      assertEqual(rows.length, 1, "A1: should find exactly 1 row with clearance L2");
      assertEqual(rows[0].key, "row1", "A1: should be row1");
      db.destroyDB({ dbMain: "USERS" });
    });

    run("A2: array element absent — zero results (no crash)", function () {
      var ctx = makeAssertDB(realDrive, emptyAssertIndex("a2"));
      indexIdsToTrash.push(ctx.indexFileId);
      var db = ctx.db;
      db.addToDB({
        key: "r", id: 1,
        tags: [{ type: "color" }, { type: "size" }]  // neither has "missing_field"
      }, { dbMain: "USERS" });

      var rows = db.lookupByCriteria(
        [{ path: ["tags"], param: "missing_field", criterion: "x" }],
        { dbMain: "USERS" }
      );
      assertEqual(rows.length, 0, "A2: should return 0 rows for absent param");
      db.destroyDB({ dbMain: "USERS" });
    });

    run("A3: nested intermediate array, match at index 2", function () {
      var ctx = makeAssertDB(realDrive, emptyAssertIndex("a3"));
      indexIdsToTrash.push(ctx.indexFileId);
      var db = ctx.db;
      db.addToDB({
        key: "a", id: 1,
        data: { sections: [{ role: "viewer" }, { role: "editor" }, { role: "owner" }] }
      }, { dbMain: "USERS" });
      db.addToDB({
        key: "b", id: 2,
        data: { sections: [{ role: "viewer" }] }
      }, { dbMain: "USERS" });

      var rows = db.lookupByCriteria(
        [{ path: ["data", "sections"], param: "role", criterion: "owner" }],
        { dbMain: "USERS" }
      );
      assertEqual(rows.length, 1, "A3: should find row with owner at index 2");
      assertEqual(rows[0].key, "a", "A3: key should be 'a'");
      db.destroyDB({ dbMain: "USERS" });
    });

    // ── B. OPEN_DB composite key — two dbMains with fragment suffix _1 don't collide ──

    run("B1: two dbMains with same fragment suffix don't collide in OPEN_DB", function () {
      var folder = DriveApp.getFolderById(TEST_FOLDER_ID);
      var name = "ASSERT_INDEX_B1_" + Date.now() + ".json";
      var payload = {
        USERS:  emptyDbMain("usr"),
        ORDERS: emptyDbMain("ord")
      };
      var indexId = folder.createFile(name, JSON.stringify(payload), "application/json").getId();
      indexIdsToTrash.push(indexId);
      var db = SHARD_DB.init(indexId, realDrive);

      db.addToDB({ key: "u1", id: 1, type: "user" },  { dbMain: "USERS" });
      db.addToDB({ key: "o1", id: 1, type: "order" }, { dbMain: "ORDERS" });
      db.saveToDBFiles();

      var u = db.lookUpById(1, { dbMain: "USERS" });
      var o = db.lookUpById(1, { dbMain: "ORDERS" });
      assertNotNull(u, "B1: USERS id=1 should be found");
      assertNotNull(o, "B1: ORDERS id=1 should be found");
      assertEqual(u.type, "user",  "B1: USERS row should be type=user");
      assertEqual(o.type, "order", "B1: ORDERS row should be type=order");

      var vU = db.validateRoutingConsistency({ dbMain: "USERS" });
      var vO = db.validateRoutingConsistency({ dbMain: "ORDERS" });
      assert(vU.ok, "B1: USERS routing: " + vU.errors.join("; "));
      assert(vO.ok, "B1: ORDERS routing: " + vO.errors.join("; "));
      db.destroyDB({});
    });

    // ── C. createNewFile rollback guard ─────────────────────────────────────
    // We verify structurally that after a successful createJSON the fileId is
    // populated (not left as ""). A forced failure path requires a custom adapter.

    run("C1: createNewFile sets fileId after successful Drive file creation", function () {
      var ctx = makeAssertDB(realDrive, emptyAssertIndex("c1"));
      indexIdsToTrash.push(ctx.indexFileId);
      var db = ctx.db;
      db.addToDB({ key: "x", id: 1, v: 1 }, { dbMain: "USERS" });
      db.saveToDBFiles();

      var frag = db.INDEX.USERS.dbFragments.USERS_1;
      assertNotNull(frag, "C1: USERS_1 fragment should exist");
      assert(frag.fileId !== "", "C1: fileId must be set after save (not empty string)");
      db.destroyDB({ dbMain: "USERS" });
    });

    // ── D. lookupByCriteria id + extra criteria: warning emitted ────────────
    // We can't easily intercept console.warn on GAS, so we verify the behaviour:
    // the id fast-path still returns the correct row (not broken by the warning).

    run("D1: lookupByCriteria with id + extra criterion still returns the correct row", function () {
      var ctx = makeAssertDB(realDrive, emptyAssertIndex("d1"));
      indexIdsToTrash.push(ctx.indexFileId);
      var db = ctx.db;
      db.addToDB({ key: "a", id: 42, tag: "hit" },  { dbMain: "USERS" });
      db.addToDB({ key: "b", id: 43, tag: "miss" }, { dbMain: "USERS" });

      // id fast-path (extra criterion is warned about but row is still returned)
      var rows = db.lookupByCriteria(
        [{ param: "id", criterion: 42 }, { param: "tag", criterion: "miss" }],
        { dbMain: "USERS" }
      );
      assertEqual(rows.length, 1, "D1: id fast-path should return 1 row");
      assertEqual(rows[0].tag, "hit", "D1: should return id=42 row regardless of extra criterion");
      db.destroyDB({ dbMain: "USERS" });
    });

    // ── E. maxEntriesCount per-instance override ─────────────────────────────

    run("E1: maxEntriesCount=3 creates new fragment after 3 rows", function () {
      var folder = DriveApp.getFolderById(TEST_FOLDER_ID);
      var name = "ASSERT_INDEX_E1_" + Date.now() + ".json";
      var indexId = folder.createFile(name, JSON.stringify(emptyAssertIndex("e1")), "application/json").getId();
      indexIdsToTrash.push(indexId);
      var db = SHARD_DB.init(indexId, realDrive, { maxEntriesCount: 3 });

      assertEqual(db.maxEntriesCount, 3, "E1: instance should report maxEntriesCount=3");
      for (var i = 1; i <= 4; i++) {
        db.addToDB({ key: "k" + i, id: i, n: i }, { dbMain: "USERS" });
      }
      db.saveToDBFiles();

      assertEqual(db.INDEX.USERS.properties.fragmentsList.length, 2,
        "E1: should have 2 fragments (rolls at 3)");
      assertNotNull(db.lookUpById(4, { dbMain: "USERS" }), "E1: id=4 should be in second fragment");
      var v = db.validateRoutingConsistency({ dbMain: "USERS" });
      assert(v.ok, "E1: routing consistency: " + v.errors.join("; "));
      db.destroyDB({ dbMain: "USERS" });
    });

    // ── F. Full API round-trip on real Drive ─────────────────────────────────

    run("F1: add → save → reload (new init) → all data intact", function () {
      var ctx = makeAssertDB(realDrive, emptyAssertIndex("f1"));
      indexIdsToTrash.push(ctx.indexFileId);
      var db = ctx.db;
      db.addToDB({ key: "p1", id: 1, payload: "hello" }, { dbMain: "USERS" });
      db.addToDB({ key: "p2", id: 2, payload: "world" }, { dbMain: "USERS" });
      db.saveToDBFiles();

      var db2 = SHARD_DB.init(ctx.indexFileId, realDrive);
      assertEqual(db2.lookUpByKey("p1", { dbMain: "USERS" }).payload, "hello", "F1: p1 must survive reload");
      assertEqual(db2.lookUpById(2, { dbMain: "USERS" }).payload, "world", "F1: id=2 must survive reload");
      var v = db2.validateRoutingConsistency({ dbMain: "USERS" });
      assert(v.ok, "F1: routing after reload: " + v.errors.join("; "));
      db2.destroyDB({ dbMain: "USERS" });
    });

    run("F2: delete → save → reload — deleted row absent", function () {
      var ctx = makeAssertDB(realDrive, emptyAssertIndex("f2"));
      indexIdsToTrash.push(ctx.indexFileId);
      var db = ctx.db;
      db.addToDB({ key: "keep", id: 1, v: 1 }, { dbMain: "USERS" });
      db.addToDB({ key: "gone", id: 2, v: 2 }, { dbMain: "USERS" });
      db.deleteFromDBById(2, { dbMain: "USERS" });
      db.saveToDBFiles();

      var db2 = SHARD_DB.init(ctx.indexFileId, realDrive);
      assertNotNull(db2.lookUpById(1, { dbMain: "USERS" }), "F2: id=1 should survive");
      assertNull(db2.lookUpById(2, { dbMain: "USERS" }), "F2: id=2 must be absent after delete+reload");
      assertNull(db2.lookUpByKey("gone", { dbMain: "USERS" }), "F2: key 'gone' must be absent");
      db2.destroyDB({ dbMain: "USERS" });
    });

    run("F3: key-change → save → reload — new key resolves, old key absent", function () {
      var ctx = makeAssertDB(realDrive, emptyAssertIndex("f3"));
      indexIdsToTrash.push(ctx.indexFileId);
      var db = ctx.db;
      db.addToDB({ key: "oldKey", id: 10, v: 1 }, { dbMain: "USERS" });
      db.addToDB({ key: "newKey", id: 10, v: 2 }, { dbMain: "USERS" });
      db.saveToDBFiles();

      var db2 = SHARD_DB.init(ctx.indexFileId, realDrive);
      assertEqual(db2.lookUpByKey("newKey", { dbMain: "USERS" }).v, 2, "F3: new key resolves");
      assertNull(db2.lookUpByKey("oldKey", { dbMain: "USERS" }), "F3: old key must be gone");
      db2.destroyDB({ dbMain: "USERS" });
    });

    run("F4: lookupByCriteria criteria scan works on real Drive data", function () {
      var ctx = makeAssertDB(realDrive, emptyAssertIndex("f4"));
      indexIdsToTrash.push(ctx.indexFileId);
      var db = ctx.db;
      db.addToDB({ key: "a", id: 1, email: "alice@x.com" }, { dbMain: "USERS" });
      db.addToDB({ key: "b", id: 2 /* no email */ },          { dbMain: "USERS" });
      db.addToDB({ key: "c", id: 3, email: "charlie@x.com" }, { dbMain: "USERS" });

      var rows = db.lookupByCriteria(
        [{ param: "email", criterion: "alice@x.com" }],
        { dbMain: "USERS" }
      );
      assertEqual(rows.length, 1, "F4: only alice should match");
      assertEqual(rows[0].key, "a", "F4: matched row key");
      db.destroyDB({ dbMain: "USERS" });
    });

    run("F5: clearDB then addToDB produces a consistent DB", function () {
      var ctx = makeAssertDB(realDrive, emptyAssertIndex("f5"));
      indexIdsToTrash.push(ctx.indexFileId);
      var db = ctx.db;
      db.addToDB({ key: "a", id: 1, v: 1 }, { dbMain: "USERS" });
      db.addToDB({ key: "b", id: 2, v: 2 }, { dbMain: "USERS" });
      db.clearDB({ dbMain: "USERS" });

      db.addToDB({ key: "c", id: 3, v: 3 }, { dbMain: "USERS" });
      db.addToDB({ key: "a", id: 1, v: 10 }, { dbMain: "USERS" }); // reuse

      assertEqual(db.lookUpById(3, { dbMain: "USERS" }).v, 3, "F5: new row present");
      assertEqual(db.lookUpByKey("a", { dbMain: "USERS" }).v, 10, "F5: reused key resolved");
      assertNull(db.lookUpByKey("b", { dbMain: "USERS" }), "F5: cleared key absent");

      var v = db.validateRoutingConsistency({ dbMain: "USERS" });
      assert(v.ok, "F5: consistency after clearDB+add: " + v.errors.join("; "));
      db.destroyDB({ dbMain: "USERS" });
    });

    run("F6: addExternalConfig + getExternalConfig round-trips on real Drive", function () {
      var ctx = makeAssertDB(realDrive, emptyAssertIndex("f6"));
      indexIdsToTrash.push(ctx.indexFileId);
      var db = ctx.db;
      db.addToDB({ key: "x", id: 1, v: 1 }, { dbMain: "USERS" });
      db.saveToDBFiles();
      db.addExternalConfig("ttl", 60, { dbMain: "USERS", dbFragment: "USERS_1" });
      db.saveIndex();

      var db2 = SHARD_DB.init(ctx.indexFileId, realDrive);
      assertEqual(db2.getExternalConfig("ttl", { dbMain: "USERS", dbFragment: "USERS_1" }), 60,
        "F6: externalConfig must survive INDEX persist + reload");
      db2.destroyDB({ dbMain: "USERS" });
    });

    // ── G. wrapWithBackupRestore ─────────────────────────────────────────────

    run("G1: writeToJSON mirrors every write to the backup file", function () {
      var baseAdapter = getRealDriveAdapter();
      var backupAdapter = SHARD_DB_TOOLKIT.wrapWithBackupRestore(
        SHARD_DB_TOOLKIT.createDriveToolkitAdapter(), { enabled: true }
      );
      var folder = DriveApp.getFolderById(TEST_FOLDER_ID);
      var name = "ASSERT_INDEX_G1_" + Date.now() + ".json";
      var indexId = folder.createFile(name, JSON.stringify(emptyAssertIndex("g1")), "application/json").getId();
      indexIdsToTrash.push(indexId);

      var db = SHARD_DB.init(indexId, backupAdapter);
      db.addToDB({ key: "r1", id: 1, v: "backed" }, { dbMain: "USERS" });
      db.saveToDBFiles();

      // Read the fragment file directly via base adapter and confirm data is there
      var frag = db.INDEX.USERS.dbFragments.USERS_1;
      assertNotNull(frag, "G1: USERS_1 fragment should exist");
      assert(frag.fileId !== "", "G1: fragment fileId set");

      // Confirm backup file exists (named <fragName>.backup.json in same folder)
      var fragFile = DriveApp.getFileById(frag.fileId);
      var backupName = fragFile.getName().replace(/\.json$/i, ".backup.json");
      var backupIt = folder.getFilesByName(backupName);
      assert(backupIt.hasNext(), "G1: backup fragment file should have been created");
      var backupContent = JSON.parse(backupIt.next().getBlob().getDataAsString());
      assertNotNull(backupContent.data, "G1: backup should contain a data object");
      assertEqual(backupContent.data[1].v, "backed", "G1: backup data should match primary");
      db.destroyDB({ dbMain: "USERS" });
    });

    run("G2: readFromJSON falls back to backup when primary returns null", function () {
      var driveAdapter = SHARD_DB_TOOLKIT.createDriveToolkitAdapter();
      var backupAdapter = SHARD_DB_TOOLKIT.wrapWithBackupRestore(driveAdapter, { enabled: true });
      var folder = DriveApp.getFolderById(TEST_FOLDER_ID);
      var name = "ASSERT_INDEX_G2_" + Date.now() + ".json";
      var indexId = folder.createFile(name, JSON.stringify(emptyAssertIndex("g2")), "application/json").getId();
      indexIdsToTrash.push(indexId);

      var db = SHARD_DB.init(indexId, backupAdapter);
      db.addToDB({ key: "safe", id: 1, v: "restore-me" }, { dbMain: "USERS" });
      db.saveToDBFiles();

      var fragFileId = db.INDEX.USERS.dbFragments.USERS_1.fileId;
      assert(fragFileId !== "", "G2: fragment fileId must be set before corruption test");

      // Corrupt the primary fragment (overwrite with invalid JSON via DriveApp directly)
      DriveApp.getFileById(fragFileId).setContent("CORRUPTED_NOT_JSON");

      // A new init with backupAdapter should fall back to the backup copy
      var db2 = SHARD_DB.init(indexId, backupAdapter);
      var row = db2.lookUpById(1, { dbMain: "USERS" });
      assertNotNull(row, "G2: row should be restored from backup after primary corruption");
      assertEqual(row.v, "restore-me", "G2: restored row value must match original");
      db2.destroyDB({ dbMain: "USERS" });
    });

    // ── H. Nominal-ops matrix — correctness + timing + size at multiple scales ─
    //
    // One sequential flow per scale. "Nominal" means the batch size per phase
    // equals the number of shards (fragments) at that N, so every shard is
    // touched exactly once — reflecting the worst-case real-world operation cost
    // without being a synthetic stress test.
    //
    // Phases per scale:
    //   1. SEED N rows (timed)
    //      → validateRoutingConsistency + idRangesSorted valid
    //   2. SAVE + RELOAD (timed)
    //      → full correctness: every row by-id AND by-key
    //   3. NOMINAL IN-PLACE UPDATES — one per shard (timed)
    //      → every updated row: new value by-id and by-key
    //   4. NOMINAL KEY-CHANGE UPDATES — one per shard (timed)
    //      → new key resolves; old key null; validateRoutingConsistency
    //   5. NOMINAL DELETES BY ID — one per shard (timed)
    //      → each deleted id null by-id and by-key; idRangesSorted valid
    //   6. NOMINAL DELETES BY KEY — one per shard (timed)
    //      → each deleted key null; by-id null
    //   7. FINAL SAVE + RELOAD (timed)
    //      → validateRoutingConsistency; one survivor per shard intact;
    //        all deleted rows still absent
    //
    // Report: per-scale CSV row + full plain-text summary at the end.
    //
    // Scales: 100 / 1 000 / 10 000 / 50 000 / 100 000
    // Batch size per scale = fragmentsCount after seed (≈ ceil(N / MAX_ENTRIES))

    var PERF_SIZES = [100, 1000, 10000, 50000, 100000];
    var folder = DriveApp.getFolderById(TEST_FOLDER_ID);

    Logger.log(
      "\n── Group H: nominal-ops matrix — scales " + PERF_SIZES.join(", ") + " ──\n" +
      "Batch per phase = number of shards (one op per shard).\n" +
      "Correctness checked after every phase. Timing reported per phase.\n"
    );
    Logger.log(
      "csv_header,N,shards,index_bytes,keys," +
      "seed_ms,save_ms,reload_ms," +
      "upd_inplace_ms,upd_keychange_ms," +
      "del_by_id_ms,del_by_key_ms," +
      "final_save_ms,final_reload_ms," +
      "routing_ok"
    );

    var hReportRows = []; // collected for plain-text summary

    for (var hi = 0; hi < PERF_SIZES.length; hi++) {
      (function (hi) {
        var n = PERF_SIZES[hi];
        var label = "H" + (hi + 1) + "(n=" + n + ")";

        run(label + ": 7-phase nominal-ops flow", function () {

          // ── create ephemeral index file ──────────────────────────────────
          var perfIndexId = folder.createFile(
            "ASSERT_H_" + n + "_" + Date.now() + ".json",
            JSON.stringify({ USERS: emptyDbMain("h") }),
            "application/json"
          ).getId();
          indexIdsToTrash.push(perfIndexId);

          var db = SHARD_DB.init(perfIndexId, realDrive);
          var ctx = { dbMain: "USERS" };

          // ── helper: assert idRangesSorted is disjoint ────────────────────
          function assertRanges(tag) {
            var arr = db.INDEX.USERS.properties.idRangesSorted;
            for (var ri = 1; ri < arr.length; ri++) {
              assert(arr[ri].min > arr[ri - 1].max,
                label + " " + tag + ": idRangesSorted not sorted/disjoint at index " + ri);
            }
          }

          // ── Phase 1: SEED ────────────────────────────────────────────────
          var tSeed = timed(label + " P1 seed " + n + " rows", function () {
            for (var i = 1; i <= n; i++) {
              db.addToDB({
                key: "k_" + i,
                id: i,
                value: "v_" + i,
                profile: {
                  status: i % 2 === 0 ? "Active" : "Archived",
                  metrics: [
                    { clearance: "L1", score: i % 50 },
                    { clearance: i > n / 2 ? "L2" : "L1", score: (i + 1) % 50 }
                  ]
                }
              }, ctx);
            }
          });

          var vrSeed = db.validateRoutingConsistency({ dbMain: "USERS" });
          assert(vrSeed.ok, label + " P1: routing: " + vrSeed.errors.join("; "));
          assertRanges("P1");

          var fp = db.getIndexFootprint({ dbMain: "USERS" });
          var shardCount = fp.fragmentsCount;

          // ── Phase 2: SAVE + RELOAD ────────────────────────────────────────
          var tSave = timed(label + " P2 save", function () {
            db.saveToDBFiles();
          });

          var tReload = timed(label + " P2 reload", function () {
            db = SHARD_DB.init(perfIndexId, realDrive);
          });

          // Full correctness: every row by-id AND by-key
          var tReadAll = timed(label + " P2 read all " + n + " rows (by-id + by-key)", function () {
            for (var i = 1; i <= n; i++) {
              var byId  = db.lookUpById(i, ctx);
              var byKey = db.lookUpByKey("k_" + i, ctx);
              assertNotNull(byId,  label + " P2: lookUpById("  + i + ") null after reload");
              assertNotNull(byKey, label + " P2: lookUpByKey(k_" + i + ") null after reload");
              assertEqual(byId.value,  "v_" + i, label + " P2: by-id value mismatch id="  + i);
              assertEqual(byKey.value, "v_" + i, label + " P2: by-key value mismatch id=" + i);
            }
          });

          var vrReload = db.validateRoutingConsistency({ dbMain: "USERS" });
          assert(vrReload.ok, label + " P2: routing after reload: " + vrReload.errors.join("; "));

          // ── determine nominal batch: one row per shard ────────────────────
          // Pick the first id in each shard's idRange as the target row.
          var ranges       = db.INDEX.USERS.properties.idRangesSorted;
          var nominalIds   = [];
          for (var ri = 0; ri < ranges.length; ri++) {
            nominalIds.push(ranges[ri].min);
          }
          var batchSize = nominalIds.length; // == shardCount

          // ── Phase 3: NOMINAL IN-PLACE UPDATES ────────────────────────────
          var tUpdInplace = timed(label + " P3 in-place update x" + batchSize, function () {
            for (var bi = 0; bi < nominalIds.length; bi++) {
              var id = nominalIds[bi];
              db.addToDB({ key: "k_" + id, id: id, value: "upd_" + id }, ctx);
            }
          });

          for (var bi = 0; bi < nominalIds.length; bi++) {
            var id = nominalIds[bi];
            var byId  = db.lookUpById(id, ctx);
            var byKey = db.lookUpByKey("k_" + id, ctx);
            assertNotNull(byId,  label + " P3: lookUpById("   + id + ") null after update");
            assertNotNull(byKey, label + " P3: lookUpByKey(k_" + id + ") null after update");
            assertEqual(byId.value,  "upd_" + id, label + " P3: by-id value not updated for id="  + id);
            assertEqual(byKey.value, "upd_" + id, label + " P3: by-key value not updated for id=" + id);
          }

          // ── Phase 4: NOMINAL KEY-CHANGE UPDATES ───────────────────────────
          // Use the second id in each shard (avoid overlap with phase 3 targets).
          var kcIds = [];
          for (var ri = 0; ri < ranges.length; ri++) {
            var candidate = ranges[ri].min + 1;
            if (candidate <= ranges[ri].max) kcIds.push(candidate);
            else kcIds.push(ranges[ri].min); // single-row shard: reuse min (safe, key still changes)
          }

          var tUpdKc = timed(label + " P4 key-change update x" + kcIds.length, function () {
            for (var bi = 0; bi < kcIds.length; bi++) {
              var id = kcIds[bi];
              db.addToDB({ key: "kc_" + id, id: id, value: "kc_val_" + id }, ctx);
            }
          });

          for (var bi = 0; bi < kcIds.length; bi++) {
            var id = kcIds[bi];
            var byNewKey = db.lookUpByKey("kc_" + id, ctx);
            var byOldKey = db.lookUpByKey("k_"  + id, ctx);
            assertNotNull(byNewKey, label + " P4: new key kc_" + id + " must resolve");
            assertNull(byOldKey,    label + " P4: old key k_"  + id + " must be gone");
            assertEqual(
              db.INDEX.USERS.properties.keyToFragment["k_" + id],
              undefined,
              label + " P4: keyToFragment must not have old key k_" + id
            );
          }

          var vrKc = db.validateRoutingConsistency({ dbMain: "USERS" });
          assert(vrKc.ok, label + " P4: routing after key-change: " + vrKc.errors.join("; "));

          // ── Phase 5: NOMINAL DELETES BY ID ───────────────────────────────
          // Use third id in each shard (avoid phases 3+4 targets).
          var delByIdIds = [];
          for (var ri = 0; ri < ranges.length; ri++) {
            var candidate = ranges[ri].min + 2;
            if (candidate <= ranges[ri].max &&
                nominalIds.indexOf(candidate) === -1 &&
                kcIds.indexOf(candidate) === -1) {
              delByIdIds.push(candidate);
            } else {
              // find first unused id in range
              for (var cand = ranges[ri].min; cand <= ranges[ri].max; cand++) {
                if (nominalIds.indexOf(cand) === -1 && kcIds.indexOf(cand) === -1) {
                  delByIdIds.push(cand);
                  break;
                }
              }
            }
          }

          var tDelById = timed(label + " P5 delete-by-id x" + delByIdIds.length, function () {
            for (var bi = 0; bi < delByIdIds.length; bi++) {
              db.deleteFromDBById(delByIdIds[bi], ctx);
            }
          });

          for (var bi = 0; bi < delByIdIds.length; bi++) {
            var id = delByIdIds[bi];
            assertNull(db.lookUpById(id, ctx),       label + " P5: lookUpById("   + id + ") must be null");
            assertNull(db.lookUpByKey("k_" + id, ctx), label + " P5: lookUpByKey(k_" + id + ") must be null");
          }

          assertRanges("P5");
          var vrDel = db.validateRoutingConsistency({ dbMain: "USERS" });
          assert(vrDel.ok, label + " P5: routing after delete-by-id: " + vrDel.errors.join("; "));

          // ── Phase 6: NOMINAL DELETES BY KEY ──────────────────────────────
          // Use fourth id in each shard; these rows still have original key k_<id>.
          var allUsed = nominalIds.concat(kcIds).concat(delByIdIds);
          var delByKeyIds = [];
          for (var ri = 0; ri < ranges.length; ri++) {
            for (var cand = ranges[ri].min; cand <= ranges[ri].max; cand++) {
              if (allUsed.indexOf(cand) === -1) {
                delByKeyIds.push(cand);
                break;
              }
            }
          }

          var tDelByKey = timed(label + " P6 delete-by-key x" + delByKeyIds.length, function () {
            for (var bi = 0; bi < delByKeyIds.length; bi++) {
              db.deleteFromDBByKey("k_" + delByKeyIds[bi], ctx);
            }
          });

          for (var bi = 0; bi < delByKeyIds.length; bi++) {
            var id = delByKeyIds[bi];
            assertNull(db.lookUpByKey("k_" + id, ctx), label + " P6: key k_" + id + " must be null");
            assertNull(db.lookUpById(id, ctx),          label + " P6: id " + id + " must be null after key-delete");
          }

          assertRanges("P6");

          // criteria scan: count Active rows after all mutations
          var tCrit = timed(label + " P6 criteria scan (Active status)", function () {
            return db.lookupByCriteria(
              [{ path: ["profile"], param: "status", criterion: "Active" }],
              ctx
            );
          });
          // just assert it doesn't throw and returns an array (count varies by mutations)
          assert(Array.isArray(tCrit.result), label + " P6: criteria scan must return array");

          // ── Phase 7: FINAL SAVE + RELOAD ─────────────────────────────────
          var tFinalSave = timed(label + " P7 final save", function () {
            db.saveToDBFiles();
          });

          var tFinalReload = timed(label + " P7 final reload", function () {
            db = SHARD_DB.init(perfIndexId, realDrive);
          });

          var vrFinal = db.validateRoutingConsistency({ dbMain: "USERS" });
          assert(vrFinal.ok, label + " P7: routing after final reload: " + vrFinal.errors.join("; "));
          assertRanges("P7");

          // Spot-check: one untouched survivor per shard (find an id not in allUsed+delByKeyIds)
          var allMutated = allUsed.concat(delByKeyIds);
          var finalRanges = db.INDEX.USERS.properties.idRangesSorted;
          for (var ri = 0; ri < finalRanges.length; ri++) {
            for (var cand = finalRanges[ri].min; cand <= finalRanges[ri].max; cand++) {
              if (allMutated.indexOf(cand) === -1) {
                var survivor = db.lookUpById(cand, ctx);
                assertNotNull(survivor, label + " P7: survivor id=" + cand + " must exist after reload");
                assertEqual(survivor.value, "v_" + cand, label + " P7: survivor value mismatch id=" + cand);
                break;
              }
            }
          }

          // Deleted rows still absent after reload
          for (var bi = 0; bi < delByIdIds.length; bi++) {
            assertNull(db.lookUpById(delByIdIds[bi], ctx),
              label + " P7: deleted-by-id id=" + delByIdIds[bi] + " must still be null after reload");
          }
          for (var bi = 0; bi < delByKeyIds.length; bi++) {
            assertNull(db.lookUpByKey("k_" + delByKeyIds[bi], ctx),
              label + " P7: deleted-by-key k_" + delByKeyIds[bi] + " must still be null after reload");
          }

          // ── Final footprint ───────────────────────────────────────────────
          var fp2   = db.getIndexFootprint({ dbMain: "USERS" });
          var routeOk = vrFinal.ok ? 1 : 0;

          Logger.log(
            "csv_row," + n +
            "," + shardCount +
            "," + fp2.indexJsonBytes +
            "," + fp2.keyToFragmentCount +
            "," + tSeed.ms +
            "," + tSave.ms +
            "," + tReload.ms +
            "," + tReadAll.ms +
            "," + tUpdInplace.ms +
            "," + tUpdKc.ms +
            "," + tDelById.ms +
            "," + tDelByKey.ms +
            "," + tFinalSave.ms +
            "," + tFinalReload.ms +
            "," + routeOk
          );

          hReportRows.push({
            n: n,
            shards: shardCount,
            index_bytes: fp2.indexJsonBytes,
            keys_after: fp2.keyToFragmentCount,
            batch: batchSize,
            seed_ms: tSeed.ms,
            save_ms: tSave.ms,
            reload_ms: tReload.ms,
            read_all_ms: tReadAll.ms,
            upd_inplace_ms: tUpdInplace.ms,
            upd_kc_ms: tUpdKc.ms,
            del_by_id_ms: tDelById.ms,
            del_by_key_ms: tDelByKey.ms,
            final_save_ms: tFinalSave.ms,
            final_reload_ms: tFinalReload.ms,
            routing_ok: routeOk
          });

          db.destroyDB({ dbMain: "USERS" });
        });
      })(hi);
    }

    // ── H plain-text summary ──────────────────────────────────────────────────
    Logger.log("\n══════════════════════════════════════════════════════════════");
    Logger.log("GROUP H SUMMARY — nominal-ops timing + correctness");
    Logger.log("Batch size per phase = number of shards (one op per shard).");
    Logger.log("All correctness checks passed for every scale above.");
    Logger.log("══════════════════════════════════════════════════════════════");
    Logger.log(
      "N         | shards | batch | index_bytes | keys_left" +
      " | seed_ms | save_ms | reload_ms | read_all_ms" +
      " | upd_ip_ms | upd_kc_ms | del_id_ms | del_key_ms" +
      " | fsave_ms | freload_ms | route"
    );
    Logger.log("─────────────────────────────────────────────────────────────────────────────────────────────────────────");
    for (var ri = 0; ri < hReportRows.length; ri++) {
      var r = hReportRows[ri];
      Logger.log(
        pad(r.n, 9) + " | " +
        pad(r.shards, 6) + " | " +
        pad(r.batch, 5) + " | " +
        pad(r.index_bytes, 11) + " | " +
        pad(r.keys_after, 9) + " | " +
        pad(r.seed_ms, 7) + " | " +
        pad(r.save_ms, 7) + " | " +
        pad(r.reload_ms, 9) + " | " +
        pad(r.read_all_ms, 11) + " | " +
        pad(r.upd_inplace_ms, 9) + " | " +
        pad(r.upd_kc_ms, 9) + " | " +
        pad(r.del_by_id_ms, 9) + " | " +
        pad(r.del_by_key_ms, 9) + " | " +
        pad(r.final_save_ms, 8) + " | " +
        pad(r.final_reload_ms, 10) + " | " +
        (r.routing_ok ? "OK" : "FAIL")
      );
    }
    Logger.log("══════════════════════════════════════════════════════════════\n");

    // ── I. Partition routing — real Drive verification ────────────────────────
    //
    // These tests confirm behaviour that Node tests cannot simulate:
    // actual Drive file creation, naming, isolation, and cold-reload routing.
    //
    // Partition setup used throughout:
    //   Table:      REGISTRATIONS  (partitioned by entry.eventId)
    //   Table:      ORDERS         (plain cumulative, isolation control)
    //   filesPrefix for REGISTRATIONS: "reg"
    //   Fragment name convention: reg_REGISTRATIONS_p_<eventId>.json
    //
    // PARTITION_BY is supplied to every SHARD_DB.init call as options.partitionBy.

    var PART_BY = { REGISTRATIONS: function (entry) { return entry.eventId; } };

    function emptyPartIndex(regPrefix, ordPrefix) {
      return {
        REGISTRATIONS: {
          properties: {
            cumulative: false,
            rootFolder: TEST_FOLDER_ID,
            filesPrefix: regPrefix || "reg",
            fragmentsList: [],
            keyToFragment: {},
            idRangesSorted: []
          },
          dbFragments: {}
        },
        ORDERS: emptyDbMain(ordPrefix || "ord")
      };
    }

    /** Re-init with partitionBy so we get the right routing after a reload. */
    function reloadPartDB(indexFileId, maxEntries) {
      var opts = { partitionBy: PART_BY };
      if (maxEntries != null) opts.maxEntriesCount = maxEntries;
      return SHARD_DB.init(indexFileId, realDrive, opts);
    }

    /** Build a registration row. */
    function partReg(id, eventId, extra) {
      var row = { id: id, key: "reg_" + id, eventId: eventId };
      if (extra) Object.keys(extra).forEach(function (k) { row[k] = extra[k]; });
      return row;
    }

    /** Expected base fragment name for an event. */
    function partFragName(eventId) {
      return "REGISTRATIONS_p_" + eventId;
    }

    Logger.log("\n── Group I: partition routing — real Drive ──\n");

    // ── I1: setupPartitions creates real Drive files with correct names ───────

    run("I1: setupPartitions creates Drive files with correct naming convention", function () {
      var ctx = makeAssertDB(realDrive, emptyPartIndex("i1reg", "i1ord"), { partitionBy: PART_BY });
      indexIdsToTrash.push(ctx.indexFileId);
      var db = ctx.db;

      var created = db.setupPartitions("REGISTRATIONS", ["EVT_A", "EVT_B", "EVT_C"]);
      db.saveIndex();

      // Drive files don't exist yet — they're created lazily on first save.
      // Add one row per partition and save so fragments get real fileIds.
      db.addToDB(partReg(1, "EVT_A"), { dbMain: "REGISTRATIONS" });
      db.addToDB(partReg(2, "EVT_B"), { dbMain: "REGISTRATIONS" });
      db.addToDB(partReg(3, "EVT_C"), { dbMain: "REGISTRATIONS" });
      db.saveToDBFiles();

      // Verify that each fragment now has a non-empty fileId in the INDEX.
      var frags = db.INDEX.REGISTRATIONS.dbFragments;
      ["EVT_A", "EVT_B", "EVT_C"].forEach(function (evt) {
        var fragName = partFragName(evt);
        assertNotNull(frags[fragName], "I1: fragment " + fragName + " missing from INDEX");
        assert(frags[fragName].fileId !== "", "I1: fileId empty for fragment " + fragName);
      });

      // Verify Drive files are named according to the convention:
      // createJSON uses  filesPrefix + "_" + dbFragment  so the filename is
      // "i1reg_REGISTRATIONS_p_EVT_A.json" etc.
      ["EVT_A", "EVT_B", "EVT_C"].forEach(function (evt) {
        var fragName  = partFragName(evt);
        var fileId    = frags[fragName].fileId;
        var driveFile = DriveApp.getFileById(fileId);
        var fileName  = driveFile.getName();
        assert(
          fileName.indexOf("REGISTRATIONS_p_" + evt) !== -1,
          "I1: Drive file name should contain REGISTRATIONS_p_" + evt + " — got: " + fileName
        );
      });

      db.destroyDB({ dbMain: "REGISTRATIONS" });
    });

    // ── I2: Fragment name is deterministic across GAS executions ─────────────

    run("I2: partition fragment name is deterministic — same eventId maps to same Drive file after reload", function () {
      var ctx = makeAssertDB(realDrive, emptyPartIndex("i2reg"), { partitionBy: PART_BY });
      indexIdsToTrash.push(ctx.indexFileId);
      var db = ctx.db;

      db.addToDB(partReg(1, "EVT_STABLE"), { dbMain: "REGISTRATIONS" });
      db.saveToDBFiles();

      var fileIdBefore = db.INDEX.REGISTRATIONS.dbFragments[partFragName("EVT_STABLE")].fileId;
      assert(fileIdBefore !== "", "I2: fileId must be set after save");

      // Simulate a new GAS execution by creating a completely fresh db instance.
      var db2 = reloadPartDB(ctx.indexFileId);
      var fileIdAfter = db2.INDEX.REGISTRATIONS.dbFragments[partFragName("EVT_STABLE")].fileId;

      assertEqual(fileIdAfter, fileIdBefore, "I2: fileId must be identical after cold reload");

      // Adding another row to the same event must go to the same file, not create a new one.
      db2.addToDB(partReg(2, "EVT_STABLE"), { dbMain: "REGISTRATIONS" });
      db2.saveToDBFiles();

      var fileIdFinal = db2.INDEX.REGISTRATIONS.dbFragments[partFragName("EVT_STABLE")].fileId;
      assertEqual(fileIdFinal, fileIdBefore, "I2: second write must still use the same Drive file");

      db2.destroyDB({ dbMain: "REGISTRATIONS" });
    });

    // ── I3: Cold reload — all rows routable by key and id ────────────────────

    run("I3: cold reload — partition routing resolves all rows correctly after new GAS execution", function () {
      var ctx = makeAssertDB(realDrive, emptyPartIndex("i3reg"), { partitionBy: PART_BY });
      indexIdsToTrash.push(ctx.indexFileId);
      var db = ctx.db;

      // Two partitions, 5 rows each.
      for (var i = 1; i <= 5; i++) {
        db.addToDB(partReg(i,     "EVT_X", { score: i }),      { dbMain: "REGISTRATIONS" });
        db.addToDB(partReg(i + 5, "EVT_Y", { score: i + 5 }), { dbMain: "REGISTRATIONS" });
      }
      db.saveToDBFiles();

      var db2 = reloadPartDB(ctx.indexFileId);

      // All 10 rows must be reachable by id.
      for (var i = 1; i <= 10; i++) {
        var row = db2.lookUpById(i, { dbMain: "REGISTRATIONS" });
        assertNotNull(row, "I3: lookUpById(" + i + ") null after cold reload");
        assertEqual(row.score, i, "I3: score mismatch id=" + i);
      }

      // Direct partition lookup must also work.
      var rowP = db2.lookUpById(3, { dbMain: "REGISTRATIONS", partitionKey: "EVT_X" });
      assertNotNull(rowP, "I3: partitionKey lookup null for id=3");
      assertEqual(rowP.score, 3, "I3: partitionKey lookup score mismatch");

      var vrResult = db2.validateRoutingConsistency({ dbMain: "REGISTRATIONS" });
      assert(vrResult.ok, "I3: routing after reload: " + vrResult.errors.join("; "));

      db2.destroyDB({ dbMain: "REGISTRATIONS" });
    });

    // ── I4: Drive I/O isolation — lookupByCriteria with partitionKey ──────────
    // We verify this by checking which fragments were opened in OPEN_DB after
    // the lookup — only the target partition's fragments should be present.

    run("I4: lookupByCriteria with partitionKey only opens target partition's Drive files", function () {
      var ctx = makeAssertDB(realDrive, emptyPartIndex("i4reg"), { partitionBy: PART_BY });
      indexIdsToTrash.push(ctx.indexFileId);
      var db = ctx.db;

      db.addToDB(partReg(1, "EVT_TARGET", { tier: "gold" }),   { dbMain: "REGISTRATIONS" });
      db.addToDB(partReg(2, "EVT_TARGET", { tier: "silver" }), { dbMain: "REGISTRATIONS" });
      db.addToDB(partReg(3, "EVT_OTHER",  { tier: "gold" }),   { dbMain: "REGISTRATIONS" });
      db.saveToDBFiles();

      // Cold reload so OPEN_DB is empty — fragment files haven't been read yet.
      var db2 = reloadPartDB(ctx.indexFileId);
      assertEqual(Object.keys(db2.OPEN_DB).length, 0, "I4: OPEN_DB must be empty after cold reload");

      // Query only EVT_TARGET.
      var results = db2.lookupByCriteria(
        [{ param: "tier", criterion: "gold" }],
        { dbMain: "REGISTRATIONS", partitionKey: "EVT_TARGET" }
      );

      // Only one gold row exists in EVT_TARGET.
      assertEqual(results.length, 1, "I4: should find exactly 1 gold row in EVT_TARGET");
      assertEqual(results[0].eventId, "EVT_TARGET", "I4: result must be from EVT_TARGET");

      // Verify that OPEN_DB only contains EVT_TARGET's fragment — not EVT_OTHER.
      var openKeys = Object.keys(db2.OPEN_DB);
      var targetFrag = partFragName("EVT_TARGET");
      var otherFrag  = partFragName("EVT_OTHER");
      assert(
        openKeys.some(function (k) { return k.indexOf(targetFrag) !== -1; }),
        "I4: target fragment must be in OPEN_DB"
      );
      assert(
        !openKeys.some(function (k) { return k.indexOf(otherFrag) !== -1; }),
        "I4: other partition fragment must NOT be in OPEN_DB"
      );

      db2.destroyDB({ dbMain: "REGISTRATIONS" });
    });

    // ── I5: Overflow — two Drive files created for one partition ──────────────

    run("I5: overflow creates a second Drive file for the same partition", function () {
      var folder = DriveApp.getFolderById(TEST_FOLDER_ID);
      var name = "ASSERT_I5_" + Date.now() + ".json";
      var indexId = folder.createFile(name, JSON.stringify(emptyPartIndex("i5reg")), "application/json").getId();
      indexIdsToTrash.push(indexId);
      var db = SHARD_DB.init(indexId, realDrive, { partitionBy: PART_BY, maxEntriesCount: 3 });

      // 4 rows in the same partition → must overflow to a second shard.
      for (var i = 1; i <= 4; i++) {
        db.addToDB(partReg(i, "EVT_OVERFLOW"), { dbMain: "REGISTRATIONS" });
      }
      db.saveToDBFiles();

      var frags = db.INDEX.REGISTRATIONS.dbFragments;
      var baseFrag     = partFragName("EVT_OVERFLOW");
      var overflowFrag = baseFrag + "_2";

      assertNotNull(frags[baseFrag],     "I5: base fragment must exist in INDEX");
      assertNotNull(frags[overflowFrag], "I5: overflow fragment _2 must exist in INDEX");
      assert(frags[baseFrag].fileId     !== "", "I5: base fragment fileId must be set");
      assert(frags[overflowFrag].fileId !== "", "I5: overflow fragment fileId must be set");

      // Both Drive files must actually exist and be distinct.
      var baseFile     = DriveApp.getFileById(frags[baseFrag].fileId);
      var overflowFile = DriveApp.getFileById(frags[overflowFrag].fileId);
      assertNotNull(baseFile,     "I5: base Drive file must exist");
      assertNotNull(overflowFile, "I5: overflow Drive file must exist");
      assert(baseFile.getId() !== overflowFile.getId(), "I5: base and overflow must be different Drive files");

      // Cold reload — all 4 rows reachable.
      var db2 = reloadPartDB(indexId, 3);
      for (var i = 1; i <= 4; i++) {
        assertNotNull(db2.lookUpById(i, { dbMain: "REGISTRATIONS" }), "I5: row id=" + i + " missing after reload");
      }

      db2.destroyDB({ dbMain: "REGISTRATIONS" });
    });

    // ── I6: lookupByCriteria with partitionKey spans base + overflow Drive files ─

    run("I6: lookupByCriteria with partitionKey returns rows from base AND overflow Drive files", function () {
      var folder = DriveApp.getFolderById(TEST_FOLDER_ID);
      var name = "ASSERT_I6_" + Date.now() + ".json";
      var indexId = folder.createFile(name, JSON.stringify(emptyPartIndex("i6reg")), "application/json").getId();
      indexIdsToTrash.push(indexId);
      var db = SHARD_DB.init(indexId, realDrive, { partitionBy: PART_BY, maxEntriesCount: 3 });

      // 7 rows in EVT_SPAN → base(3) + overflow_2(3) + overflow_3(1)
      for (var i = 1; i <= 7; i++) {
        db.addToDB(partReg(i, "EVT_SPAN", { tier: i % 2 === 0 ? "gold" : "silver" }), { dbMain: "REGISTRATIONS" });
      }
      // 2 rows in a different partition — must NOT appear in results.
      db.addToDB(partReg(100, "EVT_OTHER", { tier: "gold" }), { dbMain: "REGISTRATIONS" });
      db.addToDB(partReg(101, "EVT_OTHER", { tier: "gold" }), { dbMain: "REGISTRATIONS" });
      db.saveToDBFiles();

      var db2 = reloadPartDB(indexId, 3);

      // All 7 EVT_SPAN rows, no EVT_OTHER rows.
      var all = db2.lookupByCriteria([], { dbMain: "REGISTRATIONS", partitionKey: "EVT_SPAN" });
      assertEqual(all.length, 7, "I6: should return all 7 EVT_SPAN rows across overflow shards");
      all.forEach(function (r) {
        assertEqual(r.eventId, "EVT_SPAN", "I6: all results must belong to EVT_SPAN");
      });

      // Criteria applied within the partition.
      var gold = db2.lookupByCriteria(
        [{ param: "tier", criterion: "gold" }],
        { dbMain: "REGISTRATIONS", partitionKey: "EVT_SPAN" }
      );
      assertEqual(gold.length, 3, "I6: should find exactly 3 gold rows in EVT_SPAN (ids 2,4,6)");

      var vr = db2.validateRoutingConsistency({ dbMain: "REGISTRATIONS" });
      assert(vr.ok, "I6: routing consistency: " + vr.errors.join("; "));

      db2.destroyDB({ dbMain: "REGISTRATIONS" });
    });

    // ── I7: Cross-partition isolation — Drive file integrity ─────────────────

    run("I7: deleting all rows from partition A does not corrupt partition B's Drive file", function () {
      var ctx = makeAssertDB(realDrive, emptyPartIndex("i7reg"), { partitionBy: PART_BY });
      indexIdsToTrash.push(ctx.indexFileId);
      var db = ctx.db;

      for (var i = 1; i <= 5; i++) {
        db.addToDB(partReg(i,     "EVT_DEL",  { v: i }),      { dbMain: "REGISTRATIONS" });
        db.addToDB(partReg(i + 5, "EVT_KEEP", { v: i + 5 }), { dbMain: "REGISTRATIONS" });
      }
      db.saveToDBFiles();

      // Delete all EVT_DEL rows.
      for (var i = 1; i <= 5; i++) {
        db.deleteFromDBById(i, { dbMain: "REGISTRATIONS" });
      }
      db.saveToDBFiles();

      var db2 = reloadPartDB(ctx.indexFileId);

      // EVT_DEL rows must be gone.
      for (var i = 1; i <= 5; i++) {
        assertNull(db2.lookUpById(i, { dbMain: "REGISTRATIONS" }), "I7: EVT_DEL id=" + i + " must be null");
      }

      // EVT_KEEP rows must be completely intact.
      for (var i = 1; i <= 5; i++) {
        var row = db2.lookUpById(i + 5, { dbMain: "REGISTRATIONS" });
        assertNotNull(row, "I7: EVT_KEEP id=" + (i + 5) + " must survive");
        assertEqual(row.v, i + 5, "I7: EVT_KEEP value mismatch id=" + (i + 5));
      }

      // Read the EVT_KEEP Drive file directly and verify it is valid JSON with data.
      var keepFrag   = partFragName("EVT_KEEP");
      var keepFileId = db2.INDEX.REGISTRATIONS.dbFragments[keepFrag].fileId;
      var keepRaw    = JSON.parse(DriveApp.getFileById(keepFileId).getBlob().getDataAsString());
      assert(keepRaw.data != null, "I7: EVT_KEEP Drive file must have a data object");
      assertEqual(Object.keys(keepRaw.data).length, 5, "I7: EVT_KEEP Drive file must have 5 rows");

      var vr = db2.validateRoutingConsistency({ dbMain: "REGISTRATIONS" });
      assert(vr.ok, "I7: routing after cross-partition delete: " + vr.errors.join("; "));

      db2.destroyDB({ dbMain: "REGISTRATIONS" });
    });

    // ── I8: Nominal-ops matrix — partition routing at 3 scales ───────────────
    //
    // Scales: 10 events × 20 rows,  50 events × 20 rows,  200 events × 20 rows.
    // Phases: seed → save → reload → in-place update → key-change → delete → final save/reload.
    // Correctness checked after every phase.  Timing reported per phase.
    // Also reports INDEX footprint vs equivalent cumulative DB.

    var PART_SCALES = [
      { events: 10,  rowsPerEvent: 20 },
      { events: 50,  rowsPerEvent: 20 },
      { events: 200, rowsPerEvent: 20 }
    ];

    Logger.log(
      "\n── I8: partition nominal-ops matrix — " +
      PART_SCALES.map(function (s) { return s.events + "×" + s.rowsPerEvent; }).join(", ") +
      " ──\n" +
      "Phases: seed → save → reload → in-place update → key-change → delete-by-id → delete-by-key → final save/reload.\n" +
      "Correctness verified after every phase.  validateRoutingConsistency checked at the end.\n"
    );
    Logger.log(
      "csv_part_header,events,rows_per_event,total_rows,frags," +
      "seed_ms,save_ms,reload_ms," +
      "upd_inplace_ms,upd_keychange_ms," +
      "del_by_id_ms,del_by_key_ms," +
      "final_save_ms,final_reload_ms," +
      "index_bytes,routing_ok"
    );

    var i8ReportRows = [];

    for (var si = 0; si < PART_SCALES.length; si++) {
      (function (si) {
        var scale   = PART_SCALES[si];
        var E       = scale.events;
        var RPE     = scale.rowsPerEvent;
        var total   = E * RPE;
        var label   = "I8_" + E + "x" + RPE;

        run(label + ": " + total + " rows across " + E + " partitions — 7-phase nominal-ops", function () {

          var folder = DriveApp.getFolderById(TEST_FOLDER_ID);
          var indexId = folder.createFile(
            "ASSERT_I8_" + E + "_" + Date.now() + ".json",
            JSON.stringify(emptyPartIndex("i8r")),
            "application/json"
          ).getId();
          indexIdsToTrash.push(indexId);

          var db = SHARD_DB.init(indexId, realDrive, { partitionBy: PART_BY });
          var ctx = { dbMain: "REGISTRATIONS" };

          // Build list of event IDs used in this scale.
          var eventIds = [];
          for (var e = 0; e < E; e++) {
            eventIds.push("E" + String(e + 1));
          }

          // ── Phase 1: SEED ────────────────────────────────────────────────
          var tSeed = timed(label + " P1 seed " + total + " rows", function () {
            var rowId = 1;
            for (var e = 0; e < E; e++) {
              for (var r = 0; r < RPE; r++) {
                db.addToDB(partReg(rowId, eventIds[e], {
                  score: r,
                  status: r % 2 === 0 ? "Active" : "Archived"
                }), ctx);
                rowId++;
              }
            }
          });

          var vrSeed = db.validateRoutingConsistency({ dbMain: "REGISTRATIONS" });
          assert(vrSeed.ok, label + " P1 routing: " + vrSeed.errors.join("; "));

          var fp = db.getIndexFootprint({ dbMain: "REGISTRATIONS" });
          Logger.log(label + " P1: frags=" + fp.fragmentsCount + " keys=" + fp.keyToFragmentCount + " indexBytes=" + fp.indexJsonBytes);

          // ── Phase 2: SAVE + RELOAD ────────────────────────────────────────
          var tSave = timed(label + " P2 save", function () {
            db.saveToDBFiles();
          });
          var tReload = timed(label + " P2 reload", function () {
            db = reloadPartDB(indexId);
          });

          // Spot-check: first and last row of each partition.
          var idCursor = 1;
          for (var e = 0; e < E; e++) {
            var firstId = idCursor;
            var lastId  = idCursor + RPE - 1;
            assertNotNull(
              db.lookUpById(firstId, { dbMain: "REGISTRATIONS", partitionKey: eventIds[e] }),
              label + " P2: first row of " + eventIds[e] + " (id=" + firstId + ") null after reload"
            );
            assertNotNull(
              db.lookUpById(lastId, { dbMain: "REGISTRATIONS", partitionKey: eventIds[e] }),
              label + " P2: last row of " + eventIds[e] + " (id=" + lastId + ") null after reload"
            );
            idCursor += RPE;
          }

          var vrReload = db.validateRoutingConsistency({ dbMain: "REGISTRATIONS" });
          assert(vrReload.ok, label + " P2 routing after reload: " + vrReload.errors.join("; "));

          // ── Phase 3: IN-PLACE UPDATE — first row of each partition ────────
          var updateTargets = []; // { id, eventId }
          idCursor = 1;
          for (var e = 0; e < E; e++) {
            updateTargets.push({ id: idCursor, eventId: eventIds[e] });
            idCursor += RPE;
          }

          var tUpdInplace = timed(label + " P3 in-place update x" + E, function () {
            for (var ui = 0; ui < updateTargets.length; ui++) {
              var t = updateTargets[ui];
              db.addToDB(partReg(t.id, t.eventId, { score: 9999, status: "Updated" }), ctx);
            }
          });

          for (var ui = 0; ui < updateTargets.length; ui++) {
            var t = updateTargets[ui];
            var row = db.lookUpById(t.id, { dbMain: "REGISTRATIONS", partitionKey: t.eventId });
            assertNotNull(row, label + " P3: row " + t.id + " null after in-place update");
            assertEqual(row.status, "Updated", label + " P3: status not updated for id=" + t.id);
          }

          // ── Phase 4: KEY-CHANGE — second row of each partition ────────────
          var kcTargets = [];
          idCursor = 2;
          for (var e = 0; e < E; e++) {
            kcTargets.push({ id: idCursor, eventId: eventIds[e] });
            idCursor += RPE;
          }

          var tUpdKc = timed(label + " P4 key-change x" + E, function () {
            for (var ki = 0; ki < kcTargets.length; ki++) {
              var t = kcTargets[ki];
              db.addToDB({ id: t.id, key: "kc_" + t.id, eventId: t.eventId, score: -1 }, ctx);
            }
          });

          for (var ki = 0; ki < kcTargets.length; ki++) {
            var t = kcTargets[ki];
            var newRow = db.lookUpByKey("kc_" + t.id, { dbMain: "REGISTRATIONS" });
            var oldRow = db.lookUpByKey("reg_" + t.id, { dbMain: "REGISTRATIONS" });
            assertNotNull(newRow, label + " P4: new key kc_" + t.id + " must resolve");
            assertNull(oldRow,    label + " P4: old key reg_" + t.id + " must be null");
          }

          var vrKc = db.validateRoutingConsistency({ dbMain: "REGISTRATIONS" });
          assert(vrKc.ok, label + " P4 routing: " + vrKc.errors.join("; "));

          // ── Phase 5: DELETE BY ID — third row of each partition ───────────
          var delIdTargets = [];
          idCursor = 3;
          for (var e = 0; e < E; e++) {
            delIdTargets.push({ id: idCursor, eventId: eventIds[e] });
            idCursor += RPE;
          }

          var tDelById = timed(label + " P5 delete-by-id x" + E, function () {
            for (var di = 0; di < delIdTargets.length; di++) {
              db.deleteFromDBById(delIdTargets[di].id, ctx);
            }
          });

          for (var di = 0; di < delIdTargets.length; di++) {
            assertNull(
              db.lookUpById(delIdTargets[di].id, ctx),
              label + " P5: deleted id=" + delIdTargets[di].id + " must be null"
            );
          }

          // ── Phase 6: DELETE BY KEY — fourth row of each partition ─────────
          var delKeyTargets = [];
          idCursor = 4;
          for (var e = 0; e < E; e++) {
            delKeyTargets.push({ id: idCursor, eventId: eventIds[e] });
            idCursor += RPE;
          }

          var tDelByKey = timed(label + " P6 delete-by-key x" + E, function () {
            for (var dk = 0; dk < delKeyTargets.length; dk++) {
              db.deleteFromDBByKey("reg_" + delKeyTargets[dk].id, ctx);
            }
          });

          for (var dk = 0; dk < delKeyTargets.length; dk++) {
            assertNull(
              db.lookUpByKey("reg_" + delKeyTargets[dk].id, ctx),
              label + " P6: deleted key reg_" + delKeyTargets[dk].id + " must be null"
            );
          }

          // ── Phase 7: FINAL SAVE + RELOAD ─────────────────────────────────
          var tFinalSave = timed(label + " P7 final save", function () {
            db.saveToDBFiles();
          });
          var tFinalReload = timed(label + " P7 final reload", function () {
            db = reloadPartDB(indexId);
          });

          var vrFinal = db.validateRoutingConsistency({ dbMain: "REGISTRATIONS" });
          assert(vrFinal.ok, label + " P7 routing: " + vrFinal.errors.join("; "));

          // Deleted rows must still be absent.
          for (var di = 0; di < delIdTargets.length; di++) {
            assertNull(
              db.lookUpById(delIdTargets[di].id, ctx),
              label + " P7: deleted-by-id id=" + delIdTargets[di].id + " must still be null"
            );
          }
          for (var dk = 0; dk < delKeyTargets.length; dk++) {
            assertNull(
              db.lookUpByKey("reg_" + delKeyTargets[dk].id, ctx),
              label + " P7: deleted-by-key reg_" + delKeyTargets[dk].id + " must still be null"
            );
          }

          // Untouched rows (row 5 in each event) must survive.
          idCursor = 5;
          for (var e = 0; e < E; e++) {
            var survivor = db.lookUpById(idCursor, { dbMain: "REGISTRATIONS", partitionKey: eventIds[e] });
            assertNotNull(survivor, label + " P7: survivor id=" + idCursor + " must exist");
            idCursor += RPE;
          }

          var fp2 = db.getIndexFootprint({ dbMain: "REGISTRATIONS" });
          Logger.log(
            "csv_part_row," + E + "," + RPE + "," + total + "," + fp2.fragmentsCount + "," +
            tSeed.ms + "," + tSave.ms + "," + tReload.ms + "," +
            tUpdInplace.ms + "," + tUpdKc.ms + "," +
            tDelById.ms + "," + tDelByKey.ms + "," +
            tFinalSave.ms + "," + tFinalReload.ms + "," +
            fp2.indexJsonBytes + "," + (vrFinal.ok ? 1 : 0)
          );

          i8ReportRows.push({
            events: E, rowsPerEvent: RPE, total: total,
            frags: fp2.fragmentsCount,
            seed_ms: tSeed.ms, save_ms: tSave.ms, reload_ms: tReload.ms,
            upd_inplace_ms: tUpdInplace.ms, upd_kc_ms: tUpdKc.ms,
            del_by_id_ms: tDelById.ms, del_by_key_ms: tDelByKey.ms,
            final_save_ms: tFinalSave.ms, final_reload_ms: tFinalReload.ms,
            index_bytes: fp2.indexJsonBytes, routing_ok: vrFinal.ok ? 1 : 0
          });

          db.destroyDB({ dbMain: "REGISTRATIONS" });
        });
      })(si);
    }

    // I8 plain-text summary
    Logger.log("\n══════════════════════════════════════════════════════════════");
    Logger.log("GROUP I8 SUMMARY — partition nominal-ops timing + correctness");
    Logger.log("══════════════════════════════════════════════════════════════");
    Logger.log(
      pad("events×RPE", 12) + " | " + pad("total", 6) + " | " +
      pad("frags", 5) + " | " + pad("idxBytes", 9) + " | " +
      pad("seed_ms", 7) + " | " + pad("save_ms", 7) + " | " +
      pad("reload_ms", 9) + " | " + pad("upd_ip_ms", 9) + " | " +
      pad("upd_kc_ms", 9) + " | " + pad("del_id_ms", 9) + " | " +
      pad("del_key_ms", 10) + " | " + pad("fsave_ms", 8) + " | " +
      pad("freload_ms", 10) + " | route"
    );
    Logger.log("─".repeat(140));
    for (var ri = 0; ri < i8ReportRows.length; ri++) {
      var r = i8ReportRows[ri];
      Logger.log(
        pad(r.events + "×" + r.rowsPerEvent, 12) + " | " +
        pad(r.total, 6) + " | " + pad(r.frags, 5) + " | " +
        pad(r.index_bytes, 9) + " | " +
        pad(r.seed_ms, 7) + " | " + pad(r.save_ms, 7) + " | " +
        pad(r.reload_ms, 9) + " | " + pad(r.upd_inplace_ms, 9) + " | " +
        pad(r.upd_kc_ms, 9) + " | " + pad(r.del_by_id_ms, 9) + " | " +
        pad(r.del_by_key_ms, 10) + " | " + pad(r.final_save_ms, 8) + " | " +
        pad(r.final_reload_ms, 10) + " | " + (r.routing_ok ? "OK" : "FAIL")
      );
    }
    Logger.log("══════════════════════════════════════════════════════════════\n");

    // ── I9: destroyDB on one partition fragment — other partitions' files intact ─

    run("I9: destroyDB on one partition's fragment trashes only that Drive file", function () {
      var ctx = makeAssertDB(realDrive, emptyPartIndex("i9reg"), { partitionBy: PART_BY });
      indexIdsToTrash.push(ctx.indexFileId);
      var db = ctx.db;

      db.addToDB(partReg(1, "EVT_TRASH"), { dbMain: "REGISTRATIONS" });
      db.addToDB(partReg(2, "EVT_SPARE"), { dbMain: "REGISTRATIONS" });
      db.saveToDBFiles();

      var trashFileId = db.INDEX.REGISTRATIONS.dbFragments[partFragName("EVT_TRASH")].fileId;
      var spareFileId = db.INDEX.REGISTRATIONS.dbFragments[partFragName("EVT_SPARE")].fileId;
      assert(trashFileId !== "", "I9: EVT_TRASH fileId must be set");
      assert(spareFileId !== "", "I9: EVT_SPARE fileId must be set");

      // Destroy only EVT_TRASH's fragment.
      db.destroyDB({ dbMain: "REGISTRATIONS", dbFragment: partFragName("EVT_TRASH") });

      // EVT_TRASH Drive file must be trashed.
      var trashFile = DriveApp.getFileById(trashFileId);
      assert(trashFile.isTrashed(), "I9: EVT_TRASH Drive file must be in trash after destroyDB");

      // EVT_SPARE Drive file must still be live.
      var spareFile = DriveApp.getFileById(spareFileId);
      assert(!spareFile.isTrashed(), "I9: EVT_SPARE Drive file must NOT be trashed");

      // EVT_SPARE row must still be readable.
      var row = db.lookUpById(2, { dbMain: "REGISTRATIONS" });
      assertNotNull(row, "I9: EVT_SPARE row must still be readable after destroying EVT_TRASH");

      db.destroyDB({ dbMain: "REGISTRATIONS" });
    });

    // ── I10: INDEX footprint — partition vs cumulative ─────────────────────────
    // Same total rows, different routing strategies.
    // Partition routing uses one fragment per event; keyToFragment grows O(rows).
    // Cumulative routing packs all rows into size-capped fragments.
    // We report both INDEX sizes so you can make an informed trade-off.

    run("I10: INDEX footprint — partition routing vs cumulative routing (same total rows)", function () {
      var folder = DriveApp.getFolderById(TEST_FOLDER_ID);

      // Partition DB: 20 events × 10 rows = 200 total rows.
      var partIndexId = folder.createFile(
        "ASSERT_I10_PART_" + Date.now() + ".json",
        JSON.stringify(emptyPartIndex("i10p")),
        "application/json"
      ).getId();
      indexIdsToTrash.push(partIndexId);

      var partDb = SHARD_DB.init(partIndexId, realDrive, { partitionBy: PART_BY });
      var rowId = 1;
      for (var e = 0; e < 20; e++) {
        for (var r = 0; r < 10; r++) {
          partDb.addToDB(partReg(rowId, "EV" + e, { score: r }), { dbMain: "REGISTRATIONS" });
          rowId++;
        }
      }
      partDb.saveToDBFiles();

      var partFp = partDb.getIndexFootprint({ dbMain: "REGISTRATIONS" });

      // Cumulative DB: same 200 rows, no partitionBy.
      var cumIndexPayload = { USERS: emptyDbMain("i10c") };
      var cumIndexId = folder.createFile(
        "ASSERT_I10_CUM_" + Date.now() + ".json",
        JSON.stringify(cumIndexPayload),
        "application/json"
      ).getId();
      indexIdsToTrash.push(cumIndexId);

      var cumDb = SHARD_DB.init(cumIndexId, realDrive);
      for (var i = 1; i <= 200; i++) {
        cumDb.addToDB({ id: i, key: "k_" + i, score: i }, { dbMain: "USERS" });
      }
      cumDb.saveToDBFiles();

      var cumFp = cumDb.getIndexFootprint({ dbMain: "USERS" });

      Logger.log(
        "\nI10 FOOTPRINT COMPARISON (200 rows)" +
        "\n  Partition (20 events × 10 rows): frags=" + partFp.fragmentsCount +
          " keys=" + partFp.keyToFragmentCount +
          " indexBytes=" + partFp.indexJsonBytes +
        "\n  Cumulative (200 rows, 1 fragment): frags=" + cumFp.fragmentsCount +
          " keys=" + cumFp.keyToFragmentCount +
          " indexBytes=" + cumFp.indexJsonBytes +
        "\n  INDEX size ratio (partition / cumulative): " +
          (partFp.indexJsonBytes / cumFp.indexJsonBytes).toFixed(2) + "x"
      );

      // Sanity: both should have 200 keys in their routing map.
      assertEqual(partFp.keyToFragmentCount, 200, "I10: partition keyToFragment should have 200 entries");
      assertEqual(cumFp.keyToFragmentCount,  200, "I10: cumulative keyToFragment should have 200 entries");

      // Partition should have 20 fragments (one per event), cumulative should have 1.
      assertEqual(partFp.fragmentsCount, 20,  "I10: partition should have 20 fragments");
      assertEqual(cumFp.fragmentsCount,  1,   "I10: cumulative should have 1 fragment");

      partDb.destroyDB({ dbMain: "REGISTRATIONS" });
      cumDb.destroyDB({ dbMain: "USERS" });
    });

    Logger.log("── Group I complete ──\n");
  }

  return {
    runFlow: runFlow,
    runAllTestScenarios: runFlow,
    runFullBenchmarkSuite: runFullBenchmarkSuite,
    runFullApiCoverage: runFullApiCoverage,
    runSequenceScenarios: runSequenceScenarios,
    runPerfMatrix: runPerfMatrix,
    runAssertionSuite: runAssertionSuite
  };
});

/** Entry: legacy visual suite */
function runShardDbVisualDriveTestSuite() {
  if (typeof SHARD_DB_TESTS !== "undefined") {
    SHARD_DB_TESTS.runAllTestScenarios();
  } else {
    Logger.log("FATAL: SHARD_DB_TESTS not defined");
  }
}

/** Entry: full API + perf matrix — leaves one SHARD_PERF_REPORT_*.json; trashes temp INDEX files */
function runShardDbFullBenchmarkSuite() {
  if (typeof SHARD_DB_TESTS !== "undefined") {
    SHARD_DB_TESTS.runFullBenchmarkSuite({ perfSizes: [100, 1000, 10000] });
  } else {
    Logger.log("FATAL: SHARD_DB_TESTS not defined");
  }
}

/**
 * Optional: include 100000 — may exceed Apps Script execution time (test on a copy).
 */
function runShardDbFullBenchmarkSuiteWith100k() {
  if (typeof SHARD_DB_TESTS !== "undefined") {
    SHARD_DB_TESTS.runFullBenchmarkSuite({ perfSizes: [100, 1000, 10000, 100000] });
  } else {
    Logger.log("FATAL: SHARD_DB_TESTS not defined");
  }
}

/**
 * Entry: assertion suite — all bug-fix checks + full API round-trip + backup/restore + partition tests.
 * Throws on first failure. Configure SHARDDB_TEST_FOLDER_ID in Script Properties
 * to point at your test folder (or leave it unset to use the hardcoded fallback).
 */
function runShardDbAssertionSuite() {
  if (typeof SHARD_DB_TESTS === "undefined") {
    throw new Error("FATAL: SHARD_DB_TESTS not defined — check library load order.");
  }
  if (typeof SHARD_DB_TOOLKIT === "undefined") {
    throw new Error("FATAL: SHARD_DB_TOOLKIT not defined — ShardDBToolkitHelpers.js must be loaded.");
  }
  SHARD_DB_TESTS.runAssertionSuite();
}

/**
 * Entry: full assertion suite including Group I partition routing tests.
 *
 * This is an alias for runShardDbAssertionSuite() that makes it explicit you
 * want to run Groups A–I. Group I covers partition routing on real Drive:
 *   I1  — setupPartitions creates Drive files with correct naming
 *   I2  — fragment name is deterministic across GAS executions
 *   I3  — cold reload routes all rows correctly
 *   I4  — lookupByCriteria with partitionKey only opens target Drive files
 *   I5  — overflow creates a second Drive file for the same partition
 *   I6  — lookupByCriteria with partitionKey spans base + overflow Drive files
 *   I7  — cross-partition delete does not corrupt other partitions' Drive files
 *   I8  — 7-phase nominal-ops at 10×20 / 50×20 / 200×20 rows with timing table
 *   I9  — destroyDB on one partition fragment trashes only that Drive file
 *   I10 — INDEX footprint comparison: partition vs cumulative (200 rows each)
 *
 * Configure SHARDDB_TEST_FOLDER_ID in Script Properties before running.
 * Each run is self-cleaning — all ephemeral INDEX and fragment files are trashed.
 * Filter the execution log for "Group I" lines to focus on partition results.
 */
function runShardDbPartitionSuite() {
  if (typeof SHARD_DB_TESTS === "undefined") {
    throw new Error("FATAL: SHARD_DB_TESTS not defined — check library load order.");
  }
  if (typeof SHARD_DB_TOOLKIT === "undefined") {
    throw new Error("FATAL: SHARD_DB_TOOLKIT not defined — ShardDBToolkitHelpers.js must be loaded.");
  }
  SHARD_DB_TESTS.runAssertionSuite();
}
