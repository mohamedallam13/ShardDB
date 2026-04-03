;(function (root, factory) {
  root.SHARD_DB_TESTS = factory();
})(this, function () {
  const TEST_FOLDER_ID = "1E_7mgRa6Pub901rpR-BescRuita0Gkb_";

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

  return {
    runFlow: runFlow,
    runAllTestScenarios: runFlow,
    runFullBenchmarkSuite: runFullBenchmarkSuite,
    runFullApiCoverage: runFullApiCoverage,
    runSequenceScenarios: runSequenceScenarios,
    runPerfMatrix: runPerfMatrix
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
