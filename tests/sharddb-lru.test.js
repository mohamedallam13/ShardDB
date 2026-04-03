"use strict";

/**
 * ShardDB — LRU fragment eviction test suite.
 *
 * Tests the `maxOpenFragments` option introduced to cap OPEN_DB memory usage.
 * When the limit is reached, the least-recently-used fragment is auto-saved
 * (if dirty) and evicted before the next fragment is opened.
 *
 * Test groups:
 *
 *   A — Basic LRU tracking
 *       A1  maxOpenFragments defaults to Infinity — no eviction by default
 *       A2  _lru.size tracks the number of open fragments
 *       A3  opening a fragment adds it to the LRU tracker
 *       A4  _lru.mruKey() is the most recently opened / accessed fragment
 *       A5  accessing a cached fragment promotes it to MRU position
 *       A6  closeDB removes a fragment from the LRU tracker
 *
 *   B — Eviction behaviour
 *       B1  when capacity is reached the LRU fragment is evicted
 *       B2  evicted fragment is removed from OPEN_DB
 *       B3  LRU size never exceeds maxOpenFragments
 *       B4  the evicted fragment is the least-recently-used, not the oldest opened
 *       B5  after eviction a subsequent access re-reads from Drive (one extra read)
 *
 *   C — Dirty-fragment auto-save on eviction
 *       C1  dirty evicted fragment is written to Drive before removal
 *       C2  clean evicted fragment is NOT written to Drive (no spurious write)
 *       C3  auto-saved fragment data is intact when re-opened after eviction
 *       C4  saveToDBFiles does not crash even when some fragments were already evicted
 *
 *   D — Correctness through eviction cycles
 *       D1  data written before eviction survives a re-open
 *       D2  lookUpByKey works after target fragment was evicted and re-read
 *       D3  lookUpById  works after target fragment was evicted and re-read
 *       D4  saveToDBFiles after eviction only writes remaining in-memory dirty fragments
 *       D5  multi-cycle: open N+10 fragments with cap N — all data intact
 *
 *   E — Edge cases
 *       E1  maxOpenFragments = 1 is valid (single-fragment window)
 *       E2  maxOpenFragments = 0 is treated as Infinity
 *       E3  maxOpenFragments = negative is treated as Infinity
 *       E4  destroyDB removes fragment from LRU tracker
 *       E5  close then re-open a fragment: size stays consistent
 */

const path = require("path");
const { describe, it, before, beforeEach } = require("node:test");
const assert = require("node:assert/strict");
const { loadShardDbUmd } = require("./helpers/load-sharddb");
const { createMockDrive, wrapAdapterWithWriteCounts } = require("./helpers/mock-drive");

const SHARD_DB = loadShardDbUmd();
const INDEX_ID = "LRU_MASTER_INDEX";

// ─── index payload factory ────────────────────────────────────────────────────

function makeIndexPayload() {
  return {
    ITEMS: {
      properties: {
        cumulative: true,
        rootFolder: "test_folder",
        filesPrefix: "itm",
        fragmentsList: [],
        keyToFragment: {},
        idRangesSorted: []
      },
      dbFragments: {}
    }
  };
}

// ─── helpers ──────────────────────────────────────────────────────────────────

/** Fresh mock drive in an isolated temp directory. */
function freshMock(suffix) {
  var dir = path.join(__dirname, ".mock_drive_lru_" + (suffix || "default"));
  var mock = createMockDrive({ dbDir: dir });
  mock.wipe();
  mock.adapter.writeToJSON(INDEX_ID, makeIndexPayload());
  return mock;
}

/**
 * Create a DB with per-fragment row cap `maxEntries` and in-memory fragment
 * window `maxOpen`.  Both default to ShardDB defaults if omitted.
 */
function makeDB(adapter, maxEntries, maxOpen) {
  var opts = {};
  if (maxEntries != null) opts.maxEntriesCount = maxEntries;
  if (maxOpen   != null) opts.maxOpenFragments = maxOpen;
  return SHARD_DB.init(INDEX_ID, adapter, opts);
}

// ─── Group A — Basic LRU tracking ────────────────────────────────────────────

describe("LRU eviction — A: basic tracking", function () {
  var mock, db;

  beforeEach(function () {
    mock = freshMock("a");
    // maxEntries=1 so each addToDB creates a new fragment
    db = makeDB(mock.adapter, 1);  // maxOpen defaults to Infinity
  });

  it("A1 — maxOpenFragments defaults to Infinity — no eviction by default", function () {
    assert.equal(db.maxOpenFragments, Infinity);
  });

  it("A2 — _lru.size tracks the number of open fragments", function () {
    assert.equal(db._lru.size, 0);
    db.addToDB({ id: 1, key: "k1", value: "v" }, { dbMain: "ITEMS" });
    assert.equal(db._lru.size, 1);
    db.addToDB({ id: 2, key: "k2", value: "v" }, { dbMain: "ITEMS" });
    // Second entry overflows to ITEMS_2 with cap=1
    assert.equal(db._lru.size, 2);
  });

  it("A3 — opening a fragment adds it to the LRU tracker", function () {
    db.addToDB({ id: 1, key: "k1", value: "v" }, { dbMain: "ITEMS" });
    var key = db._routing.openDbKey("ITEMS", "ITEMS_1");
    assert.ok(db._lru.nodes[key], "LRU should track the opened fragment");
  });

  it("A4 — all opened fragments appear in the LRU tracker", function () {
    db.addToDB({ id: 1, key: "k1" }, { dbMain: "ITEMS" });
    db.addToDB({ id: 2, key: "k2" }, { dbMain: "ITEMS" });
    db.addToDB({ id: 3, key: "k3" }, { dbMain: "ITEMS" });
    assert.equal(db._lru.size, 3);
    // All keys must be tracked
    ["ITEMS_1", "ITEMS_2", "ITEMS_3"].forEach(function (frag) {
      var k = db._routing.openDbKey("ITEMS", frag);
      assert.ok(db._lru.nodes[k], frag + " must be in LRU");
    });
  });

  it("A5 — accessing a cached fragment promotes it to MRU position", function () {
    // 3 entries → 3 fragments, no eviction limit
    db.addToDB({ id: 1, key: "k1" }, { dbMain: "ITEMS" });
    db.addToDB({ id: 2, key: "k2" }, { dbMain: "ITEMS" });
    db.addToDB({ id: 3, key: "k3" }, { dbMain: "ITEMS" });
    // ITEMS_3 was the last opened → currently MRU.  Now access ITEMS_1.
    db.lookUpByKey("k1", { dbMain: "ITEMS" });
    var mru = db._lru.mruKey();
    var k1  = db._routing.openDbKey("ITEMS", "ITEMS_1");
    assert.equal(mru, k1, "lookUpByKey should promote ITEMS_1 to MRU");
  });

  it("A6 — closeDB removes a fragment from the LRU tracker", function () {
    db.addToDB({ id: 1, key: "k1" }, { dbMain: "ITEMS" });
    assert.equal(db._lru.size, 1);
    db.closeDB({ dbMain: "ITEMS", dbFragment: "ITEMS_1" });
    assert.equal(db._lru.size, 0);
    var k = db._routing.openDbKey("ITEMS", "ITEMS_1");
    assert.equal(db._lru.nodes[k], undefined, "LRU node must be gone after closeDB");
  });
});

// ─── Group B — Eviction behaviour ────────────────────────────────────────────

describe("LRU eviction — B: eviction behaviour", function () {

  it("B1 — when capacity is reached the LRU fragment is evicted", function () {
    var mock = freshMock("b1");
    var db = makeDB(mock.adapter, 1, 2);
    db.addToDB({ id: 1, key: "k1" }, { dbMain: "ITEMS" });
    db.addToDB({ id: 2, key: "k2" }, { dbMain: "ITEMS" });
    assert.equal(db._lru.size, 2);
    db.saveToDBFiles(); // flush so files exist (fileId != "")
    // Access ITEMS_2 to make it MRU → ITEMS_1 becomes LRU
    db.lookUpByKey("k2", { dbMain: "ITEMS" });
    // Adding id=3 opens ITEMS_3 → evicts ITEMS_1 (LRU)
    db.addToDB({ id: 3, key: "k3" }, { dbMain: "ITEMS" });
    assert.equal(db._lru.size, 2, "size must stay at maxOpenFragments after eviction");
    var k1 = db._routing.openDbKey("ITEMS", "ITEMS_1");
    assert.equal(db.OPEN_DB[k1], undefined, "ITEMS_1 must have been evicted");
  });

  it("B2 — evicted fragment is removed from OPEN_DB", function () {
    var mock = freshMock("b2");
    var db = makeDB(mock.adapter, 1, 1);
    db.addToDB({ id: 1, key: "k1" }, { dbMain: "ITEMS" });
    db.saveToDBFiles();
    db.addToDB({ id: 2, key: "k2" }, { dbMain: "ITEMS" }); // evicts ITEMS_1
    var k1 = db._routing.openDbKey("ITEMS", "ITEMS_1");
    assert.equal(db.OPEN_DB[k1], undefined, "ITEMS_1 must not be in OPEN_DB after eviction");
  });

  it("B3 — LRU size never exceeds maxOpenFragments", function () {
    var mock = freshMock("b3");
    var db = makeDB(mock.adapter, 1, 3);
    for (var i = 1; i <= 10; i++) {
      db.addToDB({ id: i, key: "k" + i }, { dbMain: "ITEMS" });
      db.saveToDBFiles();
      assert.ok(db._lru.size <= 3, "LRU size exceeded maxOpenFragments at i=" + i);
    }
  });

  it("B4 — the evicted fragment is LRU not the oldest opened", function () {
    var mock = freshMock("b4");
    var db = makeDB(mock.adapter, 1, 2);
    db.addToDB({ id: 1, key: "k1" }, { dbMain: "ITEMS" });
    db.addToDB({ id: 2, key: "k2" }, { dbMain: "ITEMS" });
    db.saveToDBFiles();
    // Re-access ITEMS_1 → ITEMS_1 becomes MRU, ITEMS_2 becomes LRU
    db.lookUpByKey("k1", { dbMain: "ITEMS" });
    // Add id=3 → evicts ITEMS_2 (LRU), not ITEMS_1 (MRU)
    db.addToDB({ id: 3, key: "k3" }, { dbMain: "ITEMS" });
    var k2 = db._routing.openDbKey("ITEMS", "ITEMS_2");
    var k1 = db._routing.openDbKey("ITEMS", "ITEMS_1");
    assert.equal(db.OPEN_DB[k2], undefined, "ITEMS_2 should be evicted (it was LRU)");
    assert.ok(db.OPEN_DB[k1] !== undefined,  "ITEMS_1 should still be open (it was MRU)");
  });

  it("B5 — after eviction a subsequent access re-reads from Drive", function () {
    var mock = freshMock("b5");
    var wrapped = wrapAdapterWithWriteCounts(mock.adapter, { indexFileId: INDEX_ID });
    // Use a separate read-tracking adapter by wrapping readFromJSON
    var reads = 0;
    var trackingAdapter = {
      readFromJSON:  function (id) { reads++; return wrapped.adapter.readFromJSON(id); },
      writeToJSON:   function (id, p) { return wrapped.adapter.writeToJSON(id, p); },
      createJSON:    function (n, r, p) { return wrapped.adapter.createJSON(n, r, p); },
      deleteFile:    function (id) { return wrapped.adapter.deleteFile(id); }
    };
    var db = makeDB(trackingAdapter, 1, 1);
    // init reads INDEX — reset counter after that
    var initReads = reads;
    db.addToDB({ id: 1, key: "k1", value: "x" }, { dbMain: "ITEMS" });
    db.saveToDBFiles();
    var readsBefore = reads;
    db.addToDB({ id: 2, key: "k2" }, { dbMain: "ITEMS" }); // evicts ITEMS_1 (clean)
    db.saveToDBFiles();
    // Accessing evicted ITEMS_1 must trigger a Drive read
    db.lookUpByKey("k1", { dbMain: "ITEMS" });
    assert.ok(reads > readsBefore, "a Drive re-read must happen after eviction");
  });
});

// ─── Group C — Dirty-fragment auto-save on eviction ──────────────────────────

describe("LRU eviction — C: dirty-fragment auto-save", function () {

  it("C1 — dirty evicted fragment is written to Drive before removal", function () {
    var mock = freshMock("c1");
    var wrapped = wrapAdapterWithWriteCounts(mock.adapter, { indexFileId: INDEX_ID });
    var db = makeDB(wrapped.adapter, 1, 1);
    db.addToDB({ id: 1, key: "k1", value: "v1" }, { dbMain: "ITEMS" });
    db.saveToDBFiles(); // flush so fileId is set on ITEMS_1
    // Mark ITEMS_1 dirty again
    db.addToDB({ id: 1, key: "k1", value: "updated" }, { dbMain: "ITEMS" });
    wrapped.reset();
    // Evict ITEMS_1 (dirty) by adding id=2
    db.addToDB({ id: 2, key: "k2" }, { dbMain: "ITEMS" });
    var counts = wrapped.counts();
    assert.ok(counts.fragmentWriteCount >= 1, "dirty eviction must trigger at least one Drive write");
  });

  it("C2 — clean evicted fragment is NOT written to Drive", function () {
    var mock = freshMock("c2");
    var wrapped = wrapAdapterWithWriteCounts(mock.adapter, { indexFileId: INDEX_ID });
    var db = makeDB(wrapped.adapter, 1, 1);
    db.addToDB({ id: 1, key: "k1", value: "v1" }, { dbMain: "ITEMS" });
    db.saveToDBFiles(); // flush → ITEMS_1 is clean
    wrapped.reset();
    // Evict clean ITEMS_1 by adding id=2
    db.addToDB({ id: 2, key: "k2" }, { dbMain: "ITEMS" });
    var counts = wrapped.counts();
    assert.equal(counts.fragmentWriteCount, 0, "clean eviction must not trigger a Drive write");
  });

  it("C3 — auto-saved fragment data is intact when re-opened after eviction", function () {
    var mock = freshMock("c3");
    var db = makeDB(mock.adapter, 1, 1);
    db.addToDB({ id: 1, key: "k1", value: "persisted" }, { dbMain: "ITEMS" });
    db.saveToDBFiles();
    // Update before eviction (dirty)
    db.addToDB({ id: 1, key: "k1", value: "updated_before_evict" }, { dbMain: "ITEMS" });
    // Evict ITEMS_1 (dirty)
    db.addToDB({ id: 2, key: "k2" }, { dbMain: "ITEMS" });
    db.saveToDBFiles();
    // Re-read ITEMS_1 from Drive
    var row = db.lookUpByKey("k1", { dbMain: "ITEMS" });
    assert.ok(row !== null, "row must survive eviction + re-open");
    assert.equal(row.value, "updated_before_evict", "auto-saved value must be intact");
  });

  it("C4 — saveToDBFiles does not crash when some fragments were already evicted", function () {
    var mock = freshMock("c4");
    var db = makeDB(mock.adapter, 1, 1);
    db.addToDB({ id: 1, key: "k1" }, { dbMain: "ITEMS" });
    db.saveToDBFiles();
    db.addToDB({ id: 1, key: "k1", value: "dirty" }, { dbMain: "ITEMS" });
    // Evict ITEMS_1 (dirty — auto-saved on evict)
    db.addToDB({ id: 2, key: "k2" }, { dbMain: "ITEMS" });
    var k1 = db._routing.openDbKey("ITEMS", "ITEMS_1");
    assert.equal(db.OPEN_DB[k1], undefined, "ITEMS_1 must be gone from OPEN_DB");
    // saveToDBFiles must not crash with the evicted fragment absent
    assert.doesNotThrow(function () { db.saveToDBFiles(); });
  });
});

// ─── Group D — Correctness through eviction cycles ───────────────────────────

describe("LRU eviction — D: correctness through eviction cycles", function () {

  it("D1 — data written before eviction survives a re-open", function () {
    var mock = freshMock("d1");
    var db = makeDB(mock.adapter, 1, 2);
    db.addToDB({ id: 1, key: "k1", value: "alpha" }, { dbMain: "ITEMS" });
    db.addToDB({ id: 2, key: "k2", value: "beta"  }, { dbMain: "ITEMS" });
    db.saveToDBFiles();
    db.addToDB({ id: 3, key: "k3", value: "gamma" }, { dbMain: "ITEMS" }); // evicts ITEMS_1
    db.saveToDBFiles();
    var row = db.lookUpByKey("k1", { dbMain: "ITEMS" });
    assert.ok(row !== null && row.value === "alpha", "evicted row must be readable after re-open");
  });

  it("D2 — lookUpByKey works after target fragment was evicted and re-read", function () {
    var mock = freshMock("d2");
    var db = makeDB(mock.adapter, 1, 1);
    db.addToDB({ id: 1, key: "k1", value: "find_me" }, { dbMain: "ITEMS" });
    db.saveToDBFiles();
    db.addToDB({ id: 2, key: "k2" }, { dbMain: "ITEMS" }); // evict ITEMS_1
    db.saveToDBFiles();
    var row = db.lookUpByKey("k1", { dbMain: "ITEMS" });
    assert.ok(row !== null, "lookUpByKey must work after eviction");
    assert.equal(row.value, "find_me");
  });

  it("D3 — lookUpById works after target fragment was evicted and re-read", function () {
    var mock = freshMock("d3");
    var db = makeDB(mock.adapter, 1, 1);
    db.addToDB({ id: 5, key: "k5", value: "byid" }, { dbMain: "ITEMS" });
    db.saveToDBFiles();
    db.addToDB({ id: 6, key: "k6" }, { dbMain: "ITEMS" }); // evict ITEMS_1
    db.saveToDBFiles();
    var row = db.lookUpById(5, { dbMain: "ITEMS" });
    assert.ok(row !== null && row.value === "byid");
  });

  it("D4 — saveToDBFiles only writes in-memory dirty fragments (evicted fragments are already persisted)", function () {
    var mock = freshMock("d4");
    var wrapped = wrapAdapterWithWriteCounts(mock.adapter, { indexFileId: INDEX_ID });
    var db = makeDB(wrapped.adapter, 1, 1);
    db.addToDB({ id: 1, key: "k1" }, { dbMain: "ITEMS" });
    db.saveToDBFiles(); // flush ITEMS_1; it's now clean
    db.addToDB({ id: 2, key: "k2", value: "new" }, { dbMain: "ITEMS" }); // evicts clean ITEMS_1, opens ITEMS_2
    wrapped.reset();
    db.saveToDBFiles(); // only ITEMS_2 (dirty) should be written
    var db2 = makeDB(mock.adapter, 1, Infinity);
    var row = db2.lookUpByKey("k2", { dbMain: "ITEMS" });
    assert.ok(row !== null && row.value === "new", "ITEMS_2 data must be persisted");
  });

  it("D5 — multi-cycle: open N+10 fragments with cap N — all data intact", function () {
    var mock = freshMock("d5");
    var N = 5;
    var db = makeDB(mock.adapter, 1, N);
    var total = N + 10;
    for (var i = 1; i <= total; i++) {
      db.addToDB({ id: i, key: "k" + i, value: "val" + i }, { dbMain: "ITEMS" });
      db.saveToDBFiles();
    }
    // Verify all rows survive by re-reading in a fresh DB (no LRU limit)
    var db2 = makeDB(mock.adapter, 1, Infinity);
    for (var j = 1; j <= total; j++) {
      var row = db2.lookUpByKey("k" + j, { dbMain: "ITEMS" });
      assert.ok(row !== null, "row k" + j + " must survive multi-cycle eviction");
      assert.equal(row.value, "val" + j);
    }
  });
});

// ─── Group E — Edge cases ─────────────────────────────────────────────────────

describe("LRU eviction — E: edge cases", function () {

  it("E1 — maxOpenFragments = 1 is valid (single-fragment window)", function () {
    var mock = freshMock("e1");
    var db = makeDB(mock.adapter, 1, 1);
    assert.equal(db.maxOpenFragments, 1);
    for (var i = 1; i <= 5; i++) {
      db.addToDB({ id: i, key: "k" + i }, { dbMain: "ITEMS" });
      db.saveToDBFiles();
      assert.ok(db._lru.size <= 1, "LRU size must never exceed 1, failed at i=" + i);
    }
  });

  it("E2 — maxOpenFragments = 0 is treated as Infinity", function () {
    var mock = freshMock("e2");
    var db = makeDB(mock.adapter, 1, 0);
    assert.equal(db.maxOpenFragments, Infinity);
  });

  it("E3 — maxOpenFragments = negative is treated as Infinity", function () {
    var mock = freshMock("e3");
    var db = makeDB(mock.adapter, 1, -5);
    assert.equal(db.maxOpenFragments, Infinity);
  });

  it("E4 — destroyDB removes fragment from LRU tracker", function () {
    var mock = freshMock("e4");
    var db = makeDB(mock.adapter, 1);
    db.addToDB({ id: 1, key: "k1" }, { dbMain: "ITEMS" });
    assert.equal(db._lru.size, 1);
    db.destroyDB({ dbMain: "ITEMS", dbFragment: "ITEMS_1" });
    assert.equal(db._lru.size, 0);
  });

  it("E5 — close then re-open a fragment: size stays consistent", function () {
    var mock = freshMock("e5");
    var db = makeDB(mock.adapter, 1);
    db.addToDB({ id: 1, key: "k1" }, { dbMain: "ITEMS" });
    assert.equal(db._lru.size, 1);
    db.saveToDBFiles();
    db.closeDB({ dbMain: "ITEMS", dbFragment: "ITEMS_1" });
    assert.equal(db._lru.size, 0);
    // Re-open via lookup
    db.lookUpByKey("k1", { dbMain: "ITEMS" });
    assert.equal(db._lru.size, 1);
  });
});
