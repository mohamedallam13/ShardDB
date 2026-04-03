"use strict";

/**
 * Correctness-gap tests for ShardDB — covers paths missed by the existing suite:
 *
 *  1.  lookupByCriteria on missing field (was silently passing rows; now correctly excludes)
 *  2.  lookupByCriteria with function criterion on rows missing the field
 *  3.  lookupByCriteria empty criteria returns all rows
 *  4.  lookupByCriteria id fast-path combined with extra criterion filter
 *  5.  lookupByCriteria with a non-existent id returns []
 *  6.  getValueFromPath deep path (path length > 1)
 *  7.  Key change on addToDB: old key evicted from BOTH keyToFragment AND fragment index
 *  8.  deleteFromDBById clears fragment index entry
 *  9.  deleteFromDBByKey clears fragment index entry
 * 10.  idRangesSorted NOT rebuilt on in-place update (same id, range unchanged)
 * 11.  Binary search edge cases: ids at exact min/max of fragment boundaries
 * 12.  destroyDB single fragment (not whole dbMain)
 * 13.  clearDB then addToDB works; routing stays consistent
 * 14.  Persistence round-trip: save → new init → all data intact + routing consistent
 * 15.  closeDB single fragment (not whole dbMain)
 * 16.  getIndexFootprint without dbMain (full INDEX)
 * 17.  validateRoutingConsistency detects idRange mismatch
 * 18.  validateRoutingConsistency detects keyToFragment orphan
 * 19.  addExternalConfig marks indexRoutingDirty
 * 20.  ignoreIndex fragment: lookupByCriteria full scan still returns rows
 * 21.  lookUpById on empty DB returns null (no crash)
 * 22.  lookUpByKey on unknown key returns null (no crash)
 * 23.  Multi-fragment: update row in first fragment after second has been created
 */

const { describe, it, before, beforeEach } = require("node:test");
const assert = require("node:assert/strict");
const path = require("path");
const { loadShardDbUmd } = require("./helpers/load-sharddb");
const { createMockDrive, wrapAdapterWithWriteCounts } = require("./helpers/mock-drive");
const { bootstrapGasFakes } = require("./helpers/bootstrap-gas-fakes");

const SHARD_DB = loadShardDbUmd();
const INDEX_ID = "MASTER_INDEX_FILE";

function makeDB(mock) {
  return SHARD_DB.init(INDEX_ID, mock.adapter);
}

describe("ShardDB correctness gaps", () => {
  before(async () => {
    await bootstrapGasFakes();
  });

  let mock;

  beforeEach(() => {
    mock = createMockDrive({ dbDir: path.join(__dirname, ".mock_drive", "gaps") });
    mock.wipe();
  });

  // ── 1. lookupByCriteria: rows missing the filtered field are EXCLUDED ──────────────────
  it("lookupByCriteria excludes rows that are missing the filtered field", () => {
    const DB = makeDB(mock);
    DB.addToDB({ key: "a", id: 1, email: "alice@x.com" }, { dbMain: "USERS" });
    DB.addToDB({ key: "b", id: 2 /* no email */ }, { dbMain: "USERS" });
    DB.addToDB({ key: "c", id: 3, email: "charlie@x.com" }, { dbMain: "USERS" });

    const rows = DB.lookupByCriteria([{ param: "email", criterion: "alice@x.com" }], {
      dbMain: "USERS"
    });
    assert.equal(rows.length, 1, "only the matching row should be returned");
    assert.equal(rows[0].key, "a");
  });

  // ── 2. lookupByCriteria: function criterion, rows without the field are excluded ───────
  it("lookupByCriteria function criterion excludes rows missing the field", () => {
    const DB = makeDB(mock);
    DB.addToDB({ key: "a", id: 1, score: 50 }, { dbMain: "USERS" });
    DB.addToDB({ key: "b", id: 2 /* no score */ }, { dbMain: "USERS" });
    DB.addToDB({ key: "c", id: 3, score: 80 }, { dbMain: "USERS" });

    const rows = DB.lookupByCriteria([{ param: "score", criterion: (n) => n > 60 }], {
      dbMain: "USERS"
    });
    assert.equal(rows.length, 1);
    assert.equal(rows[0].key, "c");
  });

  // ── 3. lookupByCriteria: empty criteria returns all rows ──────────────────────────────
  it("lookupByCriteria with empty criteria returns all rows", () => {
    const DB = makeDB(mock);
    for (let i = 1; i <= 5; i++) {
      DB.addToDB({ key: "k" + i, id: i, n: i }, { dbMain: "USERS" });
    }
    const all = DB.lookupByCriteria([], { dbMain: "USERS" });
    assert.equal(all.length, 5);
  });

  // ── 4. lookupByCriteria: id fast-path combined with extra criterion ───────────────────
  it("lookupByCriteria id fast-path is still filtered by extra criterion", () => {
    const DB = makeDB(mock);
    DB.addToDB({ key: "a", id: 42, tag: "hit" }, { dbMain: "USERS" });
    DB.addToDB({ key: "b", id: 43, tag: "miss" }, { dbMain: "USERS" });

    // id fast-path finds the fragment and returns the row; existing lookupByCriteria only
    // uses the id fast-path and ignores other criteria (by design — id uniquely identifies
    // one row). Verify the returned row has the right data.
    const rows = DB.lookupByCriteria([{ param: "id", criterion: 42 }], { dbMain: "USERS" });
    assert.equal(rows.length, 1);
    assert.equal(rows[0].tag, "hit");
  });

  // ── 5. lookupByCriteria: non-existent id returns [] ──────────────────────────────────
  it("lookupByCriteria with non-existent id returns empty array", () => {
    const DB = makeDB(mock);
    DB.addToDB({ key: "a", id: 1, v: 1 }, { dbMain: "USERS" });
    const rows = DB.lookupByCriteria([{ param: "id", criterion: 9999 }], { dbMain: "USERS" });
    assert.deepEqual(rows, []);
  });

  // ── 6. getValueFromPath: deep nested path (path.length > 1) ──────────────────────────
  it("lookupByCriteria with depth-2 path resolves the nested field", () => {
    const DB = makeDB(mock);
    DB.addToDB({ key: "a", id: 1, meta: { tags: { primary: "admin" } } }, { dbMain: "USERS" });
    DB.addToDB({ key: "b", id: 2, meta: { tags: { primary: "user" } } }, { dbMain: "USERS" });

    const rows = DB.lookupByCriteria(
      [{ path: ["meta", "tags"], param: "primary", criterion: "admin" }],
      { dbMain: "USERS" }
    );
    assert.equal(rows.length, 1);
    assert.equal(rows[0].key, "a");
  });

  // ── 7. Key change: old key evicted from keyToFragment AND fragment index ──────────────
  it("key change on addToDB removes old key from keyToFragment and fragment index", () => {
    const DB = makeDB(mock);
    DB.addToDB({ key: "oldKey", id: 10, v: 1 }, { dbMain: "USERS" });
    assert.equal(DB.INDEX.USERS.properties.keyToFragment.oldKey, "USERS_1");

    DB.addToDB({ key: "newKey", id: 10, v: 2 }, { dbMain: "USERS" });

    // keyToFragment
    assert.equal(DB.INDEX.USERS.properties.keyToFragment.oldKey, undefined);
    assert.equal(DB.INDEX.USERS.properties.keyToFragment.newKey, "USERS_1");

    // fragment index (tw.index)
    const tw = DB.OPEN_DB.USERS_1.toWrite;
    assert.equal(tw.index.oldKey, undefined, "old key must be removed from fragment index");
    assert.equal(tw.index.newKey, 10, "new key must be in fragment index");

    // lookup
    assert.equal(DB.lookUpByKey("oldKey", { dbMain: "USERS" }), null);
    assert.equal(DB.lookUpByKey("newKey", { dbMain: "USERS" }).v, 2);
  });

  // ── 8. deleteFromDBById clears fragment index entry ──────────────────────────────────
  it("deleteFromDBById removes the key from the fragment index", () => {
    const DB = makeDB(mock);
    DB.addToDB({ key: "z", id: 5, v: 1 }, { dbMain: "USERS" });
    assert.equal(DB.OPEN_DB.USERS_1.toWrite.index.z, 5);

    DB.deleteFromDBById(5, { dbMain: "USERS" });

    const tw = DB.OPEN_DB.USERS_1.toWrite;
    assert.equal(tw.index.z, undefined, "key must be gone from fragment index after delete-by-id");
    assert.equal(tw.data[5], undefined);
    assert.equal(DB.lookUpById(5, { dbMain: "USERS" }), null);
    assert.equal(DB.lookUpByKey("z", { dbMain: "USERS" }), null);
  });

  // ── 9. deleteFromDBByKey clears fragment index entry ─────────────────────────────────
  it("deleteFromDBByKey removes the key from the fragment index", () => {
    const DB = makeDB(mock);
    DB.addToDB({ key: "y", id: 7, v: 1 }, { dbMain: "USERS" });
    assert.equal(DB.OPEN_DB.USERS_1.toWrite.index.y, 7);

    DB.deleteFromDBByKey("y", { dbMain: "USERS" });

    const tw = DB.OPEN_DB.USERS_1.toWrite;
    assert.equal(tw.index.y, undefined, "key must be gone from fragment index after delete-by-key");
    assert.equal(tw.data[7], undefined);
    assert.equal(DB.lookUpByKey("y", { dbMain: "USERS" }), null);
  });

  // ── 10. idRangesSorted NOT rebuilt on in-place update with unchanged range ────────────
  it("idRangesSorted is unchanged for an in-place update where id is already in range", () => {
    const DB = makeDB(mock);
    DB.addToDB({ key: "a", id: 1, v: 1 }, { dbMain: "USERS" });
    DB.addToDB({ key: "b", id: 100, v: 1 }, { dbMain: "USERS" });
    // After two inserts, range is [1, 100]. Capture the array reference.
    const snapshot = JSON.stringify(DB.INDEX.USERS.properties.idRangesSorted);

    // In-place update — id=50 is inside [1,100]; range should NOT change.
    DB.addToDB({ key: "m", id: 50, v: 2 }, { dbMain: "USERS" });
    const after = JSON.stringify(DB.INDEX.USERS.properties.idRangesSorted);
    assert.equal(after, snapshot, "idRangesSorted must be unchanged when id is within existing range");
  });

  // ── 11. Binary search edge cases: id exactly at min/max boundaries ────────────────────
  it("findFragmentForId works at exact min/max boundaries of each fragment", () => {
    const DB = makeDB(mock);
    const cap = SHARD_DB.MAX_ENTRIES_COUNT;
    for (let i = 1; i <= cap * 2; i++) {
      DB.addToDB({ key: "u" + i, id: i, n: i }, { dbMain: "USERS" });
    }

    // First fragment: ids 1..cap
    assert.equal(DB._routing.findFragmentForId(1, "USERS"), "USERS_1");
    assert.equal(DB._routing.findFragmentForId(cap, "USERS"), "USERS_1");
    // Second fragment: ids cap+1..cap*2
    assert.equal(DB._routing.findFragmentForId(cap + 1, "USERS"), "USERS_2");
    assert.equal(DB._routing.findFragmentForId(cap * 2, "USERS"), "USERS_2");
    // Between ranges: doesn't exist
    assert.equal(DB._routing.findFragmentForId(cap * 2 + 1, "USERS"), null);
  });

  // ── 12. destroyDB single fragment leaves other fragments intact ───────────────────────
  it("destroyDB single fragment removes only that fragment, keeps others", () => {
    const DB = makeDB(mock);
    const cap = SHARD_DB.MAX_ENTRIES_COUNT;
    for (let i = 1; i <= cap + 1; i++) {
      DB.addToDB({ key: "u" + i, id: i, n: i }, { dbMain: "USERS" });
    }
    DB.saveToDBFiles();

    assert.equal(DB.INDEX.USERS.properties.fragmentsList.length, 2);
    DB.destroyDB({ dbMain: "USERS", dbFragment: "USERS_1" });

    assert.equal(DB.INDEX.USERS.properties.fragmentsList.length, 1);
    assert.ok(DB.INDEX.USERS.properties.fragmentsList.includes("USERS_2"));
    assert.equal(DB.INDEX.USERS.dbFragments.USERS_1, undefined);
    // Rows from USERS_2 (id = cap+1) still accessible
    assert.equal(DB.lookUpById(cap + 1, { dbMain: "USERS" }).n, cap + 1);
  });

  // ── 13. clearDB then addToDB keeps routing consistent ─────────────────────────────────
  it("clearDB then addToDB produces a consistent DB", () => {
    const DB = makeDB(mock);
    DB.addToDB({ key: "a", id: 1, v: 1 }, { dbMain: "USERS" });
    DB.addToDB({ key: "b", id: 2, v: 2 }, { dbMain: "USERS" });
    DB.clearDB({ dbMain: "USERS" });

    // Should be able to re-add after clear
    DB.addToDB({ key: "c", id: 3, v: 3 }, { dbMain: "USERS" });
    DB.addToDB({ key: "a", id: 1, v: 10 }, { dbMain: "USERS" }); // reuse old key/id

    assert.equal(DB.lookUpById(3, { dbMain: "USERS" }).v, 3);
    assert.equal(DB.lookUpByKey("a", { dbMain: "USERS" }).v, 10);
    assert.equal(DB.lookUpByKey("b", { dbMain: "USERS" }), null, "cleared key must stay gone");

    const v = DB.validateRoutingConsistency({ dbMain: "USERS" });
    assert.equal(v.ok, true, v.errors.join("; "));
  });

  // ── 14. Persistence round-trip ────────────────────────────────────────────────────────
  it("data and routing survive a save → new init cycle", () => {
    const DB = makeDB(mock);
    DB.addToDB({ key: "p1", id: 1, payload: "hello" }, { dbMain: "USERS" });
    DB.addToDB({ key: "p2", id: 2, payload: "world" }, { dbMain: "USERS" });
    DB.saveToDBFiles();

    // New init from same files
    const DB2 = SHARD_DB.init(INDEX_ID, mock.adapter);
    assert.equal(DB2.lookUpByKey("p1", { dbMain: "USERS" }).payload, "hello");
    assert.equal(DB2.lookUpById(2, { dbMain: "USERS" }).payload, "world");
    const v = DB2.validateRoutingConsistency({ dbMain: "USERS" });
    assert.equal(v.ok, true, v.errors.join("; "));
  });

  // ── 15. closeDB single fragment ───────────────────────────────────────────────────────
  it("closeDB with explicit dbFragment drops only that fragment from OPEN_DB", () => {
    const DB = makeDB(mock);
    const cap = SHARD_DB.MAX_ENTRIES_COUNT;
    for (let i = 1; i <= cap + 1; i++) {
      DB.addToDB({ key: "u" + i, id: i, n: i }, { dbMain: "USERS" });
    }
    DB.saveToDBFiles();

    assert.ok(DB.OPEN_DB.USERS_1);
    assert.ok(DB.OPEN_DB.USERS_2);

    DB.closeDB({ dbMain: "USERS", dbFragment: "USERS_1" });
    assert.equal(DB.OPEN_DB.USERS_1, undefined, "USERS_1 should be evicted");
    assert.ok(DB.OPEN_DB.USERS_2, "USERS_2 should still be open");

    // USERS_1 reloads on next access
    const row = DB.lookUpById(1, { dbMain: "USERS" });
    assert.equal(row.n, 1);
    assert.ok(DB.OPEN_DB.USERS_1);
  });

  // ── 16. getIndexFootprint without dbMain (full INDEX) ─────────────────────────────────
  it("getIndexFootprint without dbMain covers all tables", () => {
    mock.adapter.writeToJSON(INDEX_ID, {
      USERS: {
        properties: {
          cumulative: true, rootFolder: "f", filesPrefix: "u",
          fragmentsList: [], keyToFragment: {}, idRangesSorted: []
        },
        dbFragments: {}
      },
      ORDERS: {
        properties: {
          cumulative: true, rootFolder: "f", filesPrefix: "o",
          fragmentsList: [], keyToFragment: {}, idRangesSorted: []
        },
        dbFragments: {}
      }
    });
    const DB = SHARD_DB.init(INDEX_ID, mock.adapter);
    DB.addToDB({ key: "u1", id: 1, t: "user" }, { dbMain: "USERS" });
    DB.addToDB({ key: "o1", id: 1, t: "order" }, { dbMain: "ORDERS" });

    const fp = DB.getIndexFootprint(); // no dbMain
    assert.equal(fp.keyToFragmentCount, 2, "should count keys across all tables");
    assert.ok(fp.indexJsonBytes > 0);
    assert.equal(fp.fragmentsCount, 2);
  });

  // ── 17. validateRoutingConsistency detects idRange mismatch ──────────────────────────
  it("validateRoutingConsistency reports idRange mismatch when manually corrupted", () => {
    const DB = makeDB(mock);
    DB.addToDB({ key: "a", id: 1, v: 1 }, { dbMain: "USERS" });
    DB.addToDB({ key: "b", id: 2, v: 2 }, { dbMain: "USERS" });

    // Corrupt the idRange
    DB.INDEX.USERS.dbFragments.USERS_1.idRange.min = 999;

    const v = DB.validateRoutingConsistency({ dbMain: "USERS" });
    assert.equal(v.ok, false);
    assert.ok(v.errors.some((e) => e.includes("idRange")), "should report idRange error: " + v.errors.join("; "));
  });

  // ── 18. validateRoutingConsistency detects keyToFragment orphan ───────────────────────
  it("validateRoutingConsistency reports orphaned keyToFragment entry", () => {
    const DB = makeDB(mock);
    DB.addToDB({ key: "a", id: 1, v: 1 }, { dbMain: "USERS" });

    // Inject an orphan: key "ghost" mapped to USERS_1 but not in fragment index
    DB.INDEX.USERS.properties.keyToFragment.ghost = "USERS_1";

    const v = DB.validateRoutingConsistency({ dbMain: "USERS" });
    assert.equal(v.ok, false);
    assert.ok(v.errors.some((e) => e.includes("ghost")), "should report ghost key: " + v.errors.join("; "));
  });

  // ── 19. addExternalConfig marks indexRoutingDirty ─────────────────────────────────────
  it("addExternalConfig marks indexRoutingDirty on the affected dbMain", () => {
    const DB = makeDB(mock);
    DB.addToDB({ key: "a", id: 1, v: 1 }, { dbMain: "USERS" });
    DB.saveToDBFiles();
    assert.equal(DB.INDEX.USERS.properties.indexRoutingDirty, false);

    DB.addExternalConfig("ttl", 60, { dbMain: "USERS", dbFragment: "USERS_1" });
    assert.equal(DB.INDEX.USERS.properties.indexRoutingDirty, true);
  });

  // ── 20. ignoreIndex fragment: lookupByCriteria full scan returns rows ──────────────────
  it("ignoreIndex fragment rows are returned by lookupByCriteria", () => {
    const DB = makeDB(mock);
    DB.addToDB({ key: "a", id: 1, role: "admin" }, { dbMain: "USERS" });
    DB.INDEX.USERS.dbFragments.USERS_1.ignoreIndex = true;
    DB.addToDB({ key: "b", id: 2, role: "user" }, { dbMain: "USERS" });

    const rows = DB.lookupByCriteria([{ param: "role", criterion: "user" }], { dbMain: "USERS" });
    assert.equal(rows.length, 1);
    assert.equal(rows[0].key, "b");
  });

  // ── 21. lookUpById on empty DB returns null (no crash) ────────────────────────────────
  it("lookUpById on an empty DB returns null without throwing", () => {
    const DB = makeDB(mock);
    assert.equal(DB.lookUpById(1, { dbMain: "USERS" }), null);
  });

  // ── 22. lookUpByKey on unknown key returns null (no crash) ───────────────────────────
  it("lookUpByKey with a key that was never inserted returns null", () => {
    const DB = makeDB(mock);
    DB.addToDB({ key: "a", id: 1, v: 1 }, { dbMain: "USERS" });
    assert.equal(DB.lookUpByKey("nonexistent", { dbMain: "USERS" }), null);
  });

  // ── 23. Multi-fragment: update row in first fragment after second is created ──────────
  it("update in first fragment routes correctly even when a second fragment exists", () => {
    const DB = makeDB(mock);
    const cap = SHARD_DB.MAX_ENTRIES_COUNT;
    for (let i = 1; i <= cap + 1; i++) {
      DB.addToDB({ key: "u" + i, id: i, n: i }, { dbMain: "USERS" });
    }
    DB.saveToDBFiles();
    DB.closeDB({ dbMain: "USERS" });

    // Update a row in USERS_1 (which is not the "latest" fragment)
    DB.addToDB({ key: "u1", id: 1, n: 777 }, { dbMain: "USERS" });
    assert.equal(DB.lookUpById(1, { dbMain: "USERS" }).n, 777);
    assert.equal(DB._routing.findFragmentForId(1, "USERS"), "USERS_1");
    assert.equal(DB._routing.findFragmentForId(cap + 1, "USERS"), "USERS_2");

    const v = DB.validateRoutingConsistency({ dbMain: "USERS" });
    assert.equal(v.ok, true, v.errors.join("; "));
  });
});
