"use strict";

/**
 * ShardDB — Partition routing test suite.
 *
 * Tests the `partitionBy` option introduced to support semantic sharding:
 * each partition key (e.g. event_id) gets its own fragment from the start.
 * Overflow shards are created automatically once a partition exceeds maxEntriesCount.
 *
 * Test groups:
 *
 *   A — Basic partition routing
 *       A1  partitionBy routes new entries to the correct base fragment
 *       A2  fragment name follows the convention  dbMain_p_<partitionKey>
 *       A3  two entries with different partitionKeys land in different fragments
 *       A4  entries with the same partitionKey land in the same fragment
 *       A5  keyToFragment is populated correctly on addToDB
 *
 *   B — Direct lookup (zero INDEX scan)
 *       B1  lookUpByKey with partitionKey resolves without scanning other fragments
 *       B2  lookUpById  with partitionKey resolves without scanning other fragments
 *       B3  lookupByCriteria with partitionKey only opens the target partition's fragments
 *       B4  lookUpByKey without partitionKey falls back to global routing and still works
 *       B5  lookUpById  without partitionKey falls back to global routing and still works
 *
 *   C — setupPartitions (first-time DB provisioning)
 *       C1  setupPartitions pre-creates base fragments for all given partition keys
 *       C2  setupPartitions is idempotent — calling twice does not duplicate fragments
 *       C3  setupPartitions throws when dbMain has no partitionBy function
 *       C4  entries added after setupPartitions route to the pre-created fragment (no new fragment created)
 *
 *   D — Overflow sharding
 *       D1  when a partition exceeds maxEntriesCount a new overflow fragment is created
 *       D2  overflow fragment name is  baseFragment_2
 *       D3  a third overflow is  baseFragment_3
 *       D4  idRangesSorted covers both base and overflow fragments correctly
 *       D5  lookupByCriteria with partitionKey returns rows from base AND overflow fragments
 *       D6  in-place update of an existing id does NOT create an overflow fragment
 *
 *   E — Cross-partition isolation
 *       E1  deleting an entry from partition A does not affect partition B
 *       E2  clearDB on a single fragment only wipes that partition's fragment
 *       E3  validateRoutingConsistency passes for a multi-partition DB
 *
 *   F — Persist and reload
 *       F1  after saveToDBFiles + re-init, partition routing still resolves correctly
 *       F2  partition fragments survive a reload and all rows are intact
 *       F3  lookUpByKey with partitionKey works after reload (no full-scan needed)
 *       F4  overflow rows survive reload and are reachable via partitionKey lookup
 *
 *   G — Edge cases
 *       G1  partition key with special characters is sanitized in the fragment name
 *       G2  partition key that is a number is coerced to string correctly
 *       G3  addToDB on a partitioned dbMain ignores any caller-supplied dbFragment
 *             (partition routing always wins)
 *       G4  isPartitioned returns true for partitioned tables and false for others
 */

const { describe, it, before, beforeEach } = require("node:test");
const assert = require("node:assert/strict");
const { loadShardDbUmd } = require("./helpers/load-sharddb");
const { createMockDrive } = require("./helpers/mock-drive");

const SHARD_DB = loadShardDbUmd();
const INDEX_ID = "PART_MASTER_INDEX";
const SMALL_MAX = 5; // tiny cap so overflow tests are fast

// ─── index payload factory ────────────────────────────────────────────────────

/**
 * Build a fresh index payload.
 * REGISTRATIONS  — partitioned table (partitionBy = entry.eventId)
 * USERS          — plain cumulative table (control / isolation checks)
 */
function makeIndexPayload() {
  return {
    REGISTRATIONS: {
      properties: {
        cumulative: false,
        rootFolder: "test_folder",
        filesPrefix: "reg",
        fragmentsList: [],
        keyToFragment: {},
        idRangesSorted: []
      },
      dbFragments: {}
    },
    USERS: {
      properties: {
        cumulative: true,
        rootFolder: "test_folder",
        filesPrefix: "usr",
        fragmentsList: [],
        keyToFragment: {},
        idRangesSorted: []
      },
      dbFragments: {}
    }
  };
}

/** partitionBy functions supplied to every init() call. */
const PARTITION_BY = {
  REGISTRATIONS: function (entry) { return entry.eventId; }
};

// ─── helpers ──────────────────────────────────────────────────────────────────

function makeDB(mock, maxEntries) {
  var opts = { partitionBy: PARTITION_BY };
  if (maxEntries != null) opts.maxEntriesCount = maxEntries;
  return SHARD_DB.init(INDEX_ID, mock.adapter, opts);
}

/** Build a registration row. */
function reg(id, eventId, attendee) {
  return { id: id, key: "reg_" + id, eventId: eventId, attendee: attendee || "person_" + id };
}

/** Base fragment name for REGISTRATIONS and a given eventId. */
function baseFrag(eventId) {
  return "REGISTRATIONS_p_" + eventId;
}

// ─── Group A: Basic partition routing ────────────────────────────────────────

describe("Group A — basic partition routing", function () {
  var mock;
  var db;

  beforeEach(function () {
    mock = createMockDrive({
      dbDir: ".mock_drive_partition_a",
      defaultIndexPayload: makeIndexPayload()
    });
    mock.wipe();
    db = makeDB(mock);
  });

  it("A1 — new entry routes to the base partition fragment", function () {
    db.addToDB(reg(1, "EVT001"), { dbMain: "REGISTRATIONS" });
    const frag = db.INDEX.REGISTRATIONS.properties.fragmentsList[0];
    assert.equal(frag, baseFrag("EVT001"));
  });

  it("A2 — fragment name follows dbMain_p_<partitionKey> convention", function () {
    db.addToDB(reg(1, "EVT042"), { dbMain: "REGISTRATIONS" });
    const frag = db.INDEX.REGISTRATIONS.properties.fragmentsList[0];
    assert.match(frag, /^REGISTRATIONS_p_EVT042$/);
  });

  it("A3 — different partitionKeys land in different fragments", function () {
    db.addToDB(reg(1, "EVT001"), { dbMain: "REGISTRATIONS" });
    db.addToDB(reg(2, "EVT002"), { dbMain: "REGISTRATIONS" });
    const list = db.INDEX.REGISTRATIONS.properties.fragmentsList;
    assert.ok(list.includes(baseFrag("EVT001")));
    assert.ok(list.includes(baseFrag("EVT002")));
    assert.equal(list.length, 2);
  });

  it("A4 — same partitionKey always lands in the same fragment", function () {
    db.addToDB(reg(1, "EVT001"), { dbMain: "REGISTRATIONS" });
    db.addToDB(reg(2, "EVT001"), { dbMain: "REGISTRATIONS" });
    db.addToDB(reg(3, "EVT001"), { dbMain: "REGISTRATIONS" });
    const list = db.INDEX.REGISTRATIONS.properties.fragmentsList;
    // Only one fragment for EVT001 (no overflow with 3 rows and default cap).
    const evt1Frags = list.filter(function (f) { return f.indexOf(baseFrag("EVT001")) === 0; });
    assert.equal(evt1Frags.length, 1);
    // All three rows are inside that one fragment.
    const openKey = db._routing.openDbKey("REGISTRATIONS", evt1Frags[0]);
    const data = db.OPEN_DB[openKey].toWrite.data;
    assert.ok(data[1] != null);
    assert.ok(data[2] != null);
    assert.ok(data[3] != null);
  });

  it("A5 — keyToFragment is populated on addToDB", function () {
    db.addToDB(reg(1, "EVT001"), { dbMain: "REGISTRATIONS" });
    const kf = db.INDEX.REGISTRATIONS.properties.keyToFragment;
    assert.equal(kf["reg_1"], baseFrag("EVT001"));
  });
});

// ─── Group B: Direct lookup ───────────────────────────────────────────────────

describe("Group B — direct lookup with partitionKey", function () {
  var mock;
  var db;

  beforeEach(function () {
    mock = createMockDrive({
      dbDir: ".mock_drive_partition_b",
      defaultIndexPayload: makeIndexPayload()
    });
    mock.wipe();
    db = makeDB(mock);
    db.addToDB(reg(10, "EVT_A"), { dbMain: "REGISTRATIONS" });
    db.addToDB(reg(20, "EVT_A"), { dbMain: "REGISTRATIONS" });
    db.addToDB(reg(30, "EVT_B"), { dbMain: "REGISTRATIONS" });
  });

  it("B1 — lookUpByKey with partitionKey returns the correct row", function () {
    var row = db.lookUpByKey("reg_10", { dbMain: "REGISTRATIONS", partitionKey: "EVT_A" });
    assert.ok(row != null);
    assert.equal(row.id, 10);
    assert.equal(row.eventId, "EVT_A");
  });

  it("B1b — lookUpByKey with partitionKey for wrong partition returns null", function () {
    // reg_10 belongs to EVT_A; searching in EVT_B should return null.
    var row = db.lookUpByKey("reg_10", { dbMain: "REGISTRATIONS", partitionKey: "EVT_B" });
    assert.equal(row, null);
  });

  it("B2 — lookUpById with partitionKey returns the correct row", function () {
    var row = db.lookUpById(20, { dbMain: "REGISTRATIONS", partitionKey: "EVT_A" });
    assert.ok(row != null);
    assert.equal(row.id, 20);
    assert.equal(row.eventId, "EVT_A");
  });

  it("B2b — lookUpById with partitionKey for wrong partition returns null", function () {
    var row = db.lookUpById(30, { dbMain: "REGISTRATIONS", partitionKey: "EVT_A" });
    assert.equal(row, null);
  });

  it("B3 — lookupByCriteria with partitionKey only returns rows from that partition", function () {
    var results = db.lookupByCriteria([], { dbMain: "REGISTRATIONS", partitionKey: "EVT_A" });
    assert.equal(results.length, 2);
    results.forEach(function (r) { assert.equal(r.eventId, "EVT_A"); });
  });

  it("B3b — lookupByCriteria with partitionKey applies criteria within the partition", function () {
    var results = db.lookupByCriteria(
      [{ param: "id", criterion: 10 }],
      { dbMain: "REGISTRATIONS", partitionKey: "EVT_A" }
    );
    assert.equal(results.length, 1);
    assert.equal(results[0].id, 10);
  });

  it("B4 — lookUpByKey without partitionKey falls back to global routing", function () {
    var row = db.lookUpByKey("reg_30", { dbMain: "REGISTRATIONS" });
    assert.ok(row != null);
    assert.equal(row.id, 30);
  });

  it("B5 — lookUpById without partitionKey falls back to global routing", function () {
    var row = db.lookUpById(10, { dbMain: "REGISTRATIONS" });
    assert.ok(row != null);
    assert.equal(row.id, 10);
  });
});

// ─── Group C: setupPartitions ─────────────────────────────────────────────────

describe("Group C — setupPartitions (first-time provisioning)", function () {
  var mock;
  var db;

  beforeEach(function () {
    mock = createMockDrive({
      dbDir: ".mock_drive_partition_c",
      defaultIndexPayload: makeIndexPayload()
    });
    mock.wipe();
    db = makeDB(mock);
  });

  it("C1 — setupPartitions pre-creates base fragments for all given keys", function () {
    db.setupPartitions("REGISTRATIONS", ["EVT001", "EVT002", "EVT003"]);
    const list = db.INDEX.REGISTRATIONS.properties.fragmentsList;
    assert.ok(list.includes(baseFrag("EVT001")));
    assert.ok(list.includes(baseFrag("EVT002")));
    assert.ok(list.includes(baseFrag("EVT003")));
    assert.equal(list.length, 3);
  });

  it("C2 — setupPartitions is idempotent", function () {
    db.setupPartitions("REGISTRATIONS", ["EVT001", "EVT002"]);
    db.setupPartitions("REGISTRATIONS", ["EVT001", "EVT002", "EVT003"]);
    const list = db.INDEX.REGISTRATIONS.properties.fragmentsList;
    // EVT001 and EVT002 must appear exactly once.
    var count001 = list.filter(function (f) { return f === baseFrag("EVT001"); }).length;
    var count002 = list.filter(function (f) { return f === baseFrag("EVT002"); }).length;
    assert.equal(count001, 1);
    assert.equal(count002, 1);
    assert.equal(list.length, 3);
  });

  it("C3 — setupPartitions throws when dbMain has no partitionBy function", function () {
    assert.throws(function () {
      db.setupPartitions("USERS", ["X"]);
    }, /partitionBy/);
  });

  it("C4 — entries added after setupPartitions route to the pre-created fragment", function () {
    db.setupPartitions("REGISTRATIONS", ["EVT001"]);
    const listBefore = db.INDEX.REGISTRATIONS.properties.fragmentsList.slice();

    db.addToDB(reg(1, "EVT001"), { dbMain: "REGISTRATIONS" });

    const listAfter = db.INDEX.REGISTRATIONS.properties.fragmentsList;
    // No new fragment was created — should still be the same set.
    assert.deepEqual(listAfter.sort(), listBefore.sort());
  });
});

// ─── Group D: Overflow sharding ──────────────────────────────────────────────

describe("Group D — partition overflow sharding", function () {
  var mock;
  var db;
  const MAX = SMALL_MAX; // 5 rows per fragment

  beforeEach(function () {
    mock = createMockDrive({
      dbDir: ".mock_drive_partition_d",
      defaultIndexPayload: makeIndexPayload()
    });
    mock.wipe();
    db = makeDB(mock, MAX);
  });

  it("D1 — exceeding maxEntriesCount creates a new overflow fragment", function () {
    for (var i = 1; i <= MAX + 1; i++) {
      db.addToDB(reg(i, "EVT001"), { dbMain: "REGISTRATIONS" });
    }
    const list = db.INDEX.REGISTRATIONS.properties.fragmentsList;
    const evt1Frags = list.filter(function (f) { return f.indexOf(baseFrag("EVT001")) === 0; });
    assert.equal(evt1Frags.length, 2);
  });

  it("D2 — first overflow fragment is named baseFragment_2", function () {
    for (var i = 1; i <= MAX + 1; i++) {
      db.addToDB(reg(i, "EVT001"), { dbMain: "REGISTRATIONS" });
    }
    const list = db.INDEX.REGISTRATIONS.properties.fragmentsList;
    assert.ok(list.includes(baseFrag("EVT001") + "_2"));
  });

  it("D3 — second overflow fragment is named baseFragment_3", function () {
    for (var i = 1; i <= MAX * 2 + 1; i++) {
      db.addToDB(reg(i, "EVT001"), { dbMain: "REGISTRATIONS" });
    }
    const list = db.INDEX.REGISTRATIONS.properties.fragmentsList;
    assert.ok(list.includes(baseFrag("EVT001") + "_3"));
  });

  it("D4 — idRangesSorted covers both base and overflow fragments", function () {
    for (var i = 1; i <= MAX + 1; i++) {
      db.addToDB(reg(i, "EVT001"), { dbMain: "REGISTRATIONS" });
    }
    const sorted = db.INDEX.REGISTRATIONS.properties.idRangesSorted;
    const frags = sorted.map(function (r) { return r.fragment; });
    assert.ok(frags.includes(baseFrag("EVT001")));
    assert.ok(frags.includes(baseFrag("EVT001") + "_2"));
  });

  it("D5 — lookupByCriteria with partitionKey returns rows from base AND overflow", function () {
    var total = MAX + 3; // spills into overflow
    for (var i = 1; i <= total; i++) {
      db.addToDB(reg(i, "EVT001"), { dbMain: "REGISTRATIONS" });
    }
    var results = db.lookupByCriteria([], { dbMain: "REGISTRATIONS", partitionKey: "EVT001" });
    assert.equal(results.length, total);
  });

  it("D6 — in-place update of existing id does NOT create an overflow fragment", function () {
    for (var i = 1; i <= MAX; i++) {
      db.addToDB(reg(i, "EVT001"), { dbMain: "REGISTRATIONS" });
    }
    const listBefore = db.INDEX.REGISTRATIONS.properties.fragmentsList.slice();

    // Update id=1 in-place (same id, same partition).
    db.addToDB(Object.assign(reg(1, "EVT001"), { attendee: "updated_person" }), { dbMain: "REGISTRATIONS" });

    const listAfter = db.INDEX.REGISTRATIONS.properties.fragmentsList;
    assert.deepEqual(listAfter.sort(), listBefore.sort());

    // Verify value was updated.
    var row = db.lookUpById(1, { dbMain: "REGISTRATIONS" });
    assert.equal(row.attendee, "updated_person");
  });
});

// ─── Group E: Cross-partition isolation ──────────────────────────────────────

describe("Group E — cross-partition isolation", function () {
  var mock;
  var db;

  beforeEach(function () {
    mock = createMockDrive({
      dbDir: ".mock_drive_partition_e",
      defaultIndexPayload: makeIndexPayload()
    });
    mock.wipe();
    db = makeDB(mock);
    db.addToDB(reg(1, "EVT_A"), { dbMain: "REGISTRATIONS" });
    db.addToDB(reg(2, "EVT_A"), { dbMain: "REGISTRATIONS" });
    db.addToDB(reg(3, "EVT_B"), { dbMain: "REGISTRATIONS" });
    db.addToDB(reg(4, "EVT_B"), { dbMain: "REGISTRATIONS" });
  });

  it("E1 — deleting an entry from partition A does not affect partition B", function () {
    db.deleteFromDBById(1, { dbMain: "REGISTRATIONS" });

    var rowA = db.lookUpById(1, { dbMain: "REGISTRATIONS" });
    assert.equal(rowA, null);

    var rowB1 = db.lookUpById(3, { dbMain: "REGISTRATIONS" });
    var rowB2 = db.lookUpById(4, { dbMain: "REGISTRATIONS" });
    assert.ok(rowB1 != null);
    assert.ok(rowB2 != null);
  });

  it("E2 — clearDB on one partition fragment leaves other partitions intact", function () {
    db.clearDB({ dbMain: "REGISTRATIONS", dbFragment: baseFrag("EVT_A") });

    // EVT_A rows should be gone.
    var rowA = db.lookupByCriteria([], { dbMain: "REGISTRATIONS", partitionKey: "EVT_A" });
    assert.equal(rowA.length, 0);

    // EVT_B rows should survive.
    var rowB = db.lookupByCriteria([], { dbMain: "REGISTRATIONS", partitionKey: "EVT_B" });
    assert.equal(rowB.length, 2);
  });

  it("E3 — validateRoutingConsistency passes for a multi-partition DB", function () {
    var result = db.validateRoutingConsistency({ dbMain: "REGISTRATIONS" });
    assert.ok(result.ok, "Routing errors: " + result.errors.join("; "));
  });
});

// ─── Group F: Persist and reload ─────────────────────────────────────────────

describe("Group F — persist and reload", function () {
  var mock;
  const MAX = SMALL_MAX;

  before(function () {
    mock = createMockDrive({
      dbDir: ".mock_drive_partition_f",
      defaultIndexPayload: makeIndexPayload()
    });
    mock.wipe();

    var db = makeDB(mock, MAX);
    db.addToDB(reg(1, "EVT_X"), { dbMain: "REGISTRATIONS" });
    db.addToDB(reg(2, "EVT_X"), { dbMain: "REGISTRATIONS" });
    db.addToDB(reg(3, "EVT_Y"), { dbMain: "REGISTRATIONS" });
    // Add enough to create overflow for EVT_X.
    for (var i = 10; i < 10 + MAX + 1; i++) {
      db.addToDB(reg(i, "EVT_X"), { dbMain: "REGISTRATIONS" });
    }
    db.saveToDBFiles();
  });

  it("F1 — partition routing resolves correctly after reload", function () {
    var db2 = makeDB(mock, MAX);
    var frag = db2._routing.partitionFragmentForKey("REGISTRATIONS", "EVT_X");
    // Base fragment must exist in the reloaded INDEX.
    assert.ok(db2.INDEX.REGISTRATIONS.properties.fragmentsList.includes(frag));
  });

  it("F2 — all rows are intact after reload", function () {
    var db2 = makeDB(mock, MAX);
    var row1 = db2.lookUpById(1, { dbMain: "REGISTRATIONS" });
    var row3 = db2.lookUpById(3, { dbMain: "REGISTRATIONS" });
    assert.ok(row1 != null, "row 1 missing after reload");
    assert.equal(row1.eventId, "EVT_X");
    assert.ok(row3 != null, "row 3 missing after reload");
    assert.equal(row3.eventId, "EVT_Y");
  });

  it("F3 — lookUpByKey with partitionKey works after reload", function () {
    var db2 = makeDB(mock, MAX);
    var row = db2.lookUpByKey("reg_2", { dbMain: "REGISTRATIONS", partitionKey: "EVT_X" });
    assert.ok(row != null);
    assert.equal(row.id, 2);
  });

  it("F4 — overflow rows survive reload and are reachable via partitionKey", function () {
    var db2 = makeDB(mock, MAX);
    var results = db2.lookupByCriteria([], { dbMain: "REGISTRATIONS", partitionKey: "EVT_X" });
    // 2 original + (MAX+1) overflow batch = MAX+3 rows total for EVT_X
    assert.equal(results.length, MAX + 3);
    results.forEach(function (r) {
      assert.equal(r.eventId, "EVT_X");
    });
  });

  it("F5 — validateRoutingConsistency passes after reload", function () {
    var db2 = makeDB(mock, MAX);
    var result = db2.validateRoutingConsistency({ dbMain: "REGISTRATIONS" });
    assert.ok(result.ok, "Routing errors after reload: " + result.errors.join("; "));
  });
});

// ─── Group G: Edge cases ─────────────────────────────────────────────────────

describe("Group G — edge cases", function () {
  var mock;
  var db;

  beforeEach(function () {
    mock = createMockDrive({
      dbDir: ".mock_drive_partition_g",
      defaultIndexPayload: makeIndexPayload()
    });
    mock.wipe();
    db = makeDB(mock);
  });

  it("G1 — partition key with special characters is sanitized in the fragment name", function () {
    // Slash, space and colon are not in the allowed set.
    db.addToDB(
      { id: 1, key: "r1", eventId: "EVT/2026 01:00" },
      { dbMain: "REGISTRATIONS" }
    );
    const list = db.INDEX.REGISTRATIONS.properties.fragmentsList;
    // No raw slash, space or colon should appear.
    list.forEach(function (f) {
      assert.doesNotMatch(f, /[/ :]/, "unsanitized char in fragment name: " + f);
    });
    assert.equal(list.length, 1);
    assert.match(list[0], /^REGISTRATIONS_p_/);
  });

  it("G2 — numeric partition key is coerced to string", function () {
    db.addToDB({ id: 1, key: "r1", eventId: 42 }, { dbMain: "REGISTRATIONS" });
    const list = db.INDEX.REGISTRATIONS.properties.fragmentsList;
    assert.ok(list.includes("REGISTRATIONS_p_42"), "expected REGISTRATIONS_p_42, got: " + list.join(", "));
  });

  it("G3 — partitioned addToDB ignores caller-supplied dbFragment (partition routing wins)", function () {
    // Even if the caller passes dbFragment, the partition fn should route correctly.
    db.addToDB(reg(1, "EVT_Z"), { dbMain: "REGISTRATIONS", dbFragment: "SOME_OTHER_FRAG" });
    const list = db.INDEX.REGISTRATIONS.properties.fragmentsList;
    // The result must be the partition fragment, not SOME_OTHER_FRAG.
    assert.ok(list.includes(baseFrag("EVT_Z")));
    assert.ok(!list.includes("SOME_OTHER_FRAG"));
  });

  it("G4 — isPartitioned returns true for partitioned tables and false for others", function () {
    assert.equal(db._routing.isPartitioned("REGISTRATIONS"), true);
    assert.equal(db._routing.isPartitioned("USERS"), false);
  });

  it("G5 — plain cumulative table (USERS) still works alongside a partitioned one", function () {
    db.addToDB({ id: 1, key: "alice" }, { dbMain: "USERS" });
    db.addToDB({ id: 2, key: "bob" }, { dbMain: "USERS" });
    var alice = db.lookUpByKey("alice", { dbMain: "USERS" });
    assert.ok(alice != null);
    assert.equal(alice.id, 1);
    // REGISTRATIONS partitions must be unaffected.
    db.addToDB(reg(10, "EVT_A"), { dbMain: "REGISTRATIONS" });
    var result = db.validateRoutingConsistency({ dbMain: "REGISTRATIONS" });
    assert.ok(result.ok, result.errors.join("; "));
  });

  it("G6 — deleteFromDBByKey works on a partitioned table", function () {
    db.addToDB(reg(1, "EVT_A"), { dbMain: "REGISTRATIONS" });
    db.deleteFromDBByKey("reg_1", { dbMain: "REGISTRATIONS" });
    var row = db.lookUpByKey("reg_1", { dbMain: "REGISTRATIONS" });
    assert.equal(row, null);
    var byId = db.lookUpById(1, { dbMain: "REGISTRATIONS" });
    assert.equal(byId, null);
  });

  it("G7 — deleteFromDBById works on a partitioned table", function () {
    db.addToDB(reg(5, "EVT_B"), { dbMain: "REGISTRATIONS" });
    db.deleteFromDBById(5, { dbMain: "REGISTRATIONS" });
    var row = db.lookUpById(5, { dbMain: "REGISTRATIONS" });
    assert.equal(row, null);
  });
});
