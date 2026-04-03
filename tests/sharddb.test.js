"use strict";

const { describe, it, before, beforeEach } = require("node:test");
const assert = require("node:assert/strict");
const path = require("path");
const { loadShardDbUmd } = require("./helpers/load-sharddb");
const { createMockDrive } = require("./helpers/mock-drive");
const { bootstrapGasFakes } = require("./helpers/bootstrap-gas-fakes");

const SHARD_DB = loadShardDbUmd();
const INDEX_ID = "MASTER_INDEX_FILE";

describe("ShardDB", () => {
  before(async () => {
    await bootstrapGasFakes();
  });

  let mock;

  beforeEach(() => {
    mock = createMockDrive({ dbDir: path.join(__dirname, ".mock_drive", "core") });
    mock.wipe();
  });

  it("bootstraps @mcpher/gas-fakes (ScriptApp.isFake, CacheService, Utilities)", () => {
    assert.ok(globalThis.CacheService && typeof globalThis.CacheService.getScriptCache === "function");
    assert.ok(globalThis.Utilities);
    assert.equal(globalThis.ScriptApp && globalThis.ScriptApp.isFake, true);
  });

  it("init returns null when index file id is missing", () => {
    const adapter = mock.adapter;
    assert.equal(SHARD_DB.init("", adapter), null);
  });

  it("init throws when ToolkitAdapter cannot read JSON", () => {
    assert.throws(() => SHARD_DB.init(INDEX_ID, {}), /ToolkitAdapter/);
  });

  it("add, save, and lookup by id and key with O(1) routing maps", () => {
    const DB = SHARD_DB.init(INDEX_ID, mock.adapter);
    DB.addToDB({ key: "a", id: 1, v: "x" }, { dbMain: "USERS" });
    DB.saveToDBFiles();

    assert.equal(DB.lookUpById(1, { dbMain: "USERS" }).v, "x");
    assert.equal(DB.lookUpByKey("a", { dbMain: "USERS" }).v, "x");

    const kf = DB.INDEX.USERS.properties.keyToFragment;
    assert.equal(kf.a, "USERS_1");
    const sorted = DB.INDEX.USERS.properties.idRangesSorted;
    assert.equal(sorted.length, 1);
    assert.equal(sorted[0].min, 1);
    assert.equal(sorted[0].max, 1);
    assert.equal(DB._routing.findFragmentForId(1, "USERS"), "USERS_1");
  });

  it("normalizes string ids that are numeric", () => {
    const DB = SHARD_DB.init(INDEX_ID, mock.adapter);
    DB.addToDB({ key: "k", id: "42", v: 1 }, { dbMain: "USERS" });
    const row = DB.lookUpById(42, { dbMain: "USERS" });
    assert.equal(row.id, 42);
  });

  it("throws on non-numeric id", () => {
    const DB = SHARD_DB.init(INDEX_ID, mock.adapter);
    assert.throws(() => DB.addToDB({ key: "x", id: "nope", v: 1 }, { dbMain: "USERS" }), /numeric id/);
  });

  it("creates a new cumulative fragment after MAX_ENTRIES_COUNT rows", () => {
    const DB = SHARD_DB.init(INDEX_ID, mock.adapter);
    const max = SHARD_DB.MAX_ENTRIES_COUNT;
    for (let i = 1; i <= max + 1; i++) {
      DB.addToDB({ key: "u" + i, id: i, n: i }, { dbMain: "USERS" });
    }
    DB.saveToDBFiles();

    const frags = DB.INDEX.USERS.properties.fragmentsList;
    assert.equal(frags.length, 2);
    assert.ok(frags.includes("USERS_1"));
    assert.ok(frags.includes("USERS_2"));

    assert.equal(DB.lookUpById(1, { dbMain: "USERS" }).n, 1);
    assert.equal(DB.lookUpById(max + 1, { dbMain: "USERS" }).n, max + 1);

    const sorted = DB.INDEX.USERS.properties.idRangesSorted;
    assert.ok(sorted.length >= 2);
    for (let i = 1; i < sorted.length; i++) {
      assert.ok(sorted[i].min >= sorted[i - 1].min);
    }
  });

  it("lookupByCriteria with id only touches one fragment (id fast path)", () => {
    const DB = SHARD_DB.init(INDEX_ID, mock.adapter);
    const max = SHARD_DB.MAX_ENTRIES_COUNT;
    for (let i = 1; i <= max + 5; i++) {
      DB.addToDB({ key: "u" + i, id: i, tag: i === 3 ? "hit" : "miss" }, { dbMain: "USERS" });
    }
    const openedBefore = Object.keys(DB.OPEN_DB).length;
    const rows = DB.lookupByCriteria([{ param: "id", criterion: 3 }], { dbMain: "USERS" });
    assert.equal(rows.length, 1);
    assert.equal(rows[0].tag, "hit");
    const openedAfter = Object.keys(DB.OPEN_DB).length;
    assert.ok(openedAfter <= openedBefore + 1);
  });

  it("delete by id shrinks id range and updates routing", () => {
    const DB = SHARD_DB.init(INDEX_ID, mock.adapter);
    DB.addToDB({ key: "a", id: 10, v: 1 }, { dbMain: "USERS" });
    DB.addToDB({ key: "b", id: 20, v: 2 }, { dbMain: "USERS" });
    DB.deleteFromDBById(10, { dbMain: "USERS" });
    assert.equal(DB.lookUpById(10, { dbMain: "USERS" }), null);
    assert.equal(DB.lookUpByKey("a", { dbMain: "USERS" }), null);
    const r = DB.INDEX.USERS.dbFragments.USERS_1.idRange;
    assert.equal(r.min, 20);
    assert.equal(r.max, 20);
  });

  it("migrates legacy keyQueryArray into keyToFragment on load", () => {
    mock.adapter.writeToJSON(INDEX_ID, {
      USERS: {
        properties: {
          cumulative: true,
          rootFolder: "f_id",
          filesPrefix: "chk",
          fragmentsList: ["USERS_1"],
          keyToFragment: {}
        },
        dbFragments: {
          USERS_1: {
            keyQueryArray: ["legacy_a", "legacy_b"],
            idRange: { min: 1, max: 2 },
            externalConfigs: {},
            ignoreIndex: false,
            fileId: "frag1"
          }
        }
      }
    });
    mock.adapter.writeToJSON("frag1", {
      index: { legacy_a: 1, legacy_b: 2 },
      data: {
        1: { key: "legacy_a", id: 1 },
        2: { key: "legacy_b", id: 2 }
      }
    });

    const DB = SHARD_DB.init(INDEX_ID, mock.adapter);
    assert.equal(DB.INDEX.USERS.properties.keyToFragment.legacy_a, "USERS_1");
    assert.equal(DB.INDEX.USERS.properties.keyToFragment.legacy_b, "USERS_1");
    assert.deepEqual(DB.INDEX.USERS.dbFragments.USERS_1.keyQueryArray, []);
  });

  it("clearDB on a fragment purges routing keys for that shard", () => {
    const DB = SHARD_DB.init(INDEX_ID, mock.adapter);
    DB.addToDB({ key: "z", id: 99, v: 1 }, { dbMain: "USERS" });
    DB.clearDB({ dbMain: "USERS", dbFragment: "USERS_1" });
    assert.equal(DB.INDEX.USERS.properties.keyToFragment.z, undefined);
    assert.equal(DB.INDEX.USERS.dbFragments.USERS_1.idRange.min, null);
  });

  it("ignoreIndex still routes by key via keyToFragment and data scan", () => {
    const DB = SHARD_DB.init(INDEX_ID, mock.adapter);
    DB.addToDB({ key: "first", id: 1, v: "a" }, { dbMain: "USERS" });
    DB.INDEX.USERS.dbFragments.USERS_1.ignoreIndex = true;
    DB.addToDB({ key: "hidden", id: 7, v: "ok" }, { dbMain: "USERS" });
    assert.equal(DB.lookUpByKey("hidden", { dbMain: "USERS" }).v, "ok");
    DB.deleteFromDBByKey("hidden", { dbMain: "USERS" });
    assert.equal(DB.lookUpByKey("hidden", { dbMain: "USERS" }), null);
  });

  it("getIndexFootprint scales with keyToFragment (master JSON size proxy)", () => {
    const DB = SHARD_DB.init(INDEX_ID, mock.adapter);
    DB.addToDB({ key: "a", id: 1, v: 1 }, { dbMain: "USERS" });
    DB.saveToDBFiles();
    const fp1 = DB.getIndexFootprint({ dbMain: "USERS" });
    assert.equal(fp1.keyToFragmentCount, 1);
    assert.ok(fp1.indexJsonBytes > 200);
    for (let i = 2; i <= 50; i++) {
      DB.addToDB({ key: "k" + i, id: i, v: i }, { dbMain: "USERS" });
    }
    DB.saveToDBFiles();
    const fp50 = DB.getIndexFootprint({ dbMain: "USERS" });
    assert.equal(fp50.keyToFragmentCount, 50);
    assert.ok(fp50.indexJsonBytes > fp1.indexJsonBytes);
  });

  it("validateRoutingConsistency passes on healthy db", () => {
    const DB = SHARD_DB.init(INDEX_ID, mock.adapter);
    DB.addToDB({ key: "a", id: 1, v: 1 }, { dbMain: "USERS" });
    DB.addToDB({ key: "b", id: 2, v: 2 }, { dbMain: "USERS" });
    DB.saveToDBFiles();
    const v = DB.validateRoutingConsistency({ dbMain: "USERS" });
    assert.equal(v.ok, true, v.errors.join("; "));
  });

  it("addToDB update after closeDB routes to the fragment that holds the id (not latest fragment)", () => {
    const DB = SHARD_DB.init(INDEX_ID, mock.adapter);
    const max = SHARD_DB.MAX_ENTRIES_COUNT;
    for (let i = 1; i <= max + 1; i++) {
      DB.addToDB({ key: "u" + i, id: i, n: i }, { dbMain: "USERS" });
    }
    DB.saveToDBFiles();
    DB.closeDB({ dbMain: "USERS" });
    DB.addToDB({ key: "u1", id: 1, n: 99 }, { dbMain: "USERS" });
    assert.equal(DB.lookUpById(1, { dbMain: "USERS" }).n, 99);
    const v = DB.validateRoutingConsistency({ dbMain: "USERS" });
    assert.equal(v.ok, true, v.errors.join("; "));
  });
});
