"use strict";

/**
 * Deeper tests: OPEN_DB + dirty flag, persistence across close, destroy/clear,
 * routing invariants, external config, multi-fragment id routing.
 */

const { describe, it, before, beforeEach } = require("node:test");
const assert = require("node:assert/strict");
const path = require("path");
const { loadShardDbUmd } = require("./helpers/load-sharddb");
const { createMockDrive, wrapAdapterWithWriteCounts } = require("./helpers/mock-drive");
const { bootstrapGasFakes } = require("./helpers/bootstrap-gas-fakes");

const SHARD_DB = loadShardDbUmd();
const INDEX_ID = "MASTER_INDEX_FILE";

function assertIdRangesSorted(DB, dbMain) {
  const arr = DB.INDEX[dbMain].properties.idRangesSorted;
  for (let i = 1; i < arr.length; i++) {
    assert.ok(arr[i].min >= arr[i - 1].min, "idRangesSorted must be non-decreasing by min");
  }
}

describe("ShardDB extended", () => {
  before(async () => {
    await bootstrapGasFakes();
  });

  let mock;

  beforeEach(() => {
    mock = createMockDrive({ dbDir: path.join(__dirname, ".mock_drive", "extended") });
    mock.wipe();
  });

  describe("OPEN_DB and isChanged", () => {
    it("sets isChanged on write and clears after saveToDBFiles", () => {
      const DB = SHARD_DB.init(INDEX_ID, mock.adapter);
      DB.addToDB({ key: "a", id: 1, v: 1 }, { dbMain: "USERS" });
      assert.equal(DB.OPEN_DB[DB._routing.openDbKey("USERS", "USERS_1")].properties.isChanged, true);
      DB.saveToDBFiles();
      assert.equal(DB.OPEN_DB[DB._routing.openDbKey("USERS", "USERS_1")].properties.isChanged, false);
    });

    it("saveToDBFiles with no dirty fragments performs no adapter writes", () => {
      const wrapped = wrapAdapterWithWriteCounts(mock.adapter, { indexFileId: INDEX_ID });
      const DB = SHARD_DB.init(INDEX_ID, wrapped.adapter);
      DB.saveToDBFiles();
      const c = wrapped.counts();
      assert.equal(c.writeCount, 0, "no disk write if nothing changed");
    });

    it("closeDB drops OPEN_DB entry; next lookup reloads fragment from disk", () => {
      const DB = SHARD_DB.init(INDEX_ID, mock.adapter);
      DB.addToDB({ key: "a", id: 1, v: "persisted" }, { dbMain: "USERS" });
      DB.saveToDBFiles();
      assert.ok(DB.OPEN_DB[DB._routing.openDbKey("USERS", "USERS_1")]);
      DB.closeDB({ dbMain: "USERS" });
      assert.equal(DB.OPEN_DB[DB._routing.openDbKey("USERS", "USERS_1")], undefined);
      const row = DB.lookUpById(1, { dbMain: "USERS" });
      assert.equal(row.v, "persisted");
      assert.ok(DB.OPEN_DB[DB._routing.openDbKey("USERS", "USERS_1")]);
    });
  });

  describe("Routing invariants", () => {
    it("maintains sorted idRangesSorted after many inserts across three fragments", () => {
      const DB = SHARD_DB.init(INDEX_ID, mock.adapter);
      const cap = SHARD_DB.MAX_ENTRIES_COUNT;
      const total = cap * 2 + 50;
      for (let i = 1; i <= total; i++) {
        DB.addToDB({ key: "k" + i, id: i, n: i }, { dbMain: "USERS" });
      }
      assertIdRangesSorted(DB, "USERS");
      assert.equal(DB.INDEX.USERS.properties.fragmentsList.length, 3);
      assert.equal(DB._routing.findFragmentForId(1, "USERS"), "USERS_1");
      assert.equal(DB._routing.findFragmentForId(cap, "USERS"), "USERS_1");
      assert.equal(DB._routing.findFragmentForId(cap + 1, "USERS"), "USERS_2");
      assert.equal(DB._routing.findFragmentForId(total, "USERS"), "USERS_3");
    });

    it("keyToFragment size tracks keys added minus deleted", () => {
      const DB = SHARD_DB.init(INDEX_ID, mock.adapter);
      for (let i = 1; i <= 20; i++) {
        DB.addToDB({ key: "x" + i, id: i, n: i }, { dbMain: "USERS" });
      }
      assert.equal(Object.keys(DB.INDEX.USERS.properties.keyToFragment).length, 20);
      DB.deleteFromDBByKey("x5", { dbMain: "USERS" });
      assert.equal(Object.keys(DB.INDEX.USERS.properties.keyToFragment).length, 19);
      assert.equal(DB.INDEX.USERS.properties.keyToFragment.x5, undefined);
    });
  });

  describe("destroyDB / clearDB", () => {
    it("destroyDB(dbMain) removes all fragment files and leaves dbMain entry empty", () => {
      const DB = SHARD_DB.init(INDEX_ID, mock.adapter);
      DB.addToDB({ key: "a", id: 1, v: 1 }, { dbMain: "USERS" });
      DB.saveToDBFiles();
      assert.ok(DB.INDEX.USERS.dbFragments.USERS_1.fileId);
      DB.destroyDB({ dbMain: "USERS" });
      assert.equal(DB.INDEX.USERS.properties.fragmentsList.length, 0);
      assert.equal(Object.keys(DB.INDEX.USERS.dbFragments).length, 0);
    });

    it("clearDBMain empties data and purges keyToFragment", () => {
      const DB = SHARD_DB.init(INDEX_ID, mock.adapter);
      DB.addToDB({ key: "a", id: 1, v: 1 }, { dbMain: "USERS" });
      DB.clearDB({ dbMain: "USERS" });
      assert.equal(Object.keys(DB.INDEX.USERS.properties.keyToFragment).length, 0);
      assert.equal(DB.lookUpById(1, { dbMain: "USERS" }), null);
    });
  });

  describe("explicit dbFragment", () => {
    it("writes to a named fragment when cumulative is false", () => {
      const payload = {
        ITEMS: {
          properties: {
            cumulative: false,
            rootFolder: "f_id",
            filesPrefix: "it",
            fragmentsList: ["ITEMS_custom"],
            keyToFragment: {},
            idRangesSorted: []
          },
          dbFragments: {
            ITEMS_custom: {
              keyQueryArray: [],
              idRange: { min: null, max: null },
              externalConfigs: {},
              ignoreIndex: false,
              fileId: ""
            }
          }
        }
      };
      mock.adapter.writeToJSON(INDEX_ID, payload);
      const DB = SHARD_DB.init(INDEX_ID, mock.adapter);
      DB.addToDB({ key: "k1", id: 100, v: "here" }, { dbMain: "ITEMS", dbFragment: "ITEMS_custom" });
      assert.equal(DB.INDEX.ITEMS.properties.keyToFragment.k1, "ITEMS_custom");
      assert.equal(DB.lookUpByKey("k1", { dbMain: "ITEMS" }).v, "here");
    });
  });

  describe("externalConfigs", () => {
    it("round-trips externalConfigs on a fragment", () => {
      const DB = SHARD_DB.init(INDEX_ID, mock.adapter);
      DB.addToDB({ key: "a", id: 1, v: 1 }, { dbMain: "USERS" });
      DB.addExternalConfig("ttl", 3600, { dbMain: "USERS", dbFragment: "USERS_1" });
      assert.equal(DB.getExternalConfig("ttl", { dbMain: "USERS", dbFragment: "USERS_1" }), 3600);
    });
  });

  describe("Multi-table INDEX", () => {
    it("keeps USERS and ORDERS routing separate", () => {
      mock.adapter.writeToJSON(INDEX_ID, {
        USERS: {
          properties: {
            cumulative: true,
            rootFolder: "f_id",
            filesPrefix: "u",
            fragmentsList: [],
            keyToFragment: {},
            idRangesSorted: []
          },
          dbFragments: {}
        },
        ORDERS: {
          properties: {
            cumulative: true,
            rootFolder: "f_id",
            filesPrefix: "ord",
            fragmentsList: [],
            keyToFragment: {},
            idRangesSorted: []
          },
          dbFragments: {}
        }
      });
      const DB = SHARD_DB.init(INDEX_ID, mock.adapter);
      DB.addToDB({ key: "u1", id: 1, t: "user" }, { dbMain: "USERS" });
      DB.addToDB({ key: "o1", id: 1, t: "order" }, { dbMain: "ORDERS" });
      assert.equal(DB.lookUpByKey("u1", { dbMain: "USERS" }).t, "user");
      assert.equal(DB.lookUpByKey("o1", { dbMain: "ORDERS" }).t, "order");
    });
  });

  describe("lookupByCriteria", () => {
    it("filters by nested path and function criterion", () => {
      const DB = SHARD_DB.init(INDEX_ID, mock.adapter);
      DB.addToDB(
        { key: "a", id: 1, profile: { role: "admin", score: 10 } },
        { dbMain: "USERS" }
      );
      DB.addToDB(
        { key: "b", id: 2, profile: { role: "user", score: 99 } },
        { dbMain: "USERS" }
      );
      const rows = DB.lookupByCriteria(
        [
          { path: ["profile"], param: "role", criterion: "user" },
          { path: ["profile"], param: "score", criterion: (n) => n > 50 }
        ],
        { dbMain: "USERS" }
      );
      assert.equal(rows.length, 1);
      assert.equal(rows[0].key, "b");
    });
  });

  describe("Stress (moderate)", () => {
    it("500 sequential ids remain consistent for key and id lookup", () => {
      const DB = SHARD_DB.init(INDEX_ID, mock.adapter);
      for (let i = 1; i <= 500; i++) {
        DB.addToDB({ key: "idemp_" + i, id: i, n: i }, { dbMain: "USERS" });
      }
      for (let j = 1; j <= 500; j += 17) {
        assert.equal(DB.lookUpById(j, { dbMain: "USERS" }).n, j);
        assert.equal(DB.lookUpByKey("idemp_" + j, { dbMain: "USERS" }).n, j);
      }
      assertIdRangesSorted(DB, "USERS");
    });
  });
});
