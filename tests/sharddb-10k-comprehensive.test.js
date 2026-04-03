"use strict";

/**
 * Heavy suite: seed 10,000 rows, exercise every public method on the object returned by
 * SHARD_DB.init() (see ShardDB.js return block), then clear + destroy + mock wipe.
 *
 * Index size: keyToFragment has one property per live key (O(keys)) — at 10k rows expect
 * ~10k routing entries; this is inherent unless the storage model changes.
 *
 * Run: npm run test:heavy
 */

const { describe, it, before, afterEach } = require("node:test");
const assert = require("node:assert/strict");
const path = require("path");
const { loadShardDbUmd } = require("./helpers/load-sharddb");
const { createMockDrive, wrapAdapterWithWriteCounts } = require("./helpers/mock-drive");
const { bootstrapGasFakes } = require("./helpers/bootstrap-gas-fakes");

const SHARD_DB = loadShardDbUmd();
const INDEX_ID = "MASTER_INDEX_FILE";
const SEED = 10000;

describe("ShardDB 10k comprehensive API", () => {
  before(async () => {
    await bootstrapGasFakes();
  });

  let mock;

  afterEach(() => {
    if (mock && mock.wipe) mock.wipe();
  });

  it(
    "seeds 10k rows then exercises addToDB, lookups, criteria, external config, deletes, save, close, clear, destroy",
    { timeout: 240000 },
    () => {
      mock = createMockDrive({ dbDir: path.join(__dirname, ".mock_drive", "10k") });
      mock.wipe();

      const wrapped = wrapAdapterWithWriteCounts(mock.adapter, { indexFileId: INDEX_ID });
      const adapter = wrapped.adapter;

      // ---- init ----
      const DB = SHARD_DB.init(INDEX_ID, adapter);
      assert.ok(DB);
      assert.ok(typeof DB.addToDB === "function");
      assert.ok(typeof DB.lookUpByKey === "function");
      assert.ok(typeof DB.lookUpById === "function");
      assert.ok(typeof DB.lookupByCriteria === "function");
      assert.ok(typeof DB.deleteFromDBByKey === "function");
      assert.ok(typeof DB.deleteFromDBById === "function");
      assert.ok(typeof DB.saveToDBFiles === "function");
      assert.ok(typeof DB.saveIndex === "function");
      assert.ok(typeof DB.closeDB === "function");
      assert.ok(typeof DB.clearDB === "function");
      assert.ok(typeof DB.destroyDB === "function");
      assert.ok(typeof DB.getExternalConfig === "function");
      assert.ok(typeof DB.addExternalConfig === "function");

      const chunk = SHARD_DB.MAX_ENTRIES_COUNT;
      const expectedFrags = Math.ceil(SEED / chunk);

      // ---- addToDB (seed) ----
      for (let i = 1; i <= SEED; i++) {
        DB.addToDB(
          {
            key: "k_" + i,
            id: i,
            email: "u" + i + "@seed.test",
            tag: i % 7
          },
          { dbMain: "USERS" }
        );
      }

      // Index footprint: one keyToFragment entry per key (expected at scale).
      assert.equal(Object.keys(DB.INDEX.USERS.properties.keyToFragment).length, SEED);
      assert.equal(DB.INDEX.USERS.properties.fragmentsList.length, expectedFrags);
      assertIdRangesSorted(DB, "USERS");

      // ---- saveToDBFiles ----
      DB.saveToDBFiles();
      assert.equal(DB.OPEN_DB.USERS_1.properties.isChanged, false);

      // ---- lookUpByKey ----
      assert.equal(DB.lookUpByKey("k_1", { dbMain: "USERS" }).email, "u1@seed.test");
      assert.equal(DB.lookUpByKey("k_5000", { dbMain: "USERS" }).id, 5000);
      assert.equal(DB.lookUpByKey("k_" + SEED, { dbMain: "USERS" }).tag, SEED % 7);

      // ---- lookUpById ----
      assert.equal(DB.lookUpById(1, { dbMain: "USERS" }).key, "k_1");
      assert.equal(DB.lookUpById(7777, { dbMain: "USERS" }).email, "u7777@seed.test");
      assert.equal(DB.lookUpById(SEED, { dbMain: "USERS" }).key, "k_" + SEED);

      // ---- lookupByCriteria (id fast path + full scan) ----
      const byId = DB.lookupByCriteria([{ param: "id", criterion: 4242 }], { dbMain: "USERS" });
      assert.equal(byId.length, 1);
      assert.equal(byId[0].email, "u4242@seed.test");

      const byEmail = DB.lookupByCriteria([{ param: "email", criterion: "u9000@seed.test" }], {
        dbMain: "USERS"
      });
      assert.equal(byEmail.length, 1);
      assert.equal(byEmail[0].id, 9000);

      const byTag = DB.lookupByCriteria([{ param: "tag", criterion: 3 }], { dbMain: "USERS" });
      assert.ok(byTag.length > 0);
      assert.equal(byTag[0].tag, 3);

      // ---- addExternalConfig / getExternalConfig ----
      DB.addExternalConfig("meta", { seeded: true, at: SEED }, { dbMain: "USERS", dbFragment: "USERS_1" });
      assert.deepEqual(DB.getExternalConfig("meta", { dbMain: "USERS", dbFragment: "USERS_1" }), {
        seeded: true,
        at: SEED
      });

      // ---- deleteFromDBByKey / deleteFromDBById ----
      DB.deleteFromDBByKey("k_100", { dbMain: "USERS" });
      DB.deleteFromDBByKey("k_200", { dbMain: "USERS" });
      DB.deleteFromDBById(300, { dbMain: "USERS" });
      DB.deleteFromDBById(400, { dbMain: "USERS" });
      assert.ok(!DB.lookUpByKey("k_100", { dbMain: "USERS" }));
      assert.ok(!DB.lookUpById(300, { dbMain: "USERS" }));
      assert.equal(Object.keys(DB.INDEX.USERS.properties.keyToFragment).length, SEED - 4);

      // ---- saveToDBFiles + saveIndex (explicit) ----
      DB.saveToDBFiles();
      DB.saveIndex();

      // ---- closeDB: drop OPEN_DB, next read reloads from mock files ----
      DB.closeDB({ dbMain: "USERS" });
      assert.equal(Object.keys(DB.OPEN_DB).length, 0);
      const afterClose = DB.lookUpById(5000, { dbMain: "USERS" });
      assert.ok(afterClose);
      assert.equal(afterClose.email, "u5000@seed.test");
      const frag5 = DB._routing.findFragmentForId(5000, "USERS");
      assert.equal(frag5, "USERS_5");
      assert.ok(DB.OPEN_DB.USERS_5);

      // ---- clearDB: wipe shard data + routing for USERS ----
      DB.clearDB({ dbMain: "USERS" });
      assert.equal(Object.keys(DB.INDEX.USERS.properties.keyToFragment).length, 0);
      assert.ok(!DB.lookUpById(5000, { dbMain: "USERS" }));

      // ---- destroyDB: remove fragment files / index entries for dbMain ----
      DB.destroyDB({ dbMain: "USERS" });
      assert.equal(DB.INDEX.USERS.properties.fragmentsList.length, 0);
      assert.equal(Object.keys(DB.INDEX.USERS.dbFragments).length, 0);

      wrapped.reset();
      mock.wipe();
    }
  );
});

function assertIdRangesSorted(DB, dbMain) {
  const arr = DB.INDEX[dbMain].properties.idRangesSorted;
  for (let i = 1; i < arr.length; i++) {
    assert.ok(arr[i].min >= arr[i - 1].min);
  }
}
