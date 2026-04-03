"use strict";

/**
 * indexRoutingDirty: skip master INDEX write when only fragment payload changes (same id+key routing).
 * Also verifies key-change updates remove old key from routing and lookups stay consistent.
 */

const { describe, it, beforeEach } = require("node:test");
const assert = require("node:assert/strict");
const path = require("path");
const { loadShardDbUmd } = require("./helpers/load-sharddb");
const { createMockDrive, wrapAdapterWithWriteCounts } = require("./helpers/mock-drive");

const SHARD_DB = loadShardDbUmd();
const INDEX_ID = "IDX_ROUTING";

describe("ShardDB indexRoutingDirty", () => {
  let mock;
  let wrapped;

  beforeEach(() => {
    mock = createMockDrive({ dbDir: path.join(__dirname, ".mock_drive", "routing_dirty") });
    mock.wipe();
    wrapped = wrapAdapterWithWriteCounts(mock.adapter, { indexFileId: INDEX_ID });
  });

  it("saveToDBFiles skips master INDEX write on pure payload update (same id + key)", () => {
    const DB = SHARD_DB.init(INDEX_ID, wrapped.adapter);
    DB.addToDB({ key: "a", id: 1, v: 1 }, { dbMain: "USERS" });
    DB.saveToDBFiles();
    assert.equal(DB.INDEX.USERS.properties.indexRoutingDirty, false);
    wrapped.reset();

    DB.addToDB({ key: "a", id: 1, v: 2 }, { dbMain: "USERS" });
    DB.saveToDBFiles();
    const c = wrapped.counts();
    assert.equal(c.fragmentWriteCount, 1, "fragment should flush");
    assert.equal(c.indexWriteCount, 0, "master INDEX should be skipped on pure update");
    assert.equal(DB.lookUpById(1, { dbMain: "USERS" }).v, 2);
    assert.equal(DB.INDEX.USERS.properties.indexRoutingDirty, false);
  });

  it("saveToDBFiles writes INDEX on new row insert", () => {
    const DB = SHARD_DB.init(INDEX_ID, wrapped.adapter);
    DB.addToDB({ key: "x", id: 1, n: 1 }, { dbMain: "USERS" });
    assert.equal(DB.INDEX.USERS.properties.indexRoutingDirty, true);
    assert.equal(DB.lookUpByKey("x", { dbMain: "USERS" }).n, 1);
    assert.equal(DB.lookUpById(1, { dbMain: "USERS" }).key, "x");
    wrapped.reset();
    DB.saveToDBFiles();
    const c = wrapped.counts();
    assert.equal(c.fragmentWriteCount, 1, "new fragment file via createJSON should count as one fragment write");
    assert.equal(c.indexWriteCount, 1, "routing metadata changed; master INDEX must persist");
    assert.equal(DB.INDEX.USERS.properties.indexRoutingDirty, false);
    assert.equal(DB.lookUpByKey("x", { dbMain: "USERS" }).n, 1);
    assert.equal(DB.lookUpById(1, { dbMain: "USERS" }).n, 1);
    DB.closeDB({ dbMain: "USERS" });
    const DB2 = SHARD_DB.init(INDEX_ID, wrapped.adapter);
    assert.equal(DB2.lookUpByKey("x", { dbMain: "USERS" }).n, 1);
    assert.equal(DB2.lookUpById(1, { dbMain: "USERS" }).key, "x");
    assert.equal(DB2.validateRoutingConsistency({ dbMain: "USERS" }).ok, true);
  });

  it("key change on update removes old key from routing and lookUpByKey uses new key", () => {
    const DB = SHARD_DB.init(INDEX_ID, wrapped.adapter);
    DB.addToDB({ key: "oldK", id: 5, v: 1 }, { dbMain: "USERS" });
    DB.saveToDBFiles();
    assert.equal(DB.lookUpByKey("oldK", { dbMain: "USERS" }).v, 1);

    DB.addToDB({ key: "newK", id: 5, v: 2 }, { dbMain: "USERS" });
    assert.equal(DB.lookUpByKey("oldK", { dbMain: "USERS" }), null);
    assert.equal(DB.lookUpByKey("newK", { dbMain: "USERS" }).v, 2);
    assert.equal(DB.INDEX.USERS.properties.keyToFragment.oldK, undefined);
    assert.equal(DB.INDEX.USERS.properties.keyToFragment.newK, "USERS_1");
    DB.saveToDBFiles();
    const v = DB.validateRoutingConsistency({ dbMain: "USERS" });
    assert.equal(v.ok, true, v.errors.join("; "));
  });

  it("explicit saveIndex always persists and clears indexRoutingDirty", () => {
    const DB = SHARD_DB.init(INDEX_ID, wrapped.adapter);
    DB.addToDB({ key: "a", id: 1, v: 1 }, { dbMain: "USERS" });
    DB.saveToDBFiles();
    wrapped.reset();
    DB.addToDB({ key: "a", id: 1, v: 9 }, { dbMain: "USERS" });
    DB.saveIndex();
    assert.equal(wrapped.counts().indexWriteCount, 1);
    assert.equal(DB.INDEX.USERS.properties.indexRoutingDirty, false);
  });
});
