"use strict";

/**
 * Verifies @mcpher/gas-fakes is loaded and coexists with ShardDB in the same Node process.
 * Run as part of `npm test` (listed first so the environment is validated early).
 *
 * gas-fakes uses the same Drive backend as Apps Script when credentials are configured;
 * these tests use the filesystem mock for ShardDB I/O — no real Drive calls required.
 */

const { describe, it, before } = require("node:test");
const assert = require("node:assert/strict");
const path = require("path");
const { bootstrapGasFakes } = require("./helpers/bootstrap-gas-fakes");
const { loadShardDbUmd } = require("./helpers/load-sharddb");
const { createMockDrive } = require("./helpers/mock-drive");

const SHARD_DB = loadShardDbUmd();
const INDEX_ID = "MASTER_INDEX";

describe("gas-fakes + ShardDB", () => {
  before(async () => {
    await bootstrapGasFakes();
  });

  it("exposes Apps Script globals (DriveApp, CacheService, ScriptApp.isFake)", () => {
    assert.ok(globalThis.DriveApp && typeof globalThis.DriveApp.getFileById === "function");
    assert.ok(globalThis.CacheService && typeof globalThis.CacheService.getScriptCache === "function");
    assert.ok(globalThis.Utilities);
    assert.equal(globalThis.ScriptApp && globalThis.ScriptApp.isFake, true);
  });

  it("runs ShardDB against the mock adapter while gas-fakes globals are loaded", () => {
    const mock = createMockDrive({ dbDir: path.join(__dirname, ".mock_drive", "gas-fakes") });
    mock.wipe();
    const DB = SHARD_DB.init(INDEX_ID, mock.adapter);
    DB.addToDB({ key: "gf", id: 1, note: "gas-fakes coexistence" }, { dbMain: "USERS" });
    DB.saveToDBFiles();
    assert.equal(DB.lookUpById(1, { dbMain: "USERS" }).note, "gas-fakes coexistence");
    mock.wipe();
  });
});
