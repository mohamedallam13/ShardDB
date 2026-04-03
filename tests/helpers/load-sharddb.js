"use strict";

const fs = require("fs");
const path = require("path");

function loadShardDbUmd() {
  const shardDbPath = path.join(__dirname, "../../src/ShardDB/ShardDB.js");
  const shardDbCode = fs.readFileSync(shardDbPath, "utf8");
  const scriptContext = {};
  // UMD attaches SHARD_DB to `this` of the outer IIFE — must match Apps Script eval pattern.
  const fn = new Function(
    "scriptContext",
    "shardDbCode",
    '(function() { eval(shardDbCode); }).call(scriptContext); return scriptContext.SHARD_DB;'
  );
  return fn(scriptContext, shardDbCode);
}

module.exports = { loadShardDbUmd };
