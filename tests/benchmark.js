"use strict";

const fs = require("fs");
const path = require("path");
const { loadShardDbUmd } = require("./helpers/load-sharddb");
const { bootstrapGasFakes } = require("./helpers/bootstrap-gas-fakes");

const SHARD_DB = loadShardDbUmd();

async function main() {
  await bootstrapGasFakes();

const DB_DIR = path.join(__dirname, ".mock_drive", "benchmark");
if (!fs.existsSync(DB_DIR)) fs.mkdirSync(DB_DIR, { recursive: true });

const mockDriveDelay = 15;

const mockToolkitAdapter = {
  readFromJSON: function (fileId) {
    const p = path.join(DB_DIR, fileId + ".json");
    if (!fs.existsSync(p)) {
      return {
        USERS: {
          properties: {
            cumulative: true,
            rootFolder: "f_id",
            filesPrefix: "chk",
            fragmentsList: [],
            keyToFragment: {},
            idRangesSorted: []
          },
          dbFragments: {}
        }
      };
    }
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, mockDriveDelay);
    return JSON.parse(fs.readFileSync(p, "utf8"));
  },
  writeToJSON: function (fileId, payload) {
    const p = path.join(DB_DIR, fileId + ".json");
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, mockDriveDelay * 2);
    fs.writeFileSync(p, JSON.stringify(payload));
  },
  createJSON: function (name, root, payload) {
    const id = "mock_id_" + name + "_" + Date.now();
    this.writeToJSON(id, payload);
    return id;
  },
  deleteFile: function (fileId) {
    const p = path.join(DB_DIR, fileId + ".json");
    if (fs.existsSync(p)) fs.unlinkSync(p);
  }
};

console.log("==================================================");
console.log("ShardDB node benchmark (mock Drive)");
console.log("==================================================");

let totalOpsTime = 0;
function measure(name, fn) {
  const start = Date.now();
  fn();
  const dur = Date.now() - start;
  totalOpsTime += dur;
  console.log("[" + name.padEnd(25) + "] -> " + dur + " ms");
}

const DB_INDEX = "MASTER_INDEX_FILE";
let DB;

try {
  measure("Engine Initialization", () => {
    DB = SHARD_DB.init(DB_INDEX, mockToolkitAdapter);
  });

  measure("Write 3,500 Sequential Users (RAM)", () => {
    for (let i = 1000; i < 4500; i++) {
      DB.addToDB(
        { key: "usr_" + i, id: i, email: "test" + i + "@shard.com", role: "developer" },
        { dbMain: "USERS" }
      );
    }
  });

  measure("Commit Sync (Mock Disk + RAM)", () => {
    DB.saveToDBFiles();
  });

  measure("Hot Fetch User via OPEN_DB", () => {
    const res = DB.lookUpById(3500, { dbMain: "USERS" });
    if (!res || res.id !== 3500) throw new Error("Lookup failed");
  });

  measure("Criteria filter (scan)", () => {
    const res = DB.lookupByCriteria([{ param: "email", criterion: "test4120@shard.com" }], { dbMain: "USERS" });
    if (res.length !== 1) throw new Error("Criteria failed");
  });

  console.log("--------------------------------------------------");
  console.log("Total Operations Execution Time: " + totalOpsTime + " ms");
  console.log("Fragments Created on Disk: " + fs.readdirSync(DB_DIR).length);
  console.log("==================================================");
} catch (err) {
  console.error("Benchmark crash:", err);
  process.exit(1);
}

fs.rmSync(DB_DIR, { recursive: true, force: true });
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
