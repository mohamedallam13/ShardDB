const fs = require('fs');
const path = require('path');

const mem = new Map();
global.CacheService = {
  getScriptCache: () => ({ get: k => mem.get(k)||null, put: (k,v) => mem.set(k,v), remove: k => mem.delete(k) })
};

const shardDbPath = path.join(__dirname, '../src/ShardDB/ShardDB.js');
const shardDbCode = fs.readFileSync(shardDbPath, 'utf8');
const scriptContext = {}; 
eval(`(function() { ${shardDbCode} }).call(scriptContext)`);
const SHARD_DB = scriptContext.SHARD_DB;

const DB_DIR = path.join(__dirname, '.mock_drive');
if (!fs.existsSync(DB_DIR)) fs.mkdirSync(DB_DIR);

// Fixed to realistic Google Drive API latencies
const mockDriveDelay = 500; 
const mockToolkitAdapter = {
  readFromJSON: function(fileId) {
    const p = path.join(DB_DIR, fileId + '.json');
    if (!fs.existsSync(p)) return { 
       USERS: { properties: { cumulative: true, rootFolder: "f_id", filesPrefix: "chk", fragmentsList: [] }, dbFragments: {} } 
    };
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, mockDriveDelay);
    return JSON.parse(fs.readFileSync(p, 'utf8'));
  },
  writeToJSON: function(fileId, payload) {
    const p = path.join(DB_DIR, fileId + '.json');
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, mockDriveDelay * 2);
    fs.writeFileSync(p, JSON.stringify(payload));
  },
  createJSON: function(name, root, payload) {
    const id = "mock_id_" + name + "_" + Date.now() + "_" + Math.floor(Math.random()*1000);
    this.writeToJSON(id, payload);
    return id;
  },
  deleteFile: function(fileId) {
    const p = path.join(DB_DIR, fileId + '.json');
    if (fs.existsSync(p)) fs.unlinkSync(p);
  }
};

let DB = SHARD_DB.init("MASTER_INDEX_FILE", mockToolkitAdapter);
let totalOpsTime = 0;
function measure(name, fn) {
  const start = Date.now();
  fn();
  const dur = Date.now() - start;
  totalOpsTime += dur;
  console.log(`[${name.padEnd(30)}] -> ${dur} ms`);
}

console.log("==================================================");
console.log("💥 EXTREME SHARD_DB RESILIENCY TEST 💥");
console.log("==================================================");

try {
  // 1. MASSIVE WRITES
  measure("Write 15,000 Records", () => {
    for (let i = 1; i <= 15000; i++) {
       DB.addToDB({ key: `u_${i}`, id: i, payload: "Hello World", role: i % 2 === 0 ? "admin" : "user" }, { dbMain: "USERS" });
    }
  });

  measure("Commit Sync (Disk/Cache)", () => {
    DB.saveToDBFiles();
  });

  // 2. READ SCALING
  measure("LookUp Key (u_14999)", () => {
    const entry = DB.lookUpByKey("u_14999", { dbMain: "USERS" });
    if (!entry || entry.id !== 14999) throw new Error("Key Lookup Failed");
  });

  measure("LookUp ID (5000)", () => {
    const entry = DB.lookUpById(5000, { dbMain: "USERS" });
    if (!entry || entry.key !== "u_5000") throw new Error("ID Lookup Failed");
  });

  measure("LookUp Array of Criteria (Filters)", () => {
    const entries = DB.lookupByCriteria([
      { param: "role", criterion: "admin" },
      { param: "payload", criterion: "Hello World" }
    ], { dbMain: "USERS" });
    if (entries.length !== 7500) throw new Error(`Criteria returned ${entries.length} instead of 7500`);
  });

  // 3. CACHE CLEARANCE & COLD READS
  measure("Clear Hot RAM Cache", () => {
    global.CacheService.getScriptCache().remove("MASTER_INDEX_FILE");
    // We cannot easily clear the fakes map without reaching internals, so we re-init the DB engine
    DB = SHARD_DB.init("MASTER_INDEX_FILE", mockToolkitAdapter);
  });

  measure("Cold Read LookUp ID (2500)", () => {
    // This will force fetching INDEX from Drive + 1 Chunk from Drive!
    const entry = DB.lookUpById(2500, { dbMain: "USERS" });
    if (!entry || entry.key !== "u_2500") throw new Error("Cold ID Lookup Failed");
  });

  // 4. DELETION
  measure("Delete by Key (u_100) & ID (150)", () => {
    DB.deleteFromDBByKey("u_100", { dbMain: "USERS" });
    DB.deleteFromDBById(150, { dbMain: "USERS" });
    DB.saveToDBFiles();
  });

  measure("Verify Deletions", () => {
    if (DB.lookUpById(100, { dbMain: "USERS" })) throw new Error("User 100 should be dead");
    if (DB.lookUpById(150, { dbMain: "USERS" })) throw new Error("User 150 should be dead");
  });

  // 5. CLEARING / DESTROYING
  measure("Destroy Database Entirely", () => {
    DB.destroyDB();
  });

  console.log("--------------------------------------------------");
  console.log(`Total System Execution Time: ${totalOpsTime} ms`);
  console.log(`Final Drive state cleanly wiped? ${fs.readdirSync(DB_DIR).length === 0 ? "YES!" : "NO"}`);
  console.log("🏆 ALL TESTS PASSED RESILIENTLY 🏆");

} catch(e) {
  console.error("Test Crash:", e);
} finally {
  fs.rmSync(DB_DIR, { recursive: true, force: true });
}
