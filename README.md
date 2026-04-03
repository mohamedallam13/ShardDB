<div align="center">

# ShardDB

**A real database for Google Apps Script — built on Drive, zero dependencies.**

ShardDB splits your data across sharded Drive files and routes every read in O(1).  
No GCP project. No billing. No external services. Just your Drive.

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Tests](https://img.shields.io/badge/tests-68%20passing-brightgreen)](#testing)
[![Platform](https://img.shields.io/badge/platform-Google%20Apps%20Script-yellow)](https://developers.google.com/apps-script)

</div>

---

## The problem

Google Apps Script has no built-in database. Every workaround hits a wall:

| What people do | Where it breaks |
|---|---|
| One big JSON file on Drive | ~5 MB — reads and writes crawl |
| `PropertiesService` | 500 KB total storage cap |
| Google Sheets as a DB | Slow, rate-limited, wrong abstraction |
| Firestore | GCP project + billing + OAuth setup |

ShardDB solves this by turning Drive itself into a sharded document store — infrastructure you already have, already pay for, already have access to.

---

## How it works

Your data is split across **fragment files**. A single **master INDEX** holds all routing — so ShardDB knows exactly which fragment holds any row without scanning every file.

```
Google Drive/
├── MASTER_INDEX.json          ← read once on init; never grows unbounded
│     keyToFragment: { alice → USERS_1, bob → USERS_2 }
│     idRangesSorted: [{ fragment: USERS_1, min: 1, max: 1000 }, ...]
│
├── USERS_1.json               ← shard: up to 1,000 rows
│     index: { alice → 1, dave → 2 }
│     data:  { 1: { id:1, key:"alice", ... } }
│
└── USERS_2.json               ← next shard, same structure
```

Fragments load **lazily** — only the shard containing the row you need is ever read from Drive. Everything else stays on disk.

---

## Performance

Real numbers. Real Drive. Real GAS execution environment.

| Dataset | Shards | Init (reload) | Lookup by key | Lookup by id | Write / delete per shard |
|---|---|---|---|---|---|
| 100 rows | 1 | **363 ms** | O(1) | O(log 1) | < 1 ms |
| 1 000 rows | 1 | **326 ms** | O(1) | O(log 1) | < 1 ms |
| 10 000 rows | 10 | **342 ms** | O(1) | O(log 10) | < 1 ms |
| 50 000 rows | 50 | **745 ms** | O(1) | O(log 50) | < 2 ms |
| 100 000 rows | 100 | **858 ms** | O(1) | O(log 100) | 1 ms |

**The bottleneck is always Drive I/O — not ShardDB.** Routing logic is in-memory and sub-millisecond at every scale. Init is fast because only the INDEX file is read — shards open lazily on first access.

### Complexity

| Operation | Cost | Notes |
|---|---|---|
| `lookUpByKey` | **O(1)** | Hash map — no Drive read if shard is cached |
| `lookUpById` | **O(log F)** | Binary search on F fragment ranges |
| `addToDB` update in-place | **O(1)** | Same id + same key — no INDEX rewrite |
| `addToDB` new row | O(log F) | Range check + possible shard rollover |
| `deleteFromDB` | **O(1)** | Direct eviction via routing maps |
| `saveToDBFiles` routing unchanged | O(dirty shards) | INDEX write skipped entirely |
| `saveToDBFiles` routing changed | O(dirty shards + 1) | +1 for INDEX write |

---

## Quick start

### 1. Add to your project

Copy into your Apps Script project:

```
src/ShardDB/ShardDB.js
src/ShardDB/ShardDBToolkitHelpers.js
```

Or use [clasp](https://github.com/google/clasp) to push from this repo directly.

### 2. Create the index file (one time)

```javascript
function setupShardDB() {
  const folderId = "your-drive-folder-id";
  const folder = DriveApp.getFolderById(folderId);

  const index = {
    USERS: {
      properties: {
        cumulative: true,
        rootFolder: folderId,
        filesPrefix: "usr",
        fragmentsList: [],
        keyToFragment: {},
        idRangesSorted: []
      },
      dbFragments: {}
    }
  };

  const file = folder.createFile("MASTER_INDEX.json", JSON.stringify(index), "application/json");
  Logger.log("Index file ID: " + file.getId()); // store this
}
```

### 3. Use it

```javascript
const adapter = SHARD_DB_TOOLKIT.createDriveToolkitAdapter();
const db = SHARD_DB.init("your-index-file-id", adapter);
const ctx = { dbMain: "USERS" };

// Write
db.addToDB({ key: "alice", id: 1, email: "alice@example.com", role: "admin" }, ctx);
db.saveToDBFiles();

// Read — O(1)
const user = db.lookUpByKey("alice", ctx);
const same = db.lookUpById(1, ctx);

// Query
const admins = db.lookupByCriteria([{ param: "role", criterion: "admin" }], ctx);

// Nested + array queries
const results = db.lookupByCriteria([
  { path: ["profile"], param: "status", criterion: "Active" },
  { path: ["profile", "tags"], param: "clearance", criterion: "L2" }
], ctx);

// Update — same id, same key: zero INDEX write overhead
db.addToDB({ key: "alice", id: 1, email: "new@example.com", role: "admin" }, ctx);
db.saveToDBFiles();

// Delete
db.deleteFromDBByKey("alice", ctx);
db.saveToDBFiles();
```

---

## API

### Init

```javascript
const db = SHARD_DB.init(indexFileId, adapter, options?);
```

| Param | Type | Description |
|---|---|---|
| `indexFileId` | `string` | Drive file ID of the master INDEX |
| `adapter` | `object` | I/O adapter — see [Adapters](#adapters) |
| `options.maxEntriesCount` | `number` | Rows per shard (default: `1000`) |

Returns `null` if `indexFileId` is falsy.

---

### Write

#### `db.addToDB(entry, ctx)`

Insert or update. If a row with the same `id` exists it is updated in place. If the `key` changed, the old key is evicted from all routing maps automatically.

```javascript
db.addToDB({ key: "alice", id: 1, role: "admin" }, { dbMain: "USERS" });
```

#### `db.saveToDBFiles()`

Flush all dirty shards to Drive. Skips the INDEX write entirely if no routing metadata changed — pure value updates are free at the routing layer.

#### `db.saveIndex()`

Force-write the INDEX regardless of dirty state.

---

### Read

#### `db.lookUpByKey(key, ctx)` → `row | null`
#### `db.lookUpById(id, ctx)` → `row | null`

#### `db.lookupByCriteria(criteria, ctx)` → `row[]`

```javascript
// Exact match
{ param: "status", criterion: "active" }

// Nested object
{ path: ["profile"], param: "status", criterion: "active" }

// Array of objects — all elements are scanned
{ path: ["profile", "tags"], param: "label", criterion: "admin" }

// Function predicate
{ param: "score", criterion: v => v > 90 }
```

Providing an `id` criterion uses the O(log F) fast path — only one shard is opened.

---

### Delete

#### `db.deleteFromDBById(id, ctx)`
#### `db.deleteFromDBByKey(key, ctx)`

Removes the row and evicts it from all routing maps. Call `saveToDBFiles()` to persist.

---

### Lifecycle

| Method | What it does |
|---|---|
| `db.closeDB(ctx)` | Evict shard(s) from memory. Next access re-reads from Drive. |
| `db.clearDB(ctx)` | Wipe all rows and reset routing. Files stay on Drive. |
| `db.destroyDB(ctx?)` | Delete shard files from Drive. Irreversible. |

---

### Utilities

#### `db.getIndexFootprint(ctx?)` → `{ indexJsonBytes, keyToFragmentCount, fragmentsCount, ... }`

Monitor INDEX growth over time. `keyToFragment` grows ~21 bytes per key — at 100k rows that's ~2.1 MB.

#### `db.validateRoutingConsistency(ctx)` → `{ ok, errors[] }`

Full consistency check across `keyToFragment`, `idRangesSorted`, and fragment data. Use in health checks and tests.

#### `db.addExternalConfig(key, value, ctx)` / `db.getExternalConfig(key, ctx)`

Attach arbitrary metadata to a shard — schema version, sync timestamps, feature flags.

---

## Multiple tables

One INDEX file holds any number of tables. Tables are fully isolated — separate shards, separate routing.

```javascript
const index = {
  USERS:  { properties: { ..., filesPrefix: "usr" }, dbFragments: {} },
  ORDERS: { properties: { ..., filesPrefix: "ord" }, dbFragments: {} }
};

db.addToDB({ key: "u1", id: 1, name: "Alice" },  { dbMain: "USERS" });
db.addToDB({ key: "o1", id: 1, total: 49.99 },   { dbMain: "ORDERS" });
```

---

## Adapters

ShardDB is fully decoupled from Drive. You supply the I/O layer — ShardDB handles everything else.

### DriveApp adapter (built-in)

```javascript
const adapter = SHARD_DB_TOOLKIT.createDriveToolkitAdapter();
```

Uses `DriveApp` directly. No extra scopes beyond `drive`.

### Backup/restore wrapper (built-in)

Mirrors every write to a `.backup.json` file. Falls back to the backup automatically on read failure.

```javascript
const adapter = SHARD_DB_TOOLKIT.wrapWithBackupRestore(
  SHARD_DB_TOOLKIT.createDriveToolkitAdapter(),
  { enabled: true }
);
```

### Custom adapter

Plug in any storage backend by implementing four methods:

```javascript
const adapter = {
  readFromJSON:  (fileId)              => parsedObject | null,
  writeToJSON:   (fileId, payload)     => void,
  createJSON:    (name, folderId, obj) => newFileId,
  deleteFile:    (fileId)              => void
};
```

---

## Tuning shard size

```javascript
const db = SHARD_DB.init(indexFileId, adapter, { maxEntriesCount: 5000 });
```

The default is `1000` rows per shard. The Drive API call cost (~300–600 ms per file) dominates at all sizes — so larger shards mean fewer calls per save, which is almost always better. Tune based on your write pattern: if you write many rows per session before saving, go larger.

---

## Testing

Ships with a full test suite. No Google account needed for Node tests.

```bash
npm install
npm test                          # 68 correctness tests
npm run test:heavy                # 10k-row end-to-end
npm run test:perf-matrix          # throughput at 1k / 5k / 10k
npm run test:perf-matrix:full     # + 50k and 100k
```

For GAS (real Drive): open the Apps Script editor and run `runShardDbAssertionSuite`. It covers correctness across all bug-fix cases and the full nominal-ops performance matrix at all 5 scales.

---

## Project structure

```
src/
  ShardDB/
    ShardDB.js                ← core engine — zero dependencies
    ShardDBToolkitHelpers.js  ← DriveApp adapter + backup wrapper
  tests/
    ShardDB-TestSuite.js      ← GAS test suite
  appsscript.json

tests/                        ← Node test suite
  sharddb.test.js
  sharddb-correctness-gaps.test.js
  sharddb-operation-coverage.test.js
  sharddb-extended.test.js
  sharddb-10k-comprehensive.test.js
  helpers/

docs/
  ARCHITECTURE.md             ← full architecture + Mermaid diagrams
```

---

## License

MIT
