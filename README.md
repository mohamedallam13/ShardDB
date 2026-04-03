# ShardDB

A sharded, NoSQL document database that runs entirely on **Google Drive** — no external services, no billing, no infrastructure.

ShardDB splits your data across multiple Drive JSON files ("fragments") and maintains a master routing index so every lookup is fast regardless of how large your dataset grows. It is designed for Google Apps Script projects that need real persistence, real querying, and real scale — without leaving the Google ecosystem.

---

## Why this exists

Google Apps Script has no built-in database. The common workarounds all break down at scale:

| Approach | Breaks at |
|---|---|
| One big JSON file on Drive | ~5 MB — Drive read/write slows to a crawl |
| PropertiesService | 500 KB total, 9 KB per value |
| SpreadsheetApp as a DB | Slow, fragile, wrong tool |
| Firestore / external DB | Requires GCP project, billing, OAuth complexity |

ShardDB gives you a real database on infrastructure you already have.

---

## How it works

Your data is split across **fragment files** on Drive. A single **master INDEX file** holds the routing maps — so ShardDB always knows exactly which fragment holds any given row, without scanning every file.

```
Google Drive
│
├── MASTER_INDEX.json          ← read once on init; holds all routing
│     keyToFragment: { "alice" → "USERS_1", "bob" → "USERS_2" }
│     idRangesSorted: [{ fragment: "USERS_1", min: 1, max: 1000 }, ...]
│
├── USERS_1.json               ← fragment: up to 1000 rows
│     index: { "alice" → 1, "dave" → 2 }
│     data:  { 1 → { id:1, key:"alice", ... }, 2 → { ... } }
│
├── USERS_2.json               ← next fragment, same structure
└── ...
```

Fragments are loaded **lazily** — only the fragment containing the row you need is read from Drive. Everything else stays on disk.

---

## Performance (measured on real Google Drive)

These are real numbers from the GAS execution environment, not a mock:

| Dataset | Shards | Save (initial) | Reload (init) | Read all rows | Nominal write (per shard) | Nominal delete (per shard) |
|---|---|---|---|---|---|---|
| 100 rows | 1 | 3.2 s | 363 ms | 427 ms | < 1 ms | < 1 ms |
| 1 000 rows | 1 | 4.2 s | 326 ms | 409 ms | < 1 ms | < 1 ms |
| 10 000 rows | 10 | 17.7 s | 342 ms | 3.8 s | < 1 ms | 2–4 ms |
| 50 000 rows | 50 | 80.2 s | 745 ms | 19.8 s | < 2 ms | 9–14 ms |
| 100 000 rows | 100 | 159 s | 858 ms | 37.8 s | 1 ms | 32–38 ms |

**Key insight:** The bottleneck is always Drive I/O (one network call per fragment file), not ShardDB's routing logic. Individual reads and writes in memory are sub-millisecond at every scale. Reload is fast because only the INDEX file is read on `init()` — fragments load lazily.

### Complexity

| Operation | Cost |
|---|---|
| `lookUpByKey` | O(1) — hash map lookup |
| `lookUpById` | O(log F) — binary search, F = fragment count |
| `addToDB` in-place update | O(1) — no INDEX rewrite |
| `addToDB` new row | O(log F) |
| `deleteFromDB` | O(1) |
| `lookupByCriteria` with id | O(log F) — single fragment |
| `lookupByCriteria` without id | O(N) — full scan |
| `saveToDBFiles` (routing unchanged) | O(dirty fragments) — INDEX skipped |
| `saveToDBFiles` (routing changed) | O(dirty fragments + 1) |

---

## Quick start

### 1. Add the files to your Apps Script project

Copy these two files into your project:
- `src/ShardDB/ShardDB.js`
- `src/ShardDB/ShardDBToolkitHelpers.js`

Or add via [clasp](https://github.com/google/clasp).

### 2. Create an index file on Drive

```javascript
const folderId = "your-drive-folder-id";
const folder = DriveApp.getFolderById(folderId);

const initialIndex = {
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

const indexFile = folder.createFile(
  "MASTER_INDEX.json",
  JSON.stringify(initialIndex),
  "application/json"
);

Logger.log(indexFile.getId()); // save this ID
```

### 3. Initialize and use

```javascript
const adapter = SHARD_DB_TOOLKIT.createDriveToolkitAdapter();
const db = SHARD_DB.init("your-index-file-id", adapter);

// Add a row
db.addToDB(
  { key: "alice", id: 1, email: "alice@example.com", role: "admin" },
  { dbMain: "USERS" }
);

// Save to Drive
db.saveToDBFiles();

// Look up by key — O(1)
const user = db.lookUpByKey("alice", { dbMain: "USERS" });

// Look up by id — O(log F)
const user2 = db.lookUpById(1, { dbMain: "USERS" });

// Query by field
const admins = db.lookupByCriteria(
  [{ param: "role", criterion: "admin" }],
  { dbMain: "USERS" }
);

// Query nested fields
const activeL2 = db.lookupByCriteria(
  [
    { path: ["profile"], param: "status", criterion: "Active" },
    { path: ["profile", "metrics"], param: "clearance", criterion: "L2" }
  ],
  { dbMain: "USERS" }
);

// Delete
db.deleteFromDBByKey("alice", { dbMain: "USERS" });
db.deleteFromDBById(1, { dbMain: "USERS" });

// Save changes
db.saveToDBFiles();
```

---

## API reference

### Initialization

```javascript
const db = SHARD_DB.init(indexFileId, adapter, options);
```

| Parameter | Type | Description |
|---|---|---|
| `indexFileId` | string | Drive file ID of the master INDEX JSON |
| `adapter` | object | ToolkitAdapter — see Adapters section |
| `options.maxEntriesCount` | number | Rows per fragment (default: 1000) |

Returns `null` if `indexFileId` is falsy.

---

### Write

#### `addToDB(entry, ctx)`
Insert or update a row. If a row with the same `id` already exists, it is updated in place. If the `key` changed, the old key is evicted from routing automatically.

```javascript
db.addToDB({ key: "alice", id: 1, ...payload }, { dbMain: "USERS" });
```

#### `saveToDBFiles()`
Flush all dirty fragments to Drive. Writes the master INDEX only if routing metadata changed (new rows, deletes, key changes). Pure in-place value updates skip the INDEX write.

#### `saveIndex()`
Force-write the master INDEX regardless of dirty state.

---

### Read

#### `lookUpById(id, ctx)` → row | null
#### `lookUpByKey(key, ctx)` → row | null

#### `lookupByCriteria(criteria, ctx)` → row[]

Each criterion is `{ param, criterion }` or `{ path, param, criterion }`:

```javascript
// simple field
{ param: "status", criterion: "active" }

// nested object
{ path: ["profile"], param: "status", criterion: "active" }

// array of objects — scans all elements
{ path: ["profile", "tags"], param: "label", criterion: "admin" }

// function criterion
{ param: "score", criterion: (v) => v > 90 }
```

`id` criterion uses the O(log F) fast path — only one fragment is opened.

---

### Delete

#### `deleteFromDBById(id, ctx)`
#### `deleteFromDBByKey(key, ctx)`

Both remove the row from the fragment and clean up all routing maps. Call `saveToDBFiles()` afterward to persist.

---

### Lifecycle

#### `closeDB(ctx)` 
Evicts fragment(s) from memory. Next access re-reads from Drive. Useful for long-running scripts to control RAM.

#### `clearDB(ctx)`
Wipes all rows from a table (or single fragment) and resets routing. Keeps the fragment files on Drive.

#### `destroyDB(ctx)`
Deletes fragment files from Drive and removes the table from the INDEX. Irreversible.

---

### Utilities

#### `getIndexFootprint(ctx)` → `{ indexJsonBytes, keyToFragmentCount, fragmentsCount, ... }`
Returns the size of the master INDEX for a given table — useful for monitoring index growth.

#### `validateRoutingConsistency(ctx)` → `{ ok, errors[] }`
Checks that `keyToFragment`, `idRangesSorted`, and fragment data are all consistent with each other. Use this in tests and health checks.

#### `addExternalConfig(key, value, ctx)` / `getExternalConfig(key, ctx)`
Store arbitrary metadata on a fragment (e.g. schema version, timestamps).

---

## Multiple tables

One INDEX file can hold multiple tables. Each table is a top-level key in the INDEX:

```javascript
const initialIndex = {
  USERS:  { properties: { ..., filesPrefix: "usr" }, dbFragments: {} },
  ORDERS: { properties: { ..., filesPrefix: "ord" }, dbFragments: {} }
};

db.addToDB({ key: "u1", id: 1, name: "Alice" }, { dbMain: "USERS" });
db.addToDB({ key: "o1", id: 1, total: 49.99 },  { dbMain: "ORDERS" });
```

Tables are fully isolated — separate fragments, separate routing, no cross-contamination.

---

## Adapters

ShardDB is decoupled from Drive via an adapter interface. You provide the I/O layer; ShardDB handles the rest.

### Built-in: DriveToolkitAdapter

```javascript
const adapter = SHARD_DB_TOOLKIT.createDriveToolkitAdapter();
const db = SHARD_DB.init(indexFileId, adapter);
```

Uses `DriveApp` directly — no extra OAuth scopes beyond Drive.

### Built-in: Backup/Restore wrapper

Wraps any adapter. Every write is mirrored to a `.backup.json` file alongside the primary. On read failure, falls back to the backup automatically.

```javascript
const adapter = SHARD_DB_TOOLKIT.wrapWithBackupRestore(
  SHARD_DB_TOOLKIT.createDriveToolkitAdapter(),
  { enabled: true }
);
const db = SHARD_DB.init(indexFileId, adapter);
```

### Custom adapter

Implement four methods to use any storage backend:

```javascript
const adapter = {
  readFromJSON:  (fileId) => { /* return parsed object or null */ },
  writeToJSON:   (fileId, payload) => { /* write JSON */ },
  createJSON:    (name, folderId, payload) => { /* return new fileId */ },
  deleteFile:    (fileId) => { /* delete or trash */ }
};
```

---

## Tuning shard size

The default is 1000 rows per fragment. You can override it per instance:

```javascript
const db = SHARD_DB.init(indexFileId, adapter, { maxEntriesCount: 5000 });
```

**Larger shards** → fewer Drive API calls per save → faster saves.  
**Smaller shards** → each file is smaller → faster individual reads.

The Drive API call overhead (~300–600ms per file) dominates at all realistic sizes. If your workload writes many rows per session before saving, prefer larger shards. If your workload is read-heavy with lazy loads, smaller shards reduce the data parsed per access.

---

## Node.js test suite

The library ships with a full test suite that runs in Node using a mock Drive adapter — no Google account needed.

```bash
npm install
npm test           # 68 correctness tests
npm run test:heavy # 10k-row comprehensive test
npm run test:perf-matrix        # throughput benchmark (1k / 5k / 10k rows)
npm run test:perf-matrix:full   # + 50k and 100k
```

---

## Project structure

```
src/
  ShardDB/
    ShardDB.js                  ← core library (UMD, no dependencies)
    ShardDBToolkitHelpers.js    ← DriveApp adapter + backup wrapper
  tests/
    ShardDB-TestSuite.js        ← GAS test suite (run in Apps Script editor)
  appsscript.json

tests/                          ← Node.js test suite
  sharddb.test.js
  sharddb-correctness-gaps.test.js
  sharddb-operation-coverage.test.js
  sharddb-extended.test.js
  sharddb-10k-comprehensive.test.js
  helpers/
    mock-drive.js
    load-sharddb.js
    bootstrap-gas-fakes.js

docs/
  ARCHITECTURE.md               ← full architecture with Mermaid diagrams
```

---

## License

MIT
