# ShardDB — Agent Instructions

This file is read by every AI tool that works in this repository.
Follow these rules without exception.

---

## What this repo is

ShardDB is a sharded NoSQL document database for Google Apps Script, layered over Google Drive.
It has no runtime dependencies. The core library is two files:

- `src/ShardDB/ShardDB.js` — core engine (UMD module, exposes `SHARD_DB`)
- `src/ShardDB/ShardDBToolkitHelpers.js` — Drive adapter + backup wrapper (exposes `SHARD_DB_TOOLKIT`)

The GAS test suite lives in `src/tests/ShardDB-TestSuite.js`.
The Node test suite lives in `tests/`.

---

## Repo layout

```
src/
  ShardDB/
    ShardDB.js                  ← CORE — edit with extreme care
    ShardDBToolkitHelpers.js    ← adapter layer
  tests/
    ShardDB-TestSuite.js        ← GAS tests (runs in Apps Script editor)
  appsscript.json

tests/                          ← Node tests (npm test)
  sharddb.test.js
  sharddb-correctness-gaps.test.js
  sharddb-operation-coverage.test.js
  sharddb-extended.test.js
  sharddb-10k-comprehensive.test.js
  sharddb-perf-matrix.js        ← standalone benchmark (not in npm test)
  helpers/
    mock-drive.js
    load-sharddb.js
    bootstrap-gas-fakes.js

docs/
  ARCHITECTURE.md               ← architecture + Mermaid diagrams
  MANUAL_TESTING.md

README.md
AGENTS.md                       ← this file
gas-package.json
package.json
.clasp.json                     ← points to the GAS script
```

---

## Ground rules

### 1. Never break the public API

These function signatures are stable and must not change without a major version bump:

```
SHARD_DB.init(indexFileId, adapter, options?)
db.addToDB(entry, ctx)
db.lookUpById(id, ctx)
db.lookUpByKey(key, ctx)
db.lookupByCriteria(criteria, ctx)
db.deleteFromDBById(id, ctx)
db.deleteFromDBByKey(key, ctx)
db.saveToDBFiles()
db.saveIndex()
db.closeDB(ctx)
db.clearDB(ctx)
db.destroyDB(ctx?)
db.getIndexFootprint(ctx?)
db.validateRoutingConsistency(ctx)
db.addExternalConfig(key, value, ctx)
db.getExternalConfig(key, ctx)
```

`ctx` is always `{ dbMain, dbFragment? }`.

### 2. Run the Node test suite before and after any change to ShardDB.js

```bash
npm test
```

All 68 tests must pass. If you add new behaviour, add a test for it.

For large-scale correctness: `npm run test:heavy`
For performance baselines: `npm run test:perf-matrix`

### 3. The GAS test suite is the ground truth for real Drive behaviour

`src/tests/ShardDB-TestSuite.js` — entry function is `runShardDbAssertionSuite`.

It runs:
- Groups A–G: correctness assertions for all known bugs and edge cases
- Group H: 7-phase nominal-ops flow at scales 100 / 1k / 10k / 50k / 100k
  with full correctness checks and timing output

After any change to `ShardDB.js`, push to GAS and run `runShardDbAssertionSuite`.
Paste the execution log output here for review.

Push command (requires cc clasp credentials):
```bash
clasp -A "$HOME/.clasp/cc.clasprc.json" push
```

### 4. OPEN_DB keys use a composite format

`OPEN_DB` is keyed by `dbMain + "\x00" + dbFragment`, not by `dbFragment` alone.
Always use `openDbKey(dbMain, dbFragment)` — never access `OPEN_DB[fragmentName]` directly.
This prevents silent collision when two tables share the same fragment suffix.

### 5. indexRoutingDirty controls whether the INDEX is rewritten on save

Set it `true` when: new row, key change, fragment created/destroyed, clearDB, destroyDB, addExternalConfig.
Leave it `false` for: pure in-place value update (same id + same key, already routed to correct fragment).

Do not set this flag unconditionally — the entire point of this optimisation is to avoid
rewriting the INDEX on every save when routing hasn't changed.

### 6. Fragment files are the unit of Drive I/O

One Drive API call per fragment per save. This is the dominant cost (~300–600ms per call on GAS).
Design changes that affect how many fragments are written on a save have a direct performance impact.

### 7. Adapter interface

Any custom adapter must implement exactly these four methods:

```javascript
{
  readFromJSON:  (fileId) => object | null,
  writeToJSON:   (fileId, payload) => void,
  createJSON:    (name, folderId, payload) => fileId,
  deleteFile:    (fileId) => void
}
```

The mock adapter in `tests/helpers/mock-drive.js` is the reference implementation for Node tests.
The Drive adapter in `ShardDBToolkitHelpers.js` is the reference for production GAS use.

---

## Key data structures

### Master INDEX (persisted as one Drive JSON file)

```javascript
{
  USERS: {
    properties: {
      cumulative: true,
      rootFolder: "drive-folder-id",
      filesPrefix: "usr",
      fragmentsList: ["USERS_1", "USERS_2"],
      keyToFragment: { "alice": "USERS_1", "bob": "USERS_2" },
      idRangesSorted: [
        { fragment: "USERS_1", min: 1, max: 1000 },
        { fragment: "USERS_2", min: 1001, max: 2000 }
      ],
      indexRoutingDirty: false
    },
    dbFragments: {
      USERS_1: { fileId: "drive-file-id", idRange: { min: 1, max: 1000 } },
      USERS_2: { fileId: "drive-file-id", idRange: { min: 1001, max: 2000 } }
    }
  }
}
```

### Fragment file (each is a separate Drive JSON file)

```javascript
{
  index: { "alice": 1, "dave": 2 },   // key → id
  data:  { "1": { id:1, key:"alice", ... }, "2": { ... } }  // id → row
}
```

### OPEN_DB (in-memory only, never persisted directly)

```javascript
{
  "USERS\x00USERS_1": {
    properties: { isChanged: true, main: "USERS", fragment: "USERS_1" },
    toWrite: { index: {...}, data: {...} }
  }
}
```

---

## Known constraints

- **GAS execution time limit**: 6 minutes. At 100k rows / 100 fragments, a full save takes ~2.5 minutes. Design workflows that save incrementally or in batches.
- **Drive API quotas**: ~100 read/write operations per 100 seconds for consumer accounts. Shared drives have higher limits.
- **Index file size**: `keyToFragment` grows ~21 bytes per key. At 100k rows the INDEX is ~2.1 MB — still well within Drive's limits but worth monitoring with `getIndexFootprint()`.
- **`lookupByCriteria` without `id`**: Full scan across all fragments in the table. Avoid on large datasets without narrowing via `id` first.
- **Numeric ids required**: All ids must be finite numbers. String ids that are numeric are normalised automatically. Non-numeric strings throw.

---

## Versioning and releases

- Bump `version` in `gas-package.json` for any change that affects GAS behaviour.
- Bump `version` in `package.json` for Node tooling changes.
- Keep both in sync for full releases.
- Document breaking changes in a `CHANGELOG.md` if one is introduced.

---

## Pushing to GitHub

This repo is a subtree extracted from a private monorepo.
Push updates using subtree push from the parent repo, or push directly if working in a standalone clone.

Direct push (from this repo root if cloned standalone):
```bash
git push origin main
```

Subtree push (from the parent Atlas monorepo):
```bash
git subtree push --prefix=Infrastructure/Libraries/JSONDatabase sharddb main
```
