# ShardDB — Manual Testing in Apps Script

All tests run directly in the Apps Script IDE against real Google Drive.
Test files land in the **ShardDBTestJSONs** folder (`1E_7mgRa6Pub901rpR-BescRuita0Gkb_`).

---

## Entry points

Open the ShardDB Apps Script project and run any of these from the function dropdown:

| Function | What it does | Time |
|---|---|---|
| `runShardDbVisualDriveTestSuite` | 1500 rows, lookups, one delete — basic sanity check | ~30s |
| `runShardDbFullBenchmarkSuite` | Full API coverage + sequences + perf matrix at n=100, 1000, 10000 | ~3–5 min |
| `runShardDbFullBenchmarkSuiteWith100k` | Same but adds n=100000 — may hit the 6-min execution limit | ~10 min |

---

## What the benchmark suite does

1. **Full API coverage** — hits every public method: `addToDB`, `lookUpById`, `lookUpByKey`, `lookupByCriteria` (flat + nested path), `deleteFromDBByKey`, `deleteFromDBById`, `saveToDBFiles`, `saveIndex`, `closeDB`, `clearDB`, `destroyDB`, `addExternalConfig`, `getExternalConfig`, `getIndexFootprint`, `validateRoutingConsistency`

2. **Sequence scenarios** — save → close → cold lookup → update → criteria → delete

3. **Perf matrix** — for each n in `[100, 1000, 10000]`:
   - Seeds n rows with nested profile payloads
   - Times: seed, save, lookUpById (mid), lookupByCriteria (2 nested criteria), close + cold lookUpById
   - Logs INDEX JSON size (tracks keyToFragment growth)
   - Runs `validateRoutingConsistency` — must pass
   - Calls `destroyDB` to clean up shard files

---

## Output

**Logger** — every timed step prints `[ms=X] label`. Look for:
- `csv_row,...` lines — copy these for comparison
- `routing_consistency n=X OK` — routing must be OK at every size
- `index_growth n=X master_INDEX_json_bytes≈...` — tracks INDEX size growth

**Drive file** — one `SHARD_PERF_REPORT_<timestamp>.json` is created in ShardDBTestJSONs and kept after the run. Everything else (temp INDEX files, shard fragment files) is trashed automatically.

Share the report JSON or paste the `plainTextSummary` block from it when comparing builds.

---

## What to look for

| Metric | Healthy sign |
|---|---|
| `routing_ok=1` at all sizes | Routing invariants hold |
| `save_ms` grows with fragment count, not row count | O(fragments) write cost |
| `byId` stays flat across sizes | O(log F) binary search working |
| `close_lookup_ms` ≈ one Drive read latency | Cold fragment load cost |
| `index_json_bytes` grows linearly with n | Expected — O(keys) in keyToFragment |
| `legacy_kqa=0` | No legacy keyQueryArray cruft |

---

## Bugs fixed before this push

| Bug | Impact |
|---|---|
| `lookUpEntriesByCriteria` returned rows missing the filtered field | Criteria queries returned wrong results |
| `rebuildIdRangeSorted` called on every insert unconditionally | Wasted O(F log F) on in-place updates |
| `deleteIdEntriesInFragment` scanned entire fragment index | O(N) delete became O(1) |
| `validateRoutingConsistency` used `Math.min.apply` | Would throw on fragments > ~65k rows |
| `addToDB` returned wrong `this` | Misleading return value |
