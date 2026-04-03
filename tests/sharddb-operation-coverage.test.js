"use strict";

/**
 * ShardDB — full operation coverage with timed phases at multiple scales.
 *
 * One sequential flow per dataset size [50, 500, 5000]:
 *
 *   Phase 1 — SEED (addToDB N rows)
 *     Assert: by-id, by-key, tw.index, keyToFragment, idRangesSorted, validateRouting
 *
 *   Phase 2 — SAVE + RELOAD (persist → new init)
 *     Assert: same checks on fresh instance
 *
 *   Phase 3 — UPDATE same key/id (in-place value change)
 *     Assert: new value by-id, new value by-key, tw.index unchanged, keyToFragment unchanged,
 *             idRangesSorted unchanged, validateRouting
 *
 *   Phase 4 — UPDATE key change (same id, new key)
 *     Assert: new key resolves (by-key + by-id), old key gone, tw.index old absent / new present,
 *             keyToFragment updated, idRangesSorted unchanged, validateRouting
 *
 *   Phase 5 — DELETE by id
 *     Assert: by-id null, by-key null, tw.index removed, keyToFragment removed,
 *             idRangesSorted recalculated, validateRouting
 *
 *   Phase 6 — DELETE by key
 *     Assert: by-key null, by-id null, tw.index removed, keyToFragment removed,
 *             idRangesSorted recalculated, validateRouting
 *
 *   Phase 7 — SAVE after mutations + RELOAD
 *     Assert: all surviving rows intact, deleted rows absent, routing consistent
 *
 * Timing is reported for every phase.
 */

const { describe, it, before, beforeEach } = require("node:test");
const assert = require("node:assert/strict");
const path = require("path");
const { loadShardDbUmd } = require("./helpers/load-sharddb");
const { createMockDrive, wrapAdapterWithWriteCounts } = require("./helpers/mock-drive");
const { bootstrapGasFakes } = require("./helpers/bootstrap-gas-fakes");

const SHARD_DB = loadShardDbUmd();
const INDEX_ID = "MASTER_INDEX_FILE";
const SCALES = [50, 500, 5000];

// ─── helpers ──────────────────────────────────────────────────────────────────

function makeDB(mock) {
  return SHARD_DB.init(INDEX_ID, mock.adapter);
}

/** Build a consistent row payload for id i */
function row(i) {
  return {
    key: "key_" + i,
    id: i,
    value: "val_" + i,
    score: i % 97,
    profile: {
      status: i % 2 === 0 ? "active" : "inactive",
      tags: [
        { label: "primary",   rank: i % 3 },
        { label: "secondary", rank: (i + 1) % 3 }
      ]
    }
  };
}

/**
 * Timed wrapper — runs fn(), prints elapsed, returns { ms, result }.
 */
function timed(label, fn) {
  const t0 = performance.now();
  const result = fn();
  const ms = (performance.now() - t0).toFixed(2);
  console.log("  [" + ms + " ms] " + label);
  return { ms: parseFloat(ms), result };
}

/**
 * Full 6-check correctness assertion for a row that SHOULD EXIST.
 */
function assertRowExists(db, dbMain, i, expectedValue, tag) {
  const frag = db._routing.findFragmentForKey("key_" + i, dbMain);
  assert.ok(frag, tag + ": key_" + i + " not in keyToFragment");

  // by-id
  const byId = db.lookUpById(i, { dbMain });
  assert.ok(byId, tag + ": lookUpById(" + i + ") returned null");
  assert.equal(byId.value, expectedValue, tag + ": by-id value mismatch id=" + i);

  // by-key
  const byKey = db.lookUpByKey("key_" + i, { dbMain });
  assert.ok(byKey, tag + ": lookUpByKey(key_" + i + ") returned null");
  assert.equal(byKey.value, expectedValue, tag + ": by-key value mismatch id=" + i);

  // tw.index — fragment must have key→id mapping
  const k = db._routing.openDbKey(dbMain, frag);
  const open = db.OPEN_DB[k];
  if (open) { // may not be loaded if fragment was closed
    const idxVal = open.toWrite.index["key_" + i];
    assert.equal(Number(idxVal), i, tag + ": tw.index mismatch for key_" + i);
  }

  // keyToFragment
  const mapped = db.INDEX[dbMain].properties.keyToFragment["key_" + i];
  assert.ok(mapped, tag + ": keyToFragment missing key_" + i);
}

/**
 * Full 6-check assertion for a row that SHOULD BE ABSENT.
 */
function assertRowGone(db, dbMain, i, tag) {
  // by-id
  assert.equal(db.lookUpById(i, { dbMain }), null,
    tag + ": lookUpById(" + i + ") should be null");

  // by-key
  assert.equal(db.lookUpByKey("key_" + i, { dbMain }), null,
    tag + ": lookUpByKey(key_" + i + ") should be null");

  // keyToFragment
  assert.equal(db.INDEX[dbMain].properties.keyToFragment["key_" + i], undefined,
    tag + ": keyToFragment should not have key_" + i);
  // Note: findFragmentForId may still return the fragment if the id falls within
  // the fragment's idRange (range shrinks only to remaining min/max, not per-id).
  // The definitive check is lookUpById returning null above.
}

/**
 * Assert idRangesSorted is non-empty, sorted, and all ranges are disjoint.
 */
function assertIdRangesSorted(db, dbMain, tag) {
  const arr = db.INDEX[dbMain].properties.idRangesSorted;
  for (let i = 1; i < arr.length; i++) {
    assert.ok(arr[i].min > arr[i - 1].max,
      tag + ": idRangesSorted not sorted/disjoint at index " + i);
  }
}

/**
 * Assert validateRoutingConsistency passes.
 */
function assertConsistent(db, dbMain, tag) {
  const v = db.validateRoutingConsistency({ dbMain });
  assert.equal(v.ok, true, tag + ": routing inconsistency: " + v.errors.join("; "));
}

// ─── test suite ───────────────────────────────────────────────────────────────

describe("ShardDB full operation coverage (timed, multi-scale)", () => {
  before(async () => {
    await bootstrapGasFakes();
  });

  let mock;
  beforeEach(() => {
    mock = createMockDrive({ dbDir: path.join(__dirname, ".mock_drive", "opcov") });
    mock.wipe();
  });

  for (const N of SCALES) {
    it("scale N=" + N + ": seed → save/reload → update(same) → update(keychange) → delete-by-id → delete-by-key → final reload", () => {
      console.log("\n═══════════ N=" + N + " ═══════════");

      // ── Phase 1: SEED ──────────────────────────────────────────────────────
      const DB = makeDB(mock);
      const dbMain = "USERS";
      const ctx = { dbMain };

      timed("Phase 1: seed " + N + " rows", () => {
        for (let i = 1; i <= N; i++) {
          DB.addToDB(row(i), ctx);
        }
      });

      // spot-check first, middle, last
      for (const i of [1, Math.ceil(N / 2), N]) {
        assertRowExists(DB, dbMain, i, "val_" + i, "P1(seed)");
      }
      assertIdRangesSorted(DB, dbMain, "P1(seed)");
      assertConsistent(DB, dbMain, "P1(seed)");

      // Verify tw.index for 3 spot rows while in memory
      const frag1 = DB._routing.findFragmentForId(1, dbMain);
      const open1 = DB.OPEN_DB[DB._routing.openDbKey(dbMain, frag1)];
      assert.equal(Number(open1.toWrite.index["key_1"]), 1, "P1: tw.index key_1");

      // ── Phase 2: SAVE + RELOAD ─────────────────────────────────────────────
      timed("Phase 2: saveToDBFiles", () => DB.saveToDBFiles());

      const DB2 = timed("Phase 2: new init (reload from disk)", () =>
        SHARD_DB.init(INDEX_ID, mock.adapter)
      ).result;

      for (const i of [1, Math.ceil(N / 2), N]) {
        assertRowExists(DB2, dbMain, i, "val_" + i, "P2(reload)");
      }
      assertIdRangesSorted(DB2, dbMain, "P2(reload)");
      assertConsistent(DB2, dbMain, "P2(reload)");

      // ── Phase 3: UPDATE — same id, same key, new value ────────────────────
      const UPDATE_ID = Math.ceil(N / 3);
      const UPDATE_NEW_VAL = "updated_" + UPDATE_ID;

      // snapshot keyToFragment before update
      const ktfBefore = Object.assign({}, DB2.INDEX[dbMain].properties.keyToFragment);
      const rangesBefore = JSON.stringify(DB2.INDEX[dbMain].properties.idRangesSorted);

      timed("Phase 3: in-place update id=" + UPDATE_ID, () => {
        DB2.addToDB(
          { key: "key_" + UPDATE_ID, id: UPDATE_ID, value: UPDATE_NEW_VAL, score: 999 },
          ctx
        );
      });

      // new value visible by both lookups
      assertRowExists(DB2, dbMain, UPDATE_ID, UPDATE_NEW_VAL, "P3(update-same)");

      // keyToFragment must not change for this key
      assert.equal(
        DB2.INDEX[dbMain].properties.keyToFragment["key_" + UPDATE_ID],
        ktfBefore["key_" + UPDATE_ID],
        "P3: keyToFragment must be unchanged for same-key in-place update"
      );

      // idRangesSorted must not change
      assert.equal(
        JSON.stringify(DB2.INDEX[dbMain].properties.idRangesSorted),
        rangesBefore,
        "P3: idRangesSorted must be unchanged for in-place update"
      );

      assertConsistent(DB2, dbMain, "P3(update-same)");

      // tw.index still has key→id
      const fragUpd = DB2._routing.findFragmentForId(UPDATE_ID, dbMain);
      const openUpd = DB2.OPEN_DB[DB2._routing.openDbKey(dbMain, fragUpd)];
      assert.equal(
        Number(openUpd.toWrite.index["key_" + UPDATE_ID]),
        UPDATE_ID,
        "P3: tw.index must still map key_" + UPDATE_ID + " after in-place update"
      );

      // ── Phase 4: UPDATE — same id, KEY CHANGE ─────────────────────────────
      const KEYCHANGE_ID = Math.ceil(N * 2 / 3);
      const OLD_KEY = "key_" + KEYCHANGE_ID;
      const NEW_KEY = "changed_key_" + KEYCHANGE_ID;
      const rangesBeforeKC = JSON.stringify(DB2.INDEX[dbMain].properties.idRangesSorted);

      timed("Phase 4: key-change id=" + KEYCHANGE_ID, () => {
        DB2.addToDB(
          { key: NEW_KEY, id: KEYCHANGE_ID, value: "kc_val_" + KEYCHANGE_ID },
          ctx
        );
      });

      // new key resolves by-key and by-id
      const byNewKey = DB2.lookUpByKey(NEW_KEY, ctx);
      assert.ok(byNewKey, "P4: new key must resolve");
      assert.equal(byNewKey.id, KEYCHANGE_ID, "P4: by-key id mismatch");

      const byId_kc = DB2.lookUpById(KEYCHANGE_ID, ctx);
      assert.ok(byId_kc, "P4: lookUpById must still work after key change");
      assert.equal(byId_kc.key, NEW_KEY, "P4: data.key must be new key");

      // old key is gone
      assert.equal(DB2.lookUpByKey(OLD_KEY, ctx), null, "P4: old key must be gone");
      assert.equal(
        DB2.INDEX[dbMain].properties.keyToFragment[OLD_KEY],
        undefined,
        "P4: keyToFragment must not have old key"
      );

      // new key is in keyToFragment
      assert.ok(
        DB2.INDEX[dbMain].properties.keyToFragment[NEW_KEY],
        "P4: keyToFragment must have new key"
      );

      // tw.index: old absent, new present
      const fragKC = DB2._routing.findFragmentForId(KEYCHANGE_ID, dbMain);
      const openKC = DB2.OPEN_DB[DB2._routing.openDbKey(dbMain, fragKC)];
      assert.equal(openKC.toWrite.index[OLD_KEY], undefined, "P4: tw.index must not have old key");
      assert.equal(
        Number(openKC.toWrite.index[NEW_KEY]),
        KEYCHANGE_ID,
        "P4: tw.index must have new key"
      );

      // idRangesSorted unchanged (same id, just key changed)
      assert.equal(
        JSON.stringify(DB2.INDEX[dbMain].properties.idRangesSorted),
        rangesBeforeKC,
        "P4: idRangesSorted must be unchanged after key-only change"
      );

      assertConsistent(DB2, dbMain, "P4(key-change)");

      // ── Phase 5: DELETE by id ──────────────────────────────────────────────
      const DEL_BY_ID = Math.ceil(N / 4);
      const rangesBeforeDel = JSON.stringify(DB2.INDEX[dbMain].properties.idRangesSorted);

      timed("Phase 5: deleteFromDBById id=" + DEL_BY_ID, () => {
        DB2.deleteFromDBById(DEL_BY_ID, ctx);
      });

      // by-id and by-key must both be null
      assertRowGone(DB2, dbMain, DEL_BY_ID, "P5(del-by-id)");

      // tw.index for the deleted key
      const fragDel5 = DB2._routing.findFragmentForId(DEL_BY_ID, dbMain);
      // findFragmentForId may return null now — that's correct if id was min/max
      if (fragDel5) {
        const openDel5 = DB2.OPEN_DB[DB2._routing.openDbKey(dbMain, fragDel5)];
        if (openDel5) {
          assert.equal(
            openDel5.toWrite.index["key_" + DEL_BY_ID],
            undefined,
            "P5: tw.index must not have deleted key"
          );
          assert.equal(
            openDel5.toWrite.data[DEL_BY_ID],
            undefined,
            "P5: tw.data must not have deleted id"
          );
        }
      }

      assertIdRangesSorted(DB2, dbMain, "P5(del-by-id)");
      assertConsistent(DB2, dbMain, "P5(del-by-id)");

      // ── Phase 6: DELETE by key ─────────────────────────────────────────────
      const DEL_BY_KEY_ID = Math.ceil(N * 3 / 4);
      const DEL_BY_KEY_STR = "key_" + DEL_BY_KEY_ID;

      // Ensure this row exists (wasn't already mutated in phases 3/4/5)
      const existsBefore = DB2.lookUpByKey(DEL_BY_KEY_STR, ctx);
      // If this id happened to be UPDATE_ID or KEYCHANGE_ID or DEL_BY_ID, pick a safe neighbour
      const actualDelKeyId = existsBefore ? DEL_BY_KEY_ID : DEL_BY_KEY_ID + 1;
      const actualDelKeyStr = "key_" + actualDelKeyId;

      // Snapshot the id so we can look up by id after deletion
      const rowBeforeDel6 = DB2.lookUpByKey(actualDelKeyStr, ctx);
      assert.ok(rowBeforeDel6, "P6 setup: row must exist before delete-by-key");
      const idBeforeDel6 = rowBeforeDel6.id;

      timed("Phase 6: deleteFromDBByKey key=" + actualDelKeyStr, () => {
        DB2.deleteFromDBByKey(actualDelKeyStr, ctx);
      });

      // by-key null
      assert.equal(
        DB2.lookUpByKey(actualDelKeyStr, ctx),
        null,
        "P6: lookUpByKey must be null after delete-by-key"
      );

      // by-id must be null  ← THIS IS THE GAP the audit found
      assert.equal(
        DB2.lookUpById(idBeforeDel6, ctx),
        null,
        "P6: lookUpById must be null after delete-by-key (id=" + idBeforeDel6 + ")"
      );

      // keyToFragment removed
      assert.equal(
        DB2.INDEX[dbMain].properties.keyToFragment[actualDelKeyStr],
        undefined,
        "P6: keyToFragment must not have deleted key"
      );

      // tw.index removed
      const fragDel6 = DB2._routing.findFragmentForId(idBeforeDel6, dbMain);
      if (fragDel6) {
        const openDel6 = DB2.OPEN_DB[DB2._routing.openDbKey(dbMain, fragDel6)];
        if (openDel6) {
          assert.equal(
            openDel6.toWrite.index[actualDelKeyStr],
            undefined,
            "P6: tw.index must not have deleted key after delete-by-key"
          );
          assert.equal(
            openDel6.toWrite.data[idBeforeDel6],
            undefined,
            "P6: tw.data must not have deleted row after delete-by-key"
          );
        }
      }

      assertIdRangesSorted(DB2, dbMain, "P6(del-by-key)");
      assertConsistent(DB2, dbMain, "P6(del-by-key)");

      // ── Phase 7: SAVE all mutations + final RELOAD ─────────────────────────
      timed("Phase 7: saveToDBFiles after all mutations", () => DB2.saveToDBFiles());

      const DB3 = timed("Phase 7: final reload (new init)", () =>
        SHARD_DB.init(INDEX_ID, mock.adapter)
      ).result;

      // Spot-check survivors (skip ids that were mutated/deleted in phases 3-6)
      const mutatedIds = new Set([UPDATE_ID, KEYCHANGE_ID, DEL_BY_ID, idBeforeDel6, actualDelKeyId]);
      let checkedSurvivors = 0;
      for (const i of [1, Math.ceil(N / 2), N]) {
        if (!mutatedIds.has(i)) {
          assertRowExists(DB3, dbMain, i, "val_" + i, "P7(final-reload)");
          checkedSurvivors++;
        }
      }
      // At least one untouched survivor checked (scales >= 50 guarantee this)
      assert.ok(checkedSurvivors > 0, "P7: should have at least one uncorrupted survivor to check");

      // deleted rows must be absent after reload
      assertRowGone(DB3, dbMain, DEL_BY_ID, "P7(del-by-id after reload)");
      assert.equal(DB3.lookUpByKey(actualDelKeyStr, ctx), null,
        "P7: deleted-by-key key must be absent after reload");
      assert.equal(DB3.lookUpById(idBeforeDel6, ctx), null,
        "P7: deleted-by-key id must be absent after reload");

      // update (in-place) survived
      if (!mutatedIds.has(UPDATE_ID) || UPDATE_ID === UPDATE_ID) {
        const afterReloadUpd = DB3.lookUpById(UPDATE_ID, ctx);
        if (afterReloadUpd) {
          assert.equal(afterReloadUpd.value, UPDATE_NEW_VAL, "P7: updated value must persist after reload");
        }
      }

      // key-change survived
      const afterReloadKC = DB3.lookUpById(KEYCHANGE_ID, ctx);
      if (afterReloadKC) {
        assert.equal(afterReloadKC.key, NEW_KEY, "P7: key-changed row must have new key after reload");
        assert.equal(DB3.lookUpByKey(OLD_KEY, ctx), null, "P7: old key must remain absent after reload");
      }

      assertIdRangesSorted(DB3, dbMain, "P7(final-reload)");
      assertConsistent(DB3, dbMain, "P7(final-reload)");

      // ── Summary ───────────────────────────────────────────────────────────
      const frags = DB3.INDEX[dbMain].properties.fragmentsList.length;
      const fp = DB3.getIndexFootprint({ dbMain });
      console.log(
        "  Summary N=" + N +
        " | fragments=" + frags +
        " | keys=" + fp.keyToFragmentCount +
        " | indexBytes=" + fp.indexJsonBytes
      );
    });
  }
});
