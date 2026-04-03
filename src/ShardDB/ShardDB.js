;(function (root, factory) {
  root.SHARD_DB = factory()
})(this, function () {

  /**
   * Architecture (index size vs speed vs OPEN_DB)
   *
   * **OPEN_DB** — Map of opened fragments only. Each entry holds `toWrite` (working copy of
   * `{ index, data }`) and `properties.isChanged`. **saveToDBFiles()** writes dirty fragments
   * to Drive and persists the master INDEX; then clears `isChanged`. **closeDB()** drops the
   * in-memory entry; the next access re-reads the fragment JSON via the adapter (no unbounded
   * RAM for all shards unless you keep them all open).
   *
   * **Master INDEX JSON** — Must store enough to route without scanning every shard: we keep
   * **keyToFragment** (one entry per live **key**) and **idRangesSorted** (small list, one row
   * per fragment with data). Total routing metadata is still **O(keys)** for the key map;
   * that is inherent if keys are opaque strings and shards are opaque — you cannot know
   * “which shard holds key K?” without storing K (or a deterministic function of K such as
   * hash→shard with no per-key record). What we **did** remove is the **duplicate** copy of
   * every key in per-fragment **keyQueryArray** inside the same INDEX file (legacy), so the
   * master JSON does not blow up as **2× key list** anymore; fragment files still hold their
   * own `index` map for persistence.
   *
   * **Speed** — Key routing: O(1) object lookup. Id routing: O(log F) binary search on F
   * fragments. Criteria without id: still scans documents in opened / iterated shards.
   *
   * **indexRoutingDirty** — When `false`, a `saveToDBFiles()` that only flushes fragment JSON may skip
   * rewriting the master INDEX (no change to keyToFragment, fragments, id ranges on disk). Set `true`
   * on any routing metadata mutation. Pure in-place row updates (same id + same key + same fragment)
   * clear the need for an INDEX write until you call `saveIndex()` explicitly.
   *
   * **Drive I/O** — Pass any ToolkitAdapter with readFromJSON / writeToJSON / createJSON /
   * deleteFile. This library includes **SHARD_DB_TOOLKIT** in `ShardDBToolkitHelpers.js`
   * (DriveApp-only adapter + optional backup wrapper). AtlasToolkit or Advanced Drive are optional.
   */

  const MAX_ENTRIES_COUNT = 1000;

  class OpenDBEntry {
    constructor(dbMain, fragmentFileObj) {
      this.properties = { isChanged: false, main: dbMain };
      this.toWrite = fragmentFileObj || { index: {}, data: {} };
    }
  }

  class IndexEntry {
    constructor() {
      /** @deprecated Kept for legacy persisted indexes; routing uses properties.keyToFragment */
      this.keyQueryArray = [];
      this.idRange = { min: null, max: null };
      this.externalConfigs = {};
      this.ignoreIndex = false;
      this.fileId = "";
    }
  }

  /**
   * All document ids are normalized to finite numbers (JSON keys remain stringified).
   */
  function normalizeId(id) {
    const n = typeof id === "number" ? id : Number(id);
    if (!Number.isFinite(n)) {
      throw new Error("ShardDB requires a finite numeric id.");
    }
    return n;
  }

  function normalizeKey(key) {
    return String(key);
  }

  function init(indexFileId, ToolkitAdapter) {
    if (!indexFileId) return null;
    if (!ToolkitAdapter || typeof ToolkitAdapter.readFromJSON !== "function") {
      throw new Error("JSONDatabase requires ToolkitAdapter to execute Drive I/O.");
    }

    function initiateDB() {
      return ToolkitAdapter.readFromJSON(indexFileId);
    }

    const INDEX = initiateDB();
    const OPEN_DB = {};

    function ensureDbMainRouting(dbMain) {
      const props = INDEX[dbMain].properties;
      if (!props.keyToFragment) props.keyToFragment = {};
      if (!props.idRangesSorted) props.idRangesSorted = [];
    }

    function markIndexRoutingDirty(dbMain) {
      if (!INDEX[dbMain] || !INDEX[dbMain].properties) return;
      INDEX[dbMain].properties.indexRoutingDirty = true;
    }

    function clearIndexRoutingDirtyAfterPersist() {
      Object.keys(INDEX).forEach(function (dm) {
        const p = INDEX[dm].properties;
        if (p) p.indexRoutingDirty = false;
      });
    }

    /**
     * Sorted disjoint id ranges for O(log F) fragment routing (F = fragment count).
     */
    function rebuildIdRangeSorted(dbMain) {
      ensureDbMainRouting(dbMain);
      const { dbFragments } = INDEX[dbMain];
      const arr = [];
      Object.keys(dbFragments).forEach(function (f) {
        const r = dbFragments[f].idRange;
        if (r && r.min !== null && r.max !== null) {
          var lo = Number(r.min);
          var hi = Number(r.max);
          if (Number.isFinite(lo) && Number.isFinite(hi)) {
            arr.push({ fragment: f, min: lo, max: hi });
          }
        }
      });
      arr.sort(function (a, b) {
        return a.min - b.min;
      });
      INDEX[dbMain].properties.idRangesSorted = arr;
    }

    function findFragmentForId(id, dbMain) {
      const n = normalizeId(id);
      ensureDbMainRouting(dbMain);
      const arr = INDEX[dbMain].properties.idRangesSorted;
      if (!arr || arr.length === 0) return null;
      var lo = 0;
      var hi = arr.length - 1;
      while (lo <= hi) {
        var mid = (lo + hi) >> 1;
        var r = arr[mid];
        var rmin = Number(r.min);
        var rmax = Number(r.max);
        if (n < rmin) hi = mid - 1;
        else if (n > rmax) lo = mid + 1;
        else return r.fragment;
      }
      return null;
    }

    function findFragmentForKey(key, dbMain) {
      ensureDbMainRouting(dbMain);
      const k = normalizeKey(key);
      const frag = INDEX[dbMain].properties.keyToFragment[k];
      return frag || null;
    }

    /**
     * Migrate legacy per-fragment keyQueryArray into a single dbMain.properties.keyToFragment map,
     * then clear keyQueryArray to reduce index payload size on next save.
     */
    function migrateLegacyRouting(dbMain) {
      if (!INDEX[dbMain] || !INDEX[dbMain].properties || !INDEX[dbMain].dbFragments) return false;
      ensureDbMainRouting(dbMain);
      const kf = INDEX[dbMain].properties.keyToFragment;
      const { dbFragments } = INDEX[dbMain];
      var changed = false;
      Object.keys(dbFragments).forEach(function (frag) {
        const entry = dbFragments[frag];
        const legacy = entry.keyQueryArray;
        if (legacy && legacy.length) {
          for (var i = 0; i < legacy.length; i++) {
            kf[normalizeKey(legacy[i])] = frag;
          }
          entry.keyQueryArray = [];
          changed = true;
        }
      });
      rebuildIdRangeSorted(dbMain);
      return changed;
    }

    function migrateAllIndexes() {
      Object.keys(INDEX).forEach(function (dbMain) {
        const node = INDEX[dbMain];
        if (!node || !node.properties || !node.dbFragments) return;
        if (migrateLegacyRouting(dbMain)) {
          markIndexRoutingDirty(dbMain);
        }
      });
    }

    function removeKeysFromRouting(dbMain, keys) {
      ensureDbMainRouting(dbMain);
      const kf = INDEX[dbMain].properties.keyToFragment;
      if (!Array.isArray(keys)) keys = [keys];
      for (var i = 0; i < keys.length; i++) {
        delete kf[normalizeKey(keys[i])];
      }
    }

    function purgeRoutingKeysForFragment(dbMain, dbFragment) {
      ensureDbMainRouting(dbMain);
      const kf = INDEX[dbMain].properties.keyToFragment;
      const keys = Object.keys(kf);
      for (var i = 0; i < keys.length; i++) {
        if (kf[keys[i]] === dbFragment) delete kf[keys[i]];
      }
    }

    function recalcFragmentIdRangeAfterMutation(dbMain, dbFragment) {
      const open = OPEN_DB[dbFragment];
      if (!open) {
        rebuildIdRangeSorted(dbMain);
        return;
      }
      const data = open.toWrite.data;
      var ids = Object.keys(data).map(function (k) {
        return normalizeId(k);
      });
      if (ids.length === 0) {
        INDEX[dbMain].dbFragments[dbFragment].idRange = { min: null, max: null };
      } else {
        var min = ids[0];
        var max = ids[0];
        for (var i = 1; i < ids.length; i++) {
          if (ids[i] < min) min = ids[i];
          if (ids[i] > max) max = ids[i];
        }
        INDEX[dbMain].dbFragments[dbFragment].idRange = { min: min, max: max };
      }
      rebuildIdRangeSorted(dbMain);
    }

    migrateAllIndexes();

    Object.keys(INDEX).forEach(function (dm) {
      const p = INDEX[dm].properties;
      if (p && p.indexRoutingDirty === undefined) p.indexRoutingDirty = false;
    });

    function saveIndex() {
      ToolkitAdapter.writeToJSON(indexFileId, INDEX);
      clearIndexRoutingDirtyAfterPersist();
    }

    function closeDB({ dbMain, dbFragment }) {
      if (!dbFragment) closeDBMain(dbMain);
      else closeFragment(dbFragment);
    }

    function closeDBMain(dbMain) {
      const { fragmentsList } = INDEX[dbMain].properties;
      fragmentsList.forEach(closeFragment);
    }

    function closeFragment(dbFragment) {
      if (OPEN_DB[dbFragment]) delete OPEN_DB[dbFragment];
    }

    function clearDB({ dbMain, dbFragment }) {
      if (!dbFragment) clearDBMain(dbMain);
      else clearFragment(dbMain, dbFragment);
      saveIndex();
    }

    function clearDBMain(dbMain) {
      const { fragmentsList } = INDEX[dbMain].properties;
      fragmentsList.forEach(function (dbFragment) {
        clearFragment(dbMain, dbFragment);
      });
    }

    function clearFragment(dbMain, dbFragment) {
      const { fileId } = INDEX[dbMain].dbFragments[dbFragment];
      if (fileId) ToolkitAdapter.writeToJSON(fileId, {});
      INDEX[dbMain].dbFragments[dbFragment].keyQueryArray = [];
      INDEX[dbMain].dbFragments[dbFragment].idRange = { min: null, max: null };
      purgeRoutingKeysForFragment(dbMain, dbFragment);
      rebuildIdRangeSorted(dbMain);
      markIndexRoutingDirty(dbMain);
      if (OPEN_DB[dbFragment]) {
        OPEN_DB[dbFragment].toWrite = { index: {}, data: {} };
        OPEN_DB[dbFragment].properties.isChanged = false;
      }
    }

    function destroyDB({ dbMain, dbFragment } = {}) {
      if (!dbMain && !dbFragment) {
        Object.keys(INDEX).forEach(function (dbMainKey) {
          destroyDBMain(dbMainKey);
        });
      } else if (!dbFragment) {
        destroyDBMain(dbMain);
      } else {
        destroyFragment(dbMain, dbFragment);
      }
      saveIndex();
    }

    function destroyDBMain(dbMain) {
      if (!INDEX[dbMain] || !INDEX[dbMain].properties) return;
      const { fragmentsList } = INDEX[dbMain].properties;
      const fragmentsList_ = fragmentsList.slice();
      fragmentsList_.forEach(function (dbFragment) {
        destroyFragment(dbMain, dbFragment);
      });
    }

    function destroyFragment(dbMain, dbFragment) {
      const { fileId } = INDEX[dbMain].dbFragments[dbFragment];
      const { fragmentsList } = INDEX[dbMain].properties;
      if (fileId) ToolkitAdapter.deleteFile(fileId);
      purgeRoutingKeysForFragment(dbMain, dbFragment);
      delete INDEX[dbMain].dbFragments[dbFragment];
      pull(dbFragment, fragmentsList);
      if (OPEN_DB[dbFragment]) delete OPEN_DB[dbFragment];
      rebuildIdRangeSorted(dbMain);
      markIndexRoutingDirty(dbMain);
    }

    function anyDbMainNeedsIndexPersist() {
      return Object.keys(INDEX).some(function (dm) {
        return INDEX[dm].properties && INDEX[dm].properties.indexRoutingDirty === true;
      });
    }

    function saveToDBFiles() {
      let wroteFragment = false;
      Object.keys(OPEN_DB).forEach(function (dbFragment) {
        const { properties, toWrite } = OPEN_DB[dbFragment];
        const { isChanged, main } = properties;
        if (!isChanged) return;
        wroteFragment = true;
        const { fileId } = INDEX[main].dbFragments[dbFragment];
        if (fileId === "") {
          createNewFile(main, dbFragment, toWrite);
        } else {
          ToolkitAdapter.writeToJSON(fileId, toWrite);
        }
        properties.isChanged = false;
      });
      if (wroteFragment && anyDbMainNeedsIndexPersist()) {
        saveIndex();
      }
    }

    function createNewFile(dbMain, dbFragment, toWrite) {
      const { dbFragments, properties } = INDEX[dbMain];
      const { rootFolder, filesPrefix } = properties;
      const newFileId = createDBFile(toWrite, rootFolder, filesPrefix, dbFragment);
      dbFragments[dbFragment].fileId = newFileId;
      markIndexRoutingDirty(dbMain);
    }

    function addToDB(entry, { dbMain, dbFragment }) {
      const id = normalizeId(entry.id);
      const key = normalizeKey(entry.key);
      const entryNorm = Object.assign({}, entry, { id: id, key: key });

      const { properties } = INDEX[dbMain];
      const { cumulative } = properties;
      let targetFragment;
      if (dbFragment) {
        targetFragment = getProperFragment(dbMain, dbFragment);
      } else {
        const existingFrag = findFragmentForId(id, dbMain);
        if (existingFrag) {
          targetFragment = existingFrag;
          if (!OPEN_DB[targetFragment]) openDBFragment(dbMain, targetFragment);
        } else {
          targetFragment = getProperFragment(dbMain, null);
        }
      }
      if (cumulative) targetFragment = checkOpenDBSize(dbMain, targetFragment, id);
      if (!targetFragment) return;

      ensureDbMainRouting(dbMain);
      const { ignoreIndex } = INDEX[dbMain].dbFragments[targetFragment];
      const tw = OPEN_DB[targetFragment].toWrite;
      const prior = tw.data[id];

      if (prior != null && normalizeKey(prior.key) !== key) {
        const oldK = normalizeKey(prior.key);
        removeKeysFromRouting(dbMain, oldK);
        if (!ignoreIndex && tw.index[oldK] !== undefined) delete tw.index[oldK];
      }

      var indexRoutingDirty = true;
      if (prior != null && normalizeKey(prior.key) === key) {
        var prevMap = INDEX[dbMain].properties.keyToFragment[key];
        if (prevMap === targetFragment) indexRoutingDirty = false;
      }

      if (!ignoreIndex) {
        tw.index[key] = id;
      }
      INDEX[dbMain].properties.keyToFragment[key] = targetFragment;

      const range = INDEX[dbMain].dbFragments[targetFragment].idRange;
      var rangeChanged = false;
      if (!range || range.min === null) {
        INDEX[dbMain].dbFragments[targetFragment].idRange = { min: id, max: id };
        rangeChanged = true;
      } else {
        var rmin = Number(range.min);
        var rmax = Number(range.max);
        if (id < rmin) { range.min = id; rangeChanged = true; }
        if (id > rmax) { range.max = id; rangeChanged = true; }
      }
      tw.data[id] = entryNorm;
      OPEN_DB[targetFragment].properties.isChanged = true;
      if (rangeChanged) rebuildIdRangeSorted(dbMain);
      if (indexRoutingDirty) markIndexRoutingDirty(dbMain);
    }

    function lookupByCriteria(criteria = [], { dbMain, dbFragment }) {
      if (dbMain && !dbFragment) return lookUpDBMainByCriteria(criteria, dbMain);
      return lookUpFragmentByCriteria(criteria, dbMain, dbFragment);
    }

    function lookUpDBMainByCriteria(criteria, dbMain) {
      const { dbFragments } = INDEX[dbMain];
      let entries = [];
      const idObj = getCriterionObjByParam(criteria, "id");
      if (idObj) {
        var nid = normalizeId(idObj.criterion);
        var dbFrag = findFragmentForId(nid, dbMain);
        if (!dbFrag) return [];
        var fragmentExistCheck = openDBFragment(dbMain, dbFrag);
        if (!fragmentExistCheck) return [];
        var ent = lookUpInFragmentById(nid, dbMain, dbFrag);
        return ent ? [ent] : [];
      }
      Object.keys(dbFragments).forEach(function (dbFragment) {
        const fragmentExistCheck = openDBFragment(dbMain, dbFragment);
        if (!fragmentExistCheck) return;
        entries = entries.concat(lookUpFragmentByCriteria(criteria, dbMain, dbFragment));
      });
      return entries;
    }

    function lookUpFragmentByCriteria(criteria, dbMain, dbFragment) {
      const fragmentExistCheck = openDBFragment(dbMain, dbFragment);
      if (!fragmentExistCheck) return [];
      const { toWrite } = OPEN_DB[dbFragment];
      const { data } = toWrite;
      let entries = Object.values(data);
      if (!criteria || criteria.length === 0) return entries;
      criteria.forEach(function (criterionObj) {
        criterionObj = criterionObj || {};
        const { param, path, criterion } = criterionObj;
        entries = lookUpEntriesByCriteria(entries, { param: param, path: path, criterion: criterion });
      });
      return entries;
    }

    function lookUpEntriesByCriteria(entries, { param, path, criterion }) {
      return entries.filter(function (entry) {
        const value = getValueFromPath(path, param, entry);
        if (value === undefined) return false;
        if (typeof criterion === "function") return criterion(value);
        return value === criterion;
      });
    }

    function getCriterionObjByParam(criteria, param) {
      return criteria.find(function (criterionObj) {
        return criterionObj.param === param;
      });
    }

    function getValueFromPath(path = [], param, entry) {
      let value;
      if (!path || path.length === 0) return entry[param];
      path.forEach(function (pathParam, i) {
        if (i === 0) {
          value = entry[pathParam];
        } else {
          if (!value) return;
          if (Array.isArray(value)) value = value[0];
          value = value[pathParam];
        }
      });
      if (value) {
        if (Array.isArray(value)) value = value[0];
        value = value[param];
      }
      return value;
    }

    function lookUpByKey(key, { dbMain, dbFragment }) {
      if (dbMain && !dbFragment) return lookUpByKeyQueryArray(key, dbMain);
      return lookUpInFragmentByKey(key, dbMain, dbFragment);
    }

    function lookUpById(id, { dbMain, dbFragment }) {
      if (dbMain && !dbFragment) return lookUpByIdQueryArray(id, dbMain);
      return lookUpInFragmentById(id, dbMain, dbFragment);
    }

    function deleteFromDBByKey(key, { dbMain, dbFragment }) {
      key = normalizeKey(key);
      if (dbMain && !dbFragment) {
        dbFragment = findFragmentForKey(key, dbMain);
        if (!dbFragment) return;
      }
      const fragmentExistCheck = openDBFragment(dbMain, dbFragment);
      if (!fragmentExistCheck) return;
      const id = resolveIdForKeyInFragment(key, dbMain, dbFragment);
      if (id == null) return;
      deleteIdEntriesInFragment(id, dbFragment);
    }

    function deleteFromDBById(id, { dbMain, dbFragment }) {
      id = normalizeId(id);
      if (dbMain && !dbFragment) {
        dbFragment = findFragmentForId(id, dbMain);
        if (!dbFragment) return;
      }
      const fragmentExistCheck = openDBFragment(dbMain, dbFragment);
      if (!fragmentExistCheck) return;
      deleteIdEntriesInFragment(id, dbFragment);
    }

    function deleteIdEntriesInFragment(id, dbFragment) {
      const open = OPEN_DB[dbFragment];
      const dbMain = open.properties.main;
      id = normalizeId(id);
      const prior = open.toWrite.data[id];
      if (prior && prior.key != null) {
        const priorKey = normalizeKey(prior.key);
        if (open.toWrite.index[priorKey] !== undefined) delete open.toWrite.index[priorKey];
        removeKeysFromRouting(dbMain, priorKey);
      }
      delete open.toWrite.data[id];
      open.properties.isChanged = true;
      markIndexRoutingDirty(dbMain);
      recalcFragmentIdRangeAfterMutation(dbMain, dbFragment);
    }

    function lookUpByKeyQueryArray(key, dbMain) {
      key = normalizeKey(key);
      const dbFragment = findFragmentForKey(key, dbMain);
      if (!dbFragment) return null;
      const fragmentExistCheck = openDBFragment(dbMain, dbFragment);
      if (!fragmentExistCheck) return null;
      return lookUpInFragmentByKey(key, dbMain, dbFragment);
    }

    function lookUpByIdQueryArray(id, dbMain) {
      id = normalizeId(id);
      const dbFragment = findFragmentForId(id, dbMain);
      if (!dbFragment) return null;
      const fragmentExistCheck = openDBFragment(dbMain, dbFragment);
      if (!fragmentExistCheck) return null;
      return lookUpInFragmentById(id, dbMain, dbFragment);
    }

    function lookUpInFragmentByKey(key, dbMain, dbFragment) {
      key = normalizeKey(key);
      if (!INDEX[dbMain].dbFragments[dbFragment]) return null;
      const fragmentExistCheck = openDBFragment(dbMain, dbFragment);
      if (!fragmentExistCheck) return null;
      const { toWrite } = OPEN_DB[dbFragment];
      const id = resolveIdForKeyInFragment(key, dbMain, dbFragment);
      if (id == null) return null;
      return toWrite.data[id];
    }

    function lookUpInFragmentById(id, dbMain, dbFragment) {
      id = normalizeId(id);
      if (!INDEX[dbMain].dbFragments[dbFragment]) return null;
      const fragmentExistCheck = openDBFragment(dbMain, dbFragment);
      if (!fragmentExistCheck) return null;
      const { toWrite } = OPEN_DB[dbFragment];
      return toWrite.data[id];
    }

    function lookUpForKeysInFragment(id, dbMain, dbFragment) {
      if (!INDEX[dbMain].dbFragments[dbFragment]) return null;
      const { toWrite } = OPEN_DB[dbFragment];
      const { index } = toWrite;
      id = normalizeId(id);
      return Object.keys(index)
        .filter(function (_key) {
          return normalizeId(index[_key]) === id;
        })
        .map(function (_key) {
          return _key;
        });
    }

    function lookUpForIdInFragment(key, dbMain, dbFragment) {
      key = normalizeKey(key);
      if (!INDEX[dbMain].dbFragments[dbFragment]) return null;
      const { toWrite } = OPEN_DB[dbFragment];
      return toWrite.index[key];
    }

    /** Resolves id for key when ignoreIndex left the fragment index empty but data + keyToFragment exist. */
    function resolveIdForKeyInFragment(key, dbMain, dbFragment) {
      key = normalizeKey(key);
      var fromIdx = lookUpForIdInFragment(key, dbMain, dbFragment);
      if (fromIdx != null) return normalizeId(fromIdx);
      const { toWrite } = OPEN_DB[dbFragment];
      const d = toWrite.data;
      var keys = Object.keys(d);
      for (var i = 0; i < keys.length; i++) {
        var e = d[keys[i]];
        if (e && normalizeKey(e.key) === key) return normalizeId(e.id);
      }
      return null;
    }

    function createDBFile(toWrite, rootFolder, filesPrefix, dbFragment) {
      const fileName = filesPrefix + "_" + dbFragment;
      return ToolkitAdapter.createJSON(fileName, rootFolder, toWrite);
    }

    function getProperFragment(dbMain, dbFragment) {
      if (!INDEX[dbMain]) {
        throw new Error("No configs found for this DB Main.");
      }
      ensureDbMainRouting(dbMain);
      const targetFragment = checkInIndex(dbMain, dbFragment);
      if (!OPEN_DB[targetFragment]) {
        openDBFragment(dbMain, targetFragment);
      }
      return targetFragment;
    }

    function checkInIndex(dbMain, dbFragment) {
      if (!dbFragment) {
        return getLatestdbMainFragment(dbMain);
      }
      const { dbFragments } = INDEX[dbMain];
      if (!dbFragments[dbFragment]) addInIndexFile(dbMain, dbFragment);
      return dbFragment;
    }

    function getLatestdbMainFragment(dbMain) {
      let dbFragment = getLastCreatedFragment(dbMain);
      if (!dbFragment) {
        dbFragment = createNewCumulativeFragment(dbMain, dbFragment);
      }
      return dbFragment;
    }

    function openDBFragment(dbMain, dbFragment) {
      if (OPEN_DB[dbFragment]) return true;
      if (!INDEX[dbMain].dbFragments[dbFragment]) return false;
      let fragmentFileObj;
      const { fileId } = INDEX[dbMain].dbFragments[dbFragment];
      if (fileId) fragmentFileObj = ToolkitAdapter.readFromJSON(fileId);
      addToOpenDBsObj(dbMain, dbFragment, fragmentFileObj);
      return true;
    }

    function addToOpenDBsObj(dbMain, dbFragment, fragmentFileObj) {
      OPEN_DB[dbFragment] = new OpenDBEntry(dbMain, fragmentFileObj);
    }

    /**
     * Roll to a new cumulative fragment only when inserting a new id would exceed capacity.
     * In-place updates must not roll just because the shard already has MAX_ENTRIES_COUNT rows.
     */
    function checkOpenDBSize(dbMain, dbFragment, entryId) {
      const open = OPEN_DB[dbFragment];
      if (!open) return dbFragment;
      const { data } = open.toWrite;
      if (entryId != null) {
        const idKey = normalizeId(entryId);
        if (Object.prototype.hasOwnProperty.call(data, idKey) && data[idKey] != null) {
          return dbFragment;
        }
      }
      if (Object.keys(data).length >= MAX_ENTRIES_COUNT) {
        dbFragment = createNewCumulativeFragment(dbMain, dbFragment);
        openDBFragment(dbMain, dbFragment);
        return dbFragment;
      }
      return dbFragment;
    }

    function createNewCumulativeFragment(dbMain, dbFragment) {
      const lastDBFragment = dbFragment || getLastCreatedFragment(dbMain);
      const countingRegex = /_(\d+)$/g;
      let newFragment;
      if (!lastDBFragment) {
        newFragment = dbMain + "_1";
      } else if (countingRegex.test(lastDBFragment)) {
        countingRegex.lastIndex = 0;
        const match = countingRegex.exec(lastDBFragment);
        let count = parseInt(match[1], 10);
        count++;
        newFragment = lastDBFragment.replace(/_\d+$/, "") + "_" + count;
      } else {
        newFragment = lastDBFragment + "_2";
      }
      addInIndexFile(dbMain, newFragment);
      return newFragment;
    }

    function addInIndexFile(dbMain, dbFragment) {
      INDEX[dbMain].dbFragments[dbFragment] = new IndexEntry();
      INDEX[dbMain].properties.fragmentsList.push(dbFragment);
      ensureDbMainRouting(dbMain);
      markIndexRoutingDirty(dbMain);
    }

    function getLastCreatedFragment(dbMain) {
      const { properties } = INDEX[dbMain];
      const { fragmentsList } = properties;
      if (!fragmentsList || fragmentsList.length === 0) return null;
      return fragmentsList[fragmentsList.length - 1];
    }

    function getExternalConfig(key, { dbMain, dbFragment }) {
      return INDEX[dbMain].dbFragments[dbFragment].externalConfigs[key];
    }

    function addExternalConfig(key, value, { dbMain, dbFragment }) {
      INDEX[dbMain].dbFragments[dbFragment].externalConfigs[key] = value;
      markIndexRoutingDirty(dbMain);
    }

    function pull(element, array) {
      const index = array.indexOf(element);
      if (index !== -1) array.splice(index, 1);
    }

    /**
     * Serialized master INDEX size (Drive JSON) and routing metadata counts.
     * Growth is dominated by keyToFragment (one string key per live row) when using cumulative routing.
     * @param {{ dbMain?: string }} opts — omit dbMain for full INDEX byte size; pass dbMain for that subtree only.
     */
    function getIndexFootprint(opts) {
      opts = opts || {};
      const dbMain = opts.dbMain;
      let legacyKeyQueryArrayEntries = 0;
      function countLegacy(node) {
        if (!node || !node.dbFragments) return;
        Object.keys(node.dbFragments).forEach(function (f) {
          const kqa = node.dbFragments[f].keyQueryArray;
          if (kqa && kqa.length) legacyKeyQueryArrayEntries += kqa.length;
        });
      }
      if (dbMain && INDEX[dbMain]) {
        countLegacy(INDEX[dbMain]);
        const slice = {};
        slice[dbMain] = INDEX[dbMain];
        const props = INDEX[dbMain].properties || {};
        const kf = props.keyToFragment || {};
        return {
          indexJsonBytes: JSON.stringify(slice).length,
          keyToFragmentCount: Object.keys(kf).length,
          fragmentsCount: (props.fragmentsList || []).length,
          idRangesSortedCount: (props.idRangesSorted || []).length,
          legacyKeyQueryArrayEntries: legacyKeyQueryArrayEntries,
          indexRoutingDirty: props.indexRoutingDirty === true
        };
      }
      Object.keys(INDEX).forEach(function (dm) {
        countLegacy(INDEX[dm]);
      });
      return {
        indexJsonBytes: JSON.stringify(INDEX).length,
        keyToFragmentCount: Object.keys(INDEX).reduce(function (acc, dm) {
          const kf = INDEX[dm].properties && INDEX[dm].properties.keyToFragment;
          return acc + (kf ? Object.keys(kf).length : 0);
        }, 0),
        fragmentsCount: Object.keys(INDEX).reduce(function (acc, dm) {
          const fl = INDEX[dm].properties && INDEX[dm].properties.fragmentsList;
          return acc + (fl ? fl.length : 0);
        }, 0),
        idRangesSortedCount: Object.keys(INDEX).reduce(function (acc, dm) {
          const arr = INDEX[dm].properties && INDEX[dm].properties.idRangesSorted;
          return acc + (arr ? arr.length : 0);
        }, 0),
        legacyKeyQueryArrayEntries: legacyKeyQueryArrayEntries,
        indexRoutingDirty: Object.keys(INDEX).some(function (dm) {
          return INDEX[dm].properties && INDEX[dm].properties.indexRoutingDirty === true;
        })
      };
    }

    /**
     * Cross-check master INDEX routing vs opened fragment index/data (detect drift / bad edits).
     * Call after saveToDBFiles or when debugging; opens every fragment for dbMain.
     * @returns {{ ok: boolean, errors: string[] }}
     */
    function validateRoutingConsistency({ dbMain }) {
      const errors = [];
      if (!dbMain || !INDEX[dbMain]) {
        errors.push("validateRoutingConsistency: missing dbMain or unknown dbMain: " + dbMain);
        return { ok: false, errors: errors };
      }
      ensureDbMainRouting(dbMain);
      const props = INDEX[dbMain].properties;
      const kf = props.keyToFragment || {};
      const fragmentsList = props.fragmentsList || [];
      const idRangesSorted = props.idRangesSorted || [];

      fragmentsList.forEach(function (dbFragment) {
        const fragMeta = INDEX[dbMain].dbFragments[dbFragment];
        if (!fragMeta) {
          errors.push("fragmentsList references missing dbFragments entry: " + dbFragment);
          return;
        }
        if (!openDBFragment(dbMain, dbFragment)) {
          errors.push("could not open fragment: " + dbFragment);
          return;
        }
        const open = OPEN_DB[dbFragment];
        const tw = open.toWrite;
        const data = tw.data || {};
        const index = tw.index || {};
        const ignore = fragMeta.ignoreIndex;

        const ids = Object.keys(data).map(function (k) {
          return normalizeId(k);
        });
        if (ids.length > 0) {
          var dmin = ids.reduce(function (a, b) { return a < b ? a : b; });
          var dmax = ids.reduce(function (a, b) { return a > b ? a : b; });
          var ir = fragMeta.idRange;
          if (ir) {
            if (Number(ir.min) !== dmin) {
              errors.push("idRange.min mismatch " + dbFragment + " index=" + ir.min + " dataMin=" + dmin);
            }
            if (Number(ir.max) !== dmax) {
              errors.push("idRange.max mismatch " + dbFragment + " index=" + ir.max + " dataMax=" + dmax);
            }
          }
          ids.forEach(function (nid) {
            var ff = findFragmentForId(nid, dbMain);
            if (ff !== dbFragment) {
              errors.push("findFragmentForId(" + nid + ")=" + ff + " expected " + dbFragment);
            }
          });
        } else if (fragMeta.idRange && fragMeta.idRange.min != null) {
          errors.push("empty data but idRange set for " + dbFragment);
        }

        Object.keys(index).forEach(function (key) {
          var k = normalizeKey(key);
          var mapped = kf[k];
          if (mapped !== dbFragment) {
            errors.push(
              "fragment index key maps wrong: key=" + k + " keyToFragment=" + mapped + " fragment=" + dbFragment
            );
          }
          var idFromIndex = normalizeId(index[key]);
          var row = data[idFromIndex];
          if (!row) {
            errors.push("index id " + idFromIndex + " has no data row in " + dbFragment);
          } else if (normalizeKey(row.key) !== k) {
            errors.push("index vs data.key mismatch in " + dbFragment + " key=" + k);
          }
        });

        if (!ignore) {
          Object.keys(kf).forEach(function (key) {
            if (kf[key] === dbFragment && index[key] === undefined) {
              errors.push("keyToFragment orphan (missing from fragment index): key=" + key + " " + dbFragment);
            }
          });
        } else {
          Object.keys(data).forEach(function (idKey) {
            var row = data[idKey];
            if (row && row.key != null) {
              var kk = normalizeKey(row.key);
              if (kf[kk] !== dbFragment) {
                errors.push("ignoreIndex: data key not routed to this fragment key=" + kk + " frag=" + dbFragment);
              }
            }
          });
        }
      });

      Object.keys(kf).forEach(function (key) {
        var frag = kf[key];
        if (!INDEX[dbMain].dbFragments[frag]) {
          errors.push("keyToFragment points to missing fragment: key=" + key + " -> " + frag);
        }
      });

      idRangesSorted.forEach(function (r) {
        var fr = r.fragment;
        var meta = INDEX[dbMain].dbFragments[fr];
        if (!meta) {
          errors.push("idRangesSorted references unknown fragment: " + fr);
          return;
        }
        var ir = meta.idRange;
        if (!ir || ir.min == null) return;
        if (Number(r.min) !== Number(ir.min) || Number(r.max) !== Number(ir.max)) {
          errors.push("idRangesSorted row out of sync for " + fr + " sorted=(" + r.min + "," + r.max + ") meta=(" + ir.min + "," + ir.max + ")");
        }
      });

      return { ok: errors.length === 0, errors: errors };
    }

    return {
      INDEX: INDEX,
      OPEN_DB: OPEN_DB,
      addToDB: addToDB,
      lookUpByKey: lookUpByKey,
      lookUpById: lookUpById,
      lookupByCriteria: lookupByCriteria,
      deleteFromDBByKey: deleteFromDBByKey,
      deleteFromDBById: deleteFromDBById,
      saveToDBFiles: saveToDBFiles,
      saveIndex: saveIndex,
      closeDB: closeDB,
      clearDB: clearDB,
      destroyDB: destroyDB,
      getExternalConfig: getExternalConfig,
      addExternalConfig: addExternalConfig,
      getIndexFootprint: getIndexFootprint,
      validateRoutingConsistency: validateRoutingConsistency,
      /** @internal testing / advanced introspection */
      _routing: {
        findFragmentForId: findFragmentForId,
        findFragmentForKey: findFragmentForKey,
        rebuildIdRangeSorted: rebuildIdRangeSorted
      }
    };
  }

  return {
    init: init,
    MAX_ENTRIES_COUNT: MAX_ENTRIES_COUNT,
    normalizeId: normalizeId
  };
});
