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

  /**
   * Build a composite OPEN_DB key from dbMain and fragment name.
   * Prevents silent collision when two dbMains share the same fragment suffix (e.g. both named "_1").
   */
  function openDbKey(dbMain, dbFragment) {
    return dbMain + "\x00" + dbFragment;
  }

  /**
   * Partition routing helpers.
   *
   * When a dbMain is configured with a `partitionBy` function the fragment name
   * is derived deterministically from the partition key:
   *
   *   base fragment  →  `${dbMain}_p_${sanitizedPartitionKey}`
   *   overflow #2    →  `${dbMain}_p_${sanitizedPartitionKey}_2`
   *   overflow #3    →  `${dbMain}_p_${sanitizedPartitionKey}_3`  … etc.
   *
   * This lets the caller derive the exact fragment name from the partition key
   * alone — no INDEX lookup needed for the common (single-shard) case.
   */
  function sanitizePartitionKey(pk) {
    // Allow letters, digits, dash, dot, @. Replace everything else with "_".
    return String(pk).replace(/[^A-Za-z0-9\-\.@]/g, "_");
  }

  function partitionBaseFragment(dbMain, partitionKey) {
    return dbMain + "_p_" + sanitizePartitionKey(partitionKey);
  }

  /**
   * Given a base partition fragment name, return the next overflow name.
   * Base → Base_2 → Base_3 …  (same counter logic as createNewCumulativeFragment)
   */
  function nextPartitionFragment(baseFragment, lastFragment) {
    if (!lastFragment || lastFragment === baseFragment) return baseFragment + "_2";
    const countingRegex = /_(\d+)$/;
    const match = lastFragment.match(countingRegex);
    if (!match) return lastFragment + "_2";
    return lastFragment.replace(/_\d+$/, "_" + (parseInt(match[1], 10) + 1));
  }

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

  /**
   * Doubly-linked list node used by the LRU tracker.
   * `key` is the composite OPEN_DB key (dbMain + "\x00" + dbFragment).
   */
  function LRUNode(key) {
    this.key  = key;
    this.prev = null;
    this.next = null;
  }

  /**
   * O(1) LRU tracker backed by a plain-object hash map + doubly-linked list.
   * Compatible with GAS ES5 (no Map/Set required).
   *
   * Head  = most-recently used.
   * Tail  = least-recently used (eviction candidate).
   *
   * @param {number} capacity  Maximum number of entries (Infinity = no limit).
   */
  function LRUTracker(capacity) {
    this.capacity = (capacity > 0 && Number.isFinite(capacity)) ? capacity : Infinity;
    this.size     = 0;
    this.nodes    = {};   // key → LRUNode
    // Sentinel nodes avoid null-checks everywhere.
    this._head    = new LRUNode("__head__");
    this._tail    = new LRUNode("__tail__");
    this._head.next = this._tail;
    this._tail.prev = this._head;
  }

  LRUTracker.prototype._detach = function (node) {
    node.prev.next = node.next;
    node.next.prev = node.prev;
    node.prev = null;
    node.next = null;
  };

  LRUTracker.prototype._insertAfterHead = function (node) {
    node.next = this._head.next;
    node.prev = this._head;
    this._head.next.prev = node;
    this._head.next = node;
  };

  /** Add a new key (must not already exist). */
  LRUTracker.prototype.add = function (key) {
    var node = new LRUNode(key);
    this.nodes[key] = node;
    this._insertAfterHead(node);
    this.size++;
  };

  /** Promote an existing key to MRU position. No-op if key not tracked. */
  LRUTracker.prototype.touch = function (key) {
    var node = this.nodes[key];
    if (!node) return;
    this._detach(node);
    this._insertAfterHead(node);
  };

  /** Remove a key from tracking. No-op if not tracked. */
  LRUTracker.prototype.remove = function (key) {
    var node = this.nodes[key];
    if (!node) return;
    this._detach(node);
    delete this.nodes[key];
    this.size--;
  };

  /** Return the LRU (tail) key, or null if empty. */
  LRUTracker.prototype.lruKey = function () {
    var tail = this._tail.prev;
    if (tail === this._head) return null;
    return tail.key;
  };

  /** Return the MRU (head) key, or null if empty. */
  LRUTracker.prototype.mruKey = function () {
    var head = this._head.next;
    if (head === this._tail) return null;
    return head.key;
  };

  function init(indexFileId, ToolkitAdapter, options) {
    if (!indexFileId) return null;
    if (!ToolkitAdapter || typeof ToolkitAdapter.readFromJSON !== "function") {
      throw new Error("JSONDatabase requires ToolkitAdapter to execute Drive I/O.");
    }
    options = options || {};
    var instanceMaxEntries =
      options.maxEntriesCount != null && Number.isFinite(Number(options.maxEntriesCount))
        ? Number(options.maxEntriesCount)
        : MAX_ENTRIES_COUNT;

    /**
     * Per-dbMain partition functions supplied at init time.
     * Key: dbMain string.  Value: function(entry) → partitionKey string.
     * These are NOT persisted to the INDEX (functions can't be serialized).
     * The caller must pass the same `partitionBy` map on every init() call.
     */
    var PARTITION_FNS = options.partitionBy || {};

    /**
     * LRU eviction for OPEN_DB.
     *
     * `maxOpenFragments` caps how many fragment files can be held in memory at once.
     * When the limit is reached and a new fragment is opened, the least-recently-used
     * fragment is auto-saved (if dirty) and evicted from OPEN_DB before the new one
     * is added.  Default is Infinity (no eviction) so existing callers are unaffected.
     */
    var instanceMaxOpenFragments =
      options.maxOpenFragments != null && Number.isFinite(Number(options.maxOpenFragments)) &&
      Number(options.maxOpenFragments) > 0
        ? Number(options.maxOpenFragments)
        : Infinity;
    var LRU = new LRUTracker(instanceMaxOpenFragments);

    /**
     * Evict the LRU fragment from OPEN_DB when the capacity is exceeded.
     * If the fragment is dirty, it is written to Drive before removal so no
     * data is silently lost.  Called from addToOpenDBsObj before inserting a
     * new entry.
     */
    function evictLRUIfNeeded() {
      if (!Number.isFinite(LRU.capacity)) return;          // no limit — fast path
      while (LRU.size >= LRU.capacity) {
        var lruKey = LRU.lruKey();
        if (!lruKey) break;
        var evicted = OPEN_DB[lruKey];
        if (evicted && evicted.properties.isChanged) {
          // Auto-save dirty evicted fragment to Drive.
          var main     = evicted.properties.main;
          var fragment = evicted.properties.fragment;
          var fileId   = INDEX[main] &&
                         INDEX[main].dbFragments[fragment] &&
                         INDEX[main].dbFragments[fragment].fileId;
          if (fileId === "") {
            createNewFile(main, fragment, evicted.toWrite);
          } else if (fileId) {
            ToolkitAdapter.writeToJSON(fileId, evicted.toWrite);
          }
          evicted.properties.isChanged = false;
        }
        LRU.remove(lruKey);
        delete OPEN_DB[lruKey];
      }
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

    /**
     * Returns true when dbMain uses partition routing (caller supplied a partitionBy fn).
     */
    function isPartitioned(dbMain) {
      return typeof PARTITION_FNS[dbMain] === "function";
    }

    /**
     * Derive the base partition fragment name for `entry` under `dbMain`.
     * Throws if `dbMain` has no partitionBy function registered.
     */
    function partitionFragmentForEntry(dbMain, entry) {
      var pk = PARTITION_FNS[dbMain](entry);
      return partitionBaseFragment(dbMain, pk);
    }

    /**
     * Derive the base partition fragment name directly from a partition key string.
     */
    function partitionFragmentForKey(dbMain, partitionKey) {
      return partitionBaseFragment(dbMain, partitionKey);
    }

    /**
     * For a given partition base fragment, find the overflow shard that still has
     * room for a new entry (not in data), or create a new overflow shard.
     * Returns the target fragment name (opens it in OPEN_DB if needed).
     */
    function resolvePartitionFragment(dbMain, baseFragment, entryId) {
      const { dbFragments, properties } = INDEX[dbMain];
      const { fragmentsList } = properties;

      // Collect all fragments that belong to this partition (base + overflows).
      var siblings = fragmentsList.filter(function (f) {
        return f === baseFragment || f.indexOf(baseFragment + "_") === 0;
      });

      if (siblings.length === 0) {
        // First entry for this partition — create base fragment.
        addInIndexFile(dbMain, baseFragment);
        openDBFragment(dbMain, baseFragment);
        return baseFragment;
      }

      // Walk siblings in order: base first, then _2, _3 …
      siblings.sort(function (a, b) {
        if (a === baseFragment) return -1;
        if (b === baseFragment) return 1;
        var na = parseInt((a.match(/_(\d+)$/) || [0, 0])[1], 10);
        var nb = parseInt((b.match(/_(\d+)$/) || [0, 0])[1], 10);
        return na - nb;
      });

      // If entryId already lives in one of the siblings, route there (in-place update).
      // NOTE: We scan siblings directly rather than using findFragmentForId because
      // partition ranges overlap across different partitions — the binary search is
      // only valid within a single partition's sibling set, not across the whole table.
      if (entryId != null) {
        var nid = normalizeId(entryId);
        for (var si = 0; si < siblings.length; si++) {
          var sib = siblings[si];
          openDBFragment(dbMain, sib);
          var sibOpen = OPEN_DB[openDbKey(dbMain, sib)];
          if (sibOpen && sibOpen.toWrite.data[nid] != null) {
            return sib;
          }
        }
      }

      // Find last sibling with room.
      var lastSibling = siblings[siblings.length - 1];
      openDBFragment(dbMain, lastSibling);
      var open = OPEN_DB[openDbKey(dbMain, lastSibling)];
      if (open && Object.keys(open.toWrite.data).length < instanceMaxEntries) {
        return lastSibling;
      }

      // All siblings full — create next overflow.
      var newFrag = nextPartitionFragment(baseFragment, lastSibling);
      addInIndexFile(dbMain, newFrag);
      openDBFragment(dbMain, newFrag);
      return newFrag;
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
      const open = OPEN_DB[openDbKey(dbMain, dbFragment)];
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
      else closeFragment(dbMain, dbFragment);
    }

    function closeDBMain(dbMain) {
      const { fragmentsList } = INDEX[dbMain].properties;
      fragmentsList.forEach(function (frag) { closeFragment(dbMain, frag); });
    }

    function closeFragment(dbMain, dbFragment) {
      var k = openDbKey(dbMain, dbFragment);
      if (OPEN_DB[k]) {
        delete OPEN_DB[k];
        LRU.remove(k);
      }
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
      var k = openDbKey(dbMain, dbFragment);
      if (OPEN_DB[k]) {
        OPEN_DB[k].toWrite = { index: {}, data: {} };
        OPEN_DB[k].properties.isChanged = false;
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
      var k = openDbKey(dbMain, dbFragment);
      if (OPEN_DB[k]) {
        delete OPEN_DB[k];
        LRU.remove(k);
      }
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
      Object.keys(OPEN_DB).forEach(function (compositeKey) {
        const entry = OPEN_DB[compositeKey];
        const { properties, toWrite } = entry;
        const { isChanged, main, fragment } = properties;
        if (!isChanged) return;
        wroteFragment = true;
        const { fileId } = INDEX[main].dbFragments[fragment];
        if (fileId === "") {
          createNewFile(main, fragment, toWrite);
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
      var newFileId;
      try {
        newFileId = createDBFile(toWrite, rootFolder, filesPrefix, dbFragment);
      } catch (e) {
        // createJSON failed: do not store a phantom fileId — leave it as "" so
        // the next saveToDBFiles call will retry. Re-mark as changed so the retry
        // path triggers again.
        var k = openDbKey(dbMain, dbFragment);
        if (OPEN_DB[k]) OPEN_DB[k].properties.isChanged = true;
        throw e;
      }
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

      if (isPartitioned(dbMain)) {
        // Partition routing: derive fragment from partitionBy(entry).
        var basePartFrag = partitionFragmentForEntry(dbMain, entry);
        targetFragment = resolvePartitionFragment(dbMain, basePartFrag, id);
      } else if (dbFragment) {
        targetFragment = getProperFragment(dbMain, dbFragment);
      } else {
        const existingFrag = findFragmentForId(id, dbMain);
        if (existingFrag) {
          targetFragment = existingFrag;
          if (!OPEN_DB[openDbKey(dbMain, targetFragment)]) openDBFragment(dbMain, targetFragment);
        } else {
          targetFragment = getProperFragment(dbMain, null);
        }
      }
      if (!isPartitioned(dbMain) && cumulative) targetFragment = checkOpenDBSize(dbMain, targetFragment, id);
      if (!targetFragment) return;

      ensureDbMainRouting(dbMain);
      const { ignoreIndex } = INDEX[dbMain].dbFragments[targetFragment];
      const tw = OPEN_DB[openDbKey(dbMain, targetFragment)].toWrite;
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
      OPEN_DB[openDbKey(dbMain, targetFragment)].properties.isChanged = true;
      if (rangeChanged) rebuildIdRangeSorted(dbMain);
      if (indexRoutingDirty) markIndexRoutingDirty(dbMain);
    }

    function lookupByCriteria(criteria = [], { dbMain, dbFragment, partitionKey }) {
      if (dbMain && !dbFragment) {
        if (partitionKey != null && isPartitioned(dbMain)) {
          return lookUpPartitionByCriteria(criteria, dbMain, partitionKey);
        }
        return lookUpDBMainByCriteria(criteria, dbMain);
      }
      return lookUpFragmentByCriteria(criteria, dbMain, dbFragment);
    }

    /**
     * Scan only the fragments that belong to a single partition (base + overflows).
     * Avoids touching fragments for other partitions entirely.
     */
    function lookUpPartitionByCriteria(criteria, dbMain, partitionKey) {
      var baseFragment = partitionFragmentForKey(dbMain, partitionKey);
      var { fragmentsList } = INDEX[dbMain].properties;
      var siblings = fragmentsList.filter(function (f) {
        return f === baseFragment || f.indexOf(baseFragment + "_") === 0;
      });
      var entries = [];
      siblings.forEach(function (frag) {
        var ok = openDBFragment(dbMain, frag);
        if (!ok) return;
        entries = entries.concat(lookUpFragmentByCriteria(criteria, dbMain, frag));
      });
      return entries;
    }

    function lookUpDBMainByCriteria(criteria, dbMain) {
      const { dbFragments } = INDEX[dbMain];
      let entries = [];
      const idObj = getCriterionObjByParam(criteria, "id");
      if (idObj) {
        if (criteria.length > 1) {
          console.warn(
            "ShardDB.lookupByCriteria: 'id' criterion short-circuits to a single row; " +
            (criteria.length - 1) + " additional criterion/criteria are NOT applied. " +
            "Remove 'id' from the criteria array if you need compound filtering."
          );
        }
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
      const { toWrite } = OPEN_DB[openDbKey(dbMain, dbFragment)];
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
        // If the terminal node is an array (e.g. path led into an array and param
        // names a field that multiple elements might have), check whether ANY element
        // satisfies the criterion rather than requiring a single scalar match.
        if (Array.isArray(value)) {
          return value.some(function (el) {
            if (el === undefined) return false;
            if (typeof criterion === "function") return criterion(el);
            return el === criterion;
          });
        }
        if (typeof criterion === "function") return criterion(value);
        return value === criterion;
      });
    }

    function getCriterionObjByParam(criteria, param) {
      return criteria.find(function (criterionObj) {
        return criterionObj.param === param;
      });
    }

    /**
     * Traverse `path` from `entry`, then read `param`.
     *
     * When an intermediate or terminal node is an array, the function searches
     * *every* element for one that contains the next key (or `param`), rather
     * than blindly taking `[0]`. This fixes the silent correctness failure where
     * matching values stored at index > 0 were never found.
     *
     * Rules:
     *  - If a node is an object, descend directly.
     *  - If a node is an array, find the first element that has the next key.
     *  - If nothing is found at any level, return undefined.
     */
    function getValueFromPath(path, param, entry) {
      if (!path || path.length === 0) return entry[param];

      /**
       * Given a node that may be an object or array, return the child at `key`.
       * When `node` is an array, scan all elements and return the value from the
       * first element that owns `key`.
       */
      function descend(node, key) {
        if (node == null) return undefined;
        if (Array.isArray(node)) {
          for (var i = 0; i < node.length; i++) {
            var el = node[i];
            if (el != null && typeof el === "object" && !Array.isArray(el) &&
                Object.prototype.hasOwnProperty.call(el, key)) {
              return el[key];
            }
          }
          return undefined;
        }
        return node[key];
      }

      var node = entry;
      for (var i = 0; i < path.length; i++) {
        node = descend(node, path[i]);
        if (node === undefined) return undefined;
      }
      // If the node at the end of the path is an array, collect param values from
      // every element so the caller can check if ANY of them satisfies the criterion.
      if (Array.isArray(node)) {
        var collected = [];
        for (var j = 0; j < node.length; j++) {
          var el = node[j];
          if (el != null && typeof el === "object" && !Array.isArray(el) &&
              Object.prototype.hasOwnProperty.call(el, param)) {
            collected.push(el[param]);
          }
        }
        return collected.length > 0 ? collected : undefined;
      }
      return descend(node, param);
    }

    function lookUpByKey(key, { dbMain, dbFragment, partitionKey }) {
      if (dbMain && !dbFragment) {
        if (partitionKey != null && isPartitioned(dbMain)) {
          return lookUpByKeyInPartition(key, dbMain, partitionKey);
        }
        return lookUpByKeyQueryArray(key, dbMain);
      }
      return lookUpInFragmentByKey(key, dbMain, dbFragment);
    }

    function lookUpById(id, { dbMain, dbFragment, partitionKey }) {
      if (dbMain && !dbFragment) {
        if (partitionKey != null && isPartitioned(dbMain)) {
          return lookUpByIdInPartition(id, dbMain, partitionKey);
        }
        return lookUpByIdQueryArray(id, dbMain);
      }
      return lookUpInFragmentById(id, dbMain, dbFragment);
    }

    /**
     * Look up by key, scanning only the fragments for the given partition.
     * When the partition has a single shard (common case) this is a direct hit
     * with zero INDEX traversal.
     */
    function lookUpByKeyInPartition(key, dbMain, partitionKey) {
      key = normalizeKey(key);
      // Try the master routing map first (populated on every addToDB).
      var fromRouting = findFragmentForKey(key, dbMain);
      if (fromRouting) {
        var baseFragment = partitionFragmentForKey(dbMain, partitionKey);
        // Only use fromRouting if it actually belongs to this partition.
        if (fromRouting === baseFragment || fromRouting.indexOf(baseFragment + "_") === 0) {
          var ok = openDBFragment(dbMain, fromRouting);
          if (ok) return lookUpInFragmentByKey(key, dbMain, fromRouting);
        }
      }
      // Fallback: scan partition siblings.
      var results = lookUpPartitionByCriteria([{ param: "key", criterion: key }], dbMain, partitionKey);
      return results.length > 0 ? results[0] : null;
    }

    /**
     * Look up by id, scanning only the fragments for the given partition.
     * Does NOT use findFragmentForId (global binary search) because partition
     * id-ranges overlap across different partitions — ranges are only disjoint
     * within a single partition's sibling set.
     */
    function lookUpByIdInPartition(id, dbMain, partitionKey) {
      id = normalizeId(id);
      var baseFragment = partitionFragmentForKey(dbMain, partitionKey);
      var { fragmentsList } = INDEX[dbMain].properties;
      var siblings = fragmentsList.filter(function (f) {
        return f === baseFragment || f.indexOf(baseFragment + "_") === 0;
      });
      for (var i = 0; i < siblings.length; i++) {
        var ok = openDBFragment(dbMain, siblings[i]);
        if (!ok) continue;
        var row = lookUpInFragmentById(id, dbMain, siblings[i]);
        if (row != null) return row;
      }
      return null;
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
      deleteIdEntriesInFragment(id, dbMain, dbFragment);
    }

    function deleteFromDBById(id, { dbMain, dbFragment }) {
      id = normalizeId(id);
      if (dbMain && !dbFragment) {
        dbFragment = findFragmentForId(id, dbMain);
        if (!dbFragment) return;
      }
      const fragmentExistCheck = openDBFragment(dbMain, dbFragment);
      if (!fragmentExistCheck) return;
      deleteIdEntriesInFragment(id, dbMain, dbFragment);
    }

    function deleteIdEntriesInFragment(id, dbMain, dbFragment) {
      const open = OPEN_DB[openDbKey(dbMain, dbFragment)];
      // dbMain is passed explicitly; keep reading from properties for safety
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
      const { toWrite } = OPEN_DB[openDbKey(dbMain, dbFragment)];
      const id = resolveIdForKeyInFragment(key, dbMain, dbFragment);
      if (id == null) return null;
      return toWrite.data[id];
    }

    function lookUpInFragmentById(id, dbMain, dbFragment) {
      id = normalizeId(id);
      if (!INDEX[dbMain].dbFragments[dbFragment]) return null;
      const fragmentExistCheck = openDBFragment(dbMain, dbFragment);
      if (!fragmentExistCheck) return null;
      const { toWrite } = OPEN_DB[openDbKey(dbMain, dbFragment)];
      // data[id] is undefined when the id was deleted but the fragment idRange still covers it
      // (binary search correctly lands here; the row is simply absent)
      return toWrite.data[id] != null ? toWrite.data[id] : null;
    }

    function lookUpForKeysInFragment(id, dbMain, dbFragment) {
      if (!INDEX[dbMain].dbFragments[dbFragment]) return null;
      const { toWrite } = OPEN_DB[openDbKey(dbMain, dbFragment)];
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
      const { toWrite } = OPEN_DB[openDbKey(dbMain, dbFragment)];
      return toWrite.index[key];
    }

    /** Resolves id for key when ignoreIndex left the fragment index empty but data + keyToFragment exist. */
    function resolveIdForKeyInFragment(key, dbMain, dbFragment) {
      key = normalizeKey(key);
      var fromIdx = lookUpForIdInFragment(key, dbMain, dbFragment);
      if (fromIdx != null) return normalizeId(fromIdx);
      const { toWrite } = OPEN_DB[openDbKey(dbMain, dbFragment)];
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
      if (!OPEN_DB[openDbKey(dbMain, targetFragment)]) {
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
      var k = openDbKey(dbMain, dbFragment);
      if (OPEN_DB[k]) {
        LRU.touch(k);   // promote to MRU on every access
        return true;
      }
      if (!INDEX[dbMain].dbFragments[dbFragment]) return false;
      let fragmentFileObj;
      const { fileId } = INDEX[dbMain].dbFragments[dbFragment];
      if (fileId) fragmentFileObj = ToolkitAdapter.readFromJSON(fileId);
      addToOpenDBsObj(dbMain, dbFragment, fragmentFileObj);
      return true;
    }

    function addToOpenDBsObj(dbMain, dbFragment, fragmentFileObj) {
      var k = openDbKey(dbMain, dbFragment);
      evictLRUIfNeeded();
      OPEN_DB[k] = new OpenDBEntry(dbMain, fragmentFileObj);
      OPEN_DB[k].properties.fragment = dbFragment;
      LRU.add(k);
    }

    /**
     * Roll to a new cumulative fragment only when inserting a new id would exceed capacity.
     * In-place updates must not roll just because the shard already has MAX_ENTRIES_COUNT rows.
     */
    function checkOpenDBSize(dbMain, dbFragment, entryId) {
      const open = OPEN_DB[openDbKey(dbMain, dbFragment)];
      if (!open) return dbFragment;
      const { data } = open.toWrite;
      if (entryId != null) {
        const idKey = normalizeId(entryId);
        if (Object.prototype.hasOwnProperty.call(data, idKey) && data[idKey] != null) {
          return dbFragment;
        }
      }
      if (Object.keys(data).length >= instanceMaxEntries) {
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

    /**
     * Pre-create one base fragment per partition key for `dbMain`.
     *
     * Call this once when setting up a new DB so that each event / tenant / category
     * immediately owns its own shard — no INDEX lookup is needed for the first write
     * or read on each partition.
     *
     * @param {string}   dbMain        — table name (must exist in INDEX)
     * @param {string[]} partitionKeys — list of known partition values (e.g. event IDs)
     */
    function setupPartitions(dbMain, partitionKeys) {
      if (!INDEX[dbMain]) throw new Error("setupPartitions: unknown dbMain: " + dbMain);
      if (!isPartitioned(dbMain)) {
        throw new Error(
          "setupPartitions: dbMain '" + dbMain + "' has no partitionBy function. " +
          "Pass partitionBy['" + dbMain + "'] in init() options."
        );
      }
      var created = [];
      for (var i = 0; i < partitionKeys.length; i++) {
        var pk = String(partitionKeys[i]);
        var frag = partitionBaseFragment(dbMain, pk);
        if (!INDEX[dbMain].dbFragments[frag]) {
          addInIndexFile(dbMain, frag);
          created.push(frag);
        }
      }
      if (created.length > 0) markIndexRoutingDirty(dbMain);
      return created;
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
        const open = OPEN_DB[openDbKey(dbMain, dbFragment)];
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
          // findFragmentForId relies on disjoint id ranges across all fragments.
          // Partitioned tables have intentionally overlapping id ranges across
          // different partitions, so skip this cross-check for partitioned dbMains.
          if (!isPartitioned(dbMain)) {
            ids.forEach(function (nid) {
              var ff = findFragmentForId(nid, dbMain);
              if (ff !== dbFragment) {
                errors.push("findFragmentForId(" + nid + ")=" + ff + " expected " + dbFragment);
              }
            });
          }
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
      setupPartitions: setupPartitions,
      /** @internal testing / advanced introspection */
      _routing: {
        findFragmentForId: findFragmentForId,
        findFragmentForKey: findFragmentForKey,
        rebuildIdRangeSorted: rebuildIdRangeSorted,
        openDbKey: openDbKey,
        isPartitioned: isPartitioned,
        partitionFragmentForKey: partitionFragmentForKey
      },
      /** @internal LRU tracker — exposed for tests; do not mutate externally */
      _lru: LRU,
      maxEntriesCount: instanceMaxEntries,
      maxOpenFragments: instanceMaxOpenFragments
    };
  }

  return {
    init: init,
    MAX_ENTRIES_COUNT: MAX_ENTRIES_COUNT,
    normalizeId: normalizeId
  };
});
