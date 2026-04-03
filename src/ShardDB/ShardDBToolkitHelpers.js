/**
 * ShardDB toolkit helpers — live inside this library (no dependency on AtlasToolkit).
 *
 * Drive surfaces (Apps Script):
 * - DriveApp: high-level wrapper; same Google Drive backend as the REST API.
 * - Advanced Drive service (Drive.Files / v2–v3): thin client over the same HTTP API.
 * - Neither is inherently “faster” than the other for a single read/write; what dominates
 *   is how many round-trips you make. Batch where the API allows it; avoid N calls per row.
 *
 * This module uses DriveApp only so consumers do not need another library for basic I/O.
 * You can still pass a custom ToolkitAdapter (e.g. from AtlasToolkit or Advanced Drive).
 */
;(function (root, factory) {
  root.SHARD_DB_TOOLKIT = factory();
})(this, function () {
  /**
   * @typedef {{ readFromJSON: function(string): *, writeToJSON: function(string, *): void, createJSON: function(string, string, *): string, deleteFile: function(string): void }} ToolkitAdapter
   */

  function assertDrive() {
    if (typeof DriveApp === "undefined") {
      throw new Error("ShardDB toolkit: DriveApp is not available (run in Google Apps Script).");
    }
  }

  /**
   * ToolkitAdapter implemented with DriveApp only (no other Atlas libraries).
   * @param {{ useTrashed?: boolean }} [options]
   * @returns {ToolkitAdapter}
   */
  function createDriveToolkitAdapter(options) {
    assertDrive();
    options = options || {}; // reserved for future flags (e.g. trash vs hard delete)

    function readFromJSON(fileId) {
      if (!fileId) return null;
      try {
        const file = DriveApp.getFileById(fileId);
        const s = file.getBlob().getDataAsString();
        return JSON.parse(s);
      } catch (e) {
        return null;
      }
    }

    function writeToJSON(fileId, payload) {
      if (!fileId) return;
      const str = typeof payload === "string" ? payload : JSON.stringify(payload);
      DriveApp.getFileById(fileId).setContent(str);
    }

    function createJSON(name, rootFolderId, payload) {
      const folder = DriveApp.getFolderById(rootFolderId);
      const str = typeof payload === "string" ? payload : JSON.stringify(payload);
      const file = folder.createFile(name + ".json", str, "application/json");
      return file.getId();
    }

    function deleteFile(fileId) {
      if (!fileId) return;
      try {
        DriveApp.getFileById(fileId).setTrashed(true);
      } catch (e) {}
    }

    return {
      readFromJSON: readFromJSON,
      writeToJSON: writeToJSON,
      createJSON: createJSON,
      deleteFile: deleteFile
    };
  }

  /**
   * Derive a sibling backup filename in the same folder: "index.json" -> "index.backup.json"
   */
  function backupFileNameFromMain(mainName) {
    const m = mainName.match(/^(.+)(\.json)$/i);
    if (m) return m[1] + ".backup" + m[2];
    return mainName + ".backup.json";
  }

  /**
   * Wraps a ToolkitAdapter so every successful write also updates a sibling backup file,
   * and read tries the backup if the primary read throws or returns null/undefined.
   * Uses DriveApp for sibling discovery (same folder, backup name convention).
   *
   * @param {ToolkitAdapter} inner
   * @param {{ enabled?: boolean }} [options]
   * @returns {ToolkitAdapter}
   */
  function wrapWithBackupRestore(inner, options) {
    assertDrive();
    options = options || {};
    if (options.enabled === false) return inner;

    function getOrCreateBackupFileId(mainFileId) {
      const main = DriveApp.getFileById(mainFileId);
      const parents = main.getParents();
      if (!parents.hasNext()) return null;
      const folder = parents.next();
      const backupName = backupFileNameFromMain(main.getName());
      const it = folder.getFilesByName(backupName);
      const str = "{}"; // minimal valid JSON until first real write syncs
      if (it.hasNext()) return it.next().getId();
      const created = folder.createFile(backupName, str, MimeType.PLAIN_TEXT);
      return created.getId();
    }

    function writeBackupMirror(mainFileId, payload) {
      try {
        const bid = getOrCreateBackupFileId(mainFileId);
        if (bid) inner.writeToJSON(bid, payload);
      } catch (e) {
        // Backup is best-effort; primary write already succeeded via caller order
      }
    }

    function readBackupMirror(mainFileId) {
      try {
        const main = DriveApp.getFileById(mainFileId);
        const parents = main.getParents();
        if (!parents.hasNext()) return null;
        const folder = parents.next();
        const backupName = backupFileNameFromMain(main.getName());
        const it = folder.getFilesByName(backupName);
        if (!it.hasNext()) return null;
        const bid = it.next().getId();
        return inner.readFromJSON(bid);
      } catch (e) {
        return null;
      }
    }

    return {
      readFromJSON: function (fileId) {
        if (!fileId) return null;
        var primary = null;
        var err = null;
        try {
          primary = inner.readFromJSON(fileId);
        } catch (e) {
          err = e;
        }
        if (primary !== null && primary !== undefined) return primary;
        var restored = readBackupMirror(fileId);
        if (restored !== null && restored !== undefined) return restored;
        if (err) throw err;
        return null;
      },
      writeToJSON: function (fileId, payload) {
        inner.writeToJSON(fileId, payload);
        writeBackupMirror(fileId, payload);
      },
      createJSON: function (name, rootFolderId, payload) {
        const id = inner.createJSON(name, rootFolderId, payload);
        try {
          writeBackupMirror(id, payload);
        } catch (e) {}
        return id;
      },
      deleteFile: function (fileId) {
        var backupId = null;
        try {
          const main = DriveApp.getFileById(fileId);
          const parents = main.getParents();
          if (parents.hasNext()) {
            const folder = parents.next();
            const it = folder.getFilesByName(backupFileNameFromMain(main.getName()));
            if (it.hasNext()) backupId = it.next().getId();
          }
        } catch (e) {}
        inner.deleteFile(fileId);
        if (backupId) {
          try {
            inner.deleteFile(backupId);
          } catch (e2) {}
        }
      }
    };
  }

  return {
    createDriveToolkitAdapter: createDriveToolkitAdapter,
    wrapWithBackupRestore: wrapWithBackupRestore,
    backupFileNameFromMain: backupFileNameFromMain
  };
});
