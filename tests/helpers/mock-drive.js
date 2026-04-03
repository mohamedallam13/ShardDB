"use strict";

const fs = require("fs");
const path = require("path");

/**
 * Filesystem-backed mock Drive for Node tests. Each fileId maps to one JSON file.
 */
function createMockDrive({ dbDir, defaultIndexPayload } = {}) {
  const DB_DIR = dbDir || path.join(__dirname, "../.mock_drive");
  if (!fs.existsSync(DB_DIR)) fs.mkdirSync(DB_DIR, { recursive: true });

  const defaultPayload = defaultIndexPayload || {
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

  return {
    DB_DIR: DB_DIR,
    adapter: {
      readFromJSON: function (fileId) {
        const p = path.join(DB_DIR, fileId + ".json");
        if (!fs.existsSync(p)) return JSON.parse(JSON.stringify(defaultPayload));
        return JSON.parse(fs.readFileSync(p, "utf8"));
      },
      writeToJSON: function (fileId, payload) {
        const p = path.join(DB_DIR, fileId + ".json");
        fs.writeFileSync(p, JSON.stringify(payload));
      },
      createJSON: function (name, root, payload) {
        const id = "mock_id_" + name + "_" + Date.now() + "_" + Math.random().toString(36).slice(2);
        this.writeToJSON(id, payload);
        return id;
      },
      deleteFile: function (fileId) {
        const p = path.join(DB_DIR, fileId + ".json");
        if (fs.existsSync(p)) fs.unlinkSync(p);
      }
    },
    wipe: function () {
      if (fs.existsSync(DB_DIR)) fs.rmSync(DB_DIR, { recursive: true, force: true });
      fs.mkdirSync(DB_DIR, { recursive: true });
    }
  };
}

/**
 * Wraps a ToolkitAdapter to count writeToJSON calls (for asserting flush / dirty behavior).
 */
function wrapAdapterWithWriteCounts(baseAdapter, { indexFileId } = {}) {
  let writeCount = 0;
  let fragmentWriteCount = 0;
  let indexWriteCount = 0;
  const adapter = {
    readFromJSON: function (fileId) {
      return baseAdapter.readFromJSON(fileId);
    },
    writeToJSON: function (fileId, payload) {
      writeCount++;
      if (indexFileId != null && fileId === indexFileId) indexWriteCount++;
      else fragmentWriteCount++;
      return baseAdapter.writeToJSON(fileId, payload);
    },
    // createJSON must call through this adapter so mock createJSON's this.writeToJSON hits the wrapper counts.
    createJSON: function (name, root, payload) {
      return baseAdapter.createJSON.call(adapter, name, root, payload);
    },
    deleteFile: function (fileId) {
      return baseAdapter.deleteFile(fileId);
    }
  };
  return {
    adapter: adapter,
    counts: function () {
      return {
        writeCount: writeCount,
        fragmentWriteCount: fragmentWriteCount,
        indexWriteCount: indexWriteCount
      };
    },
    reset: function () {
      writeCount = 0;
      fragmentWriteCount = 0;
      indexWriteCount = 0;
    }
  };
}

module.exports = { createMockDrive, wrapAdapterWithWriteCounts };
