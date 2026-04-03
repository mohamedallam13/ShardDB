"use strict";

const path = require("path");
const { pathToFileURL } = require("node:url");

let loaded = false;

/**
 * Loads @mcpher/gas-fakes (Apps Script globals: CacheService, Utilities, ScriptApp, DriveApp, …).
 * Safe to call multiple times; only the first import runs.
 *
 * Uses `src/index.js` (not `main.js`) to avoid extra CLI noise.
 */
async function bootstrapGasFakes() {
  if (loaded) return;
  const root = path.dirname(require.resolve("@mcpher/gas-fakes/package.json"));
  await import(pathToFileURL(path.join(root, "src/index.js")));
  loaded = true;
}

module.exports = { bootstrapGasFakes };
