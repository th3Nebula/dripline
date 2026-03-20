// ── SDK entrypoint ──
// Import dripline as a library:
//   import { Dripline } from "dripline";

export { Dripline } from "./sdk.js";

// Core engine
export { QueryEngine, createEngine } from "./engine.js";

// Plugin SDK — everything needed to write a plugin
export type {
  PluginDef,
  TableDef,
  ColumnDef,
  ColumnType,
  KeyColumn,
  ListFunc,
  GetFunc,
  HydrateFunc,
  QueryContext,
  Qual,
  ConnectionConfig,
  RateLimitConfig,
  CacheEntry,
} from "./plugin/types.js";

export { PluginRegistry, registry } from "./plugin/registry.js";
export {
  loadPluginFromPath,
  loadBuiltinPlugins,
  loadPluginsFromConfig,
} from "./plugin/loader.js";

// Config
export type { DriplineConfig, CacheConfig } from "./config/types.js";
export { DEFAULT_CONFIG } from "./config/types.js";
export {
  findConfigDir,
  loadConfig,
  saveConfig,
  getConnection,
  addConnection,
  removeConnection,
} from "./config/loader.js";

// Cache & Rate Limiter
export { QueryCache, queryCache, configureCache } from "./cache.js";
export { RateLimiter, rateLimiter } from "./rate-limiter.js";

// Formatters
export { formatTable } from "./utils/table-formatter.js";
export { formatJson, formatCsv, formatLine } from "./utils/formatters.js";

// HTTP utilities for plugin authors
export { syncGet, syncGetPaginated } from "./plugins/utils/http.js";

// Built-in plugins
export { default as githubPlugin } from "./plugins/github.js";
