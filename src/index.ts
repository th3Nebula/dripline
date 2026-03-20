
export { Dripline } from "./sdk.js";

export { QueryEngine, createEngine } from "./engine.js";

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
  loadAllPlugins,
} from "./plugin/loader.js";

export { createPluginAPI, resolvePluginExport, isPluginFunction } from "./plugin/api.js";
export type {
  DriplinePluginAPI,
  PluginFunction,
  TableDefinition,
  SchemaField,
} from "./plugin/api.js";

export { parsePluginSource, installPlugin, removePlugin, listInstalled } from "./plugin/installer.js";
export type { PluginSource, InstalledPlugin } from "./plugin/installer.js";

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

export { QueryCache, queryCache, configureCache } from "./cache.js";
export { RateLimiter, rateLimiter } from "./rate-limiter.js";

export { formatTable } from "./utils/table-formatter.js";
export { formatJson, formatCsv, formatLine } from "./utils/formatters.js";

export { syncGet, syncGetPaginated } from "./plugins/utils/http.js";

export { default as githubPlugin } from "./plugins/github.js";
