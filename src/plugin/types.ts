export type ColumnType = "string" | "number" | "boolean" | "json" | "datetime";

export interface ColumnDef {
  name: string;
  type: ColumnType;
  description?: string;
}

export interface KeyColumn {
  name: string;
  /** @deprecated No longer used — all operators are extracted automatically */
  operators?: string[];
  required: "required" | "optional" | "any_of";
}

export interface Qual {
  column: string;
  operator: string;
  value: any;
}

export interface ConnectionConfig {
  name: string;
  plugin: string;
  config: Record<string, any>;
}

export interface QueryContext {
  connection: ConnectionConfig;
  quals: Qual[];
  columns: string[];
  limit?: number;
  /** High-water mark from previous sync. null on first sync, undefined during query(). */
  cursor?: { column: string; value: any } | null;
}

export type ListFunc = (
  ctx: QueryContext,
) =>
  | Generator<Record<string, any>>
  | AsyncGenerator<Record<string, any>>;
export type GetFunc = (ctx: QueryContext) => Record<string, any> | null;
export type HydrateFunc = (
  ctx: QueryContext,
  row: Record<string, any>,
) => Record<string, any>;

export interface TableDef {
  name: string;
  columns: ColumnDef[];
  keyColumns?: KeyColumn[];
  /** Row identity columns for deduplication during sync. */
  primaryKey?: string[];
  /** Default params for sync() — used when caller doesn't provide params for this table. */
  syncParams?: Record<string, any>;
  /** Column name used as high-water mark for incremental sync. Type inferred from columns[]. */
  cursor?: string;
  list: ListFunc;
  get?: GetFunc;
  hydrate?: Record<string, HydrateFunc>;
  description?: string;
}

export interface PluginDef {
  name: string;
  version: string;
  tables: TableDef[];
  connectionConfigSchema?: Record<
    string,
    {
      type: string;
      required?: boolean;
      description?: string;
      default?: any;
      env?: string;
    }
  >;
}

export interface CacheEntry<T> {
  data: T[];
  timestamp: number;
  ttl: number;
}

export interface RateLimitConfig {
  maxPerSecond?: number;
  maxPerMinute?: number;
  maxConcurrent?: number;
}
