import { describe, it, afterEach } from "node:test";
import { strict as assert } from "node:assert";
import { Dripline } from "../sdk.js";
import type { PluginDef } from "../plugin/types.js";

const mockPlugin: PluginDef = {
  name: "mock",
  version: "1.0.0",
  tables: [
    {
      name: "mock_items",
      columns: [
        { name: "id", type: "number" },
        { name: "name", type: "string" },
        { name: "value", type: "number" },
      ],
      keyColumns: [{ name: "category", required: "optional" }],
      *list(ctx) {
        const cat = ctx.quals.find((q) => q.column === "category")?.value;
        const items = [
          { id: 1, name: "a", value: 10, category: "x" },
          { id: 2, name: "b", value: 20, category: "x" },
          { id: 3, name: "c", value: 30, category: "y" },
        ];
        for (const item of items) {
          if (cat && item.category !== cat) continue;
          yield { id: item.id, name: item.name, value: item.value };
        }
      },
    },
  ],
};

let dl: Dripline;

afterEach(() => {
  dl?.close();
});

describe("Dripline SDK", () => {
  it("construct with plugin and query", () => {
    dl = new Dripline({ plugins: [mockPlugin] });
    const rows = dl.query("SELECT * FROM mock_items");
    assert.equal(rows.length, 3);
  });

  it("query with WHERE on key column", () => {
    dl = new Dripline({ plugins: [mockPlugin] });
    const rows = dl.query("SELECT * FROM mock_items WHERE category = 'x'");
    assert.equal(rows.length, 2);
  });

  it("query with aggregation", () => {
    dl = new Dripline({ plugins: [mockPlugin] });
    const rows = dl.query<{ total: number }>("SELECT SUM(value) as total FROM mock_items");
    assert.equal(rows[0].total, 60);
  });

  it("query with typed result", () => {
    dl = new Dripline({ plugins: [mockPlugin] });
    interface Item { id: number; name: string; value: number }
    const rows = dl.query<Item>("SELECT * FROM mock_items WHERE category = 'y'");
    assert.equal(rows[0].name, "c");
    assert.equal(rows[0].value, 30);
  });

  it("tables() lists available tables", () => {
    dl = new Dripline({ plugins: [mockPlugin] });
    const tables = dl.tables();
    assert.equal(tables.length, 1);
    assert.equal(tables[0].table, "mock_items");
    assert.equal(tables[0].plugin, "mock");
  });

  it("plugins() lists registered plugins", () => {
    dl = new Dripline({ plugins: [mockPlugin] });
    const plugins = dl.plugins();
    assert.equal(plugins.length, 1);
    assert.equal(plugins[0].name, "mock");
    assert.deepEqual(plugins[0].tables, ["mock_items"]);
  });

  it("cacheStats() returns stats", () => {
    dl = new Dripline({ plugins: [mockPlugin] });
    dl.query("SELECT * FROM mock_items");
    dl.query("SELECT * FROM mock_items"); // cache hit
    const stats = dl.cacheStats();
    assert.equal(stats.hits, 1);
  });

  it("clearCache() resets cache", () => {
    dl = new Dripline({ plugins: [mockPlugin] });
    dl.query("SELECT * FROM mock_items");
    dl.clearCache();
    assert.equal(dl.cacheStats().size, 0);
  });

  it("cache disabled", () => {
    dl = new Dripline({ plugins: [mockPlugin], cache: { enabled: false } });
    dl.query("SELECT * FROM mock_items");
    dl.query("SELECT * FROM mock_items");
    assert.equal(dl.cacheStats().hits, 0);
  });

  it("connection config passed to plugin", () => {
    let receivedConfig: any = null;
    const plugin: PluginDef = {
      name: "auth_test",
      version: "1.0.0",
      tables: [{
        name: "auth_items",
        columns: [{ name: "id", type: "number" }],
        *list(ctx) {
          receivedConfig = ctx.connection.config;
          yield { id: 1 };
        },
      }],
    };
    dl = new Dripline({
      plugins: [plugin],
      connections: [{ name: "myconn", plugin: "auth_test", config: { token: "secret" } }],
    });
    dl.query("SELECT * FROM auth_items");
    assert.equal(receivedConfig.token, "secret");
  });

  it("empty constructor works (no plugins)", () => {
    dl = new Dripline();
    const tables = dl.tables();
    assert.equal(tables.length, 0);
  });

  it("query pure SQL without plugins", () => {
    dl = new Dripline();
    const rows = dl.query<{ v: number }>("SELECT 1 + 1 as v");
    assert.equal(rows[0].v, 2);
  });

  it("close prevents further queries", () => {
    dl = new Dripline({ plugins: [mockPlugin] });
    dl.close();
    assert.throws(() => dl.query("SELECT 1"));
  });
});

describe("SDK import paths", () => {
  it("index.ts exports Dripline", async () => {
    const mod = await import("../index.js");
    assert.equal(typeof mod.Dripline, "function");
  });

  it("index.ts exports types and utilities", async () => {
    const mod = await import("../index.js");
    assert.equal(typeof mod.QueryEngine, "function");
    assert.equal(typeof mod.PluginRegistry, "function");
    assert.equal(typeof mod.QueryCache, "function");
    assert.equal(typeof mod.RateLimiter, "function");
    assert.equal(typeof mod.formatTable, "function");
    assert.equal(typeof mod.formatJson, "function");
    assert.equal(typeof mod.formatCsv, "function");
    assert.equal(typeof mod.syncGet, "function");
  });

  it("index.ts exports githubPlugin", async () => {
    const mod = await import("../index.js");
    assert.equal(mod.githubPlugin.name, "github");
  });
});
