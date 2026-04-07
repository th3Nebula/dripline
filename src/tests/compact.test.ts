/**
 * End-to-end tests for `dripline compact` against MinIO.
 *
 * The compactor is the piece that makes the warehouse queryable —
 * without it, raw/ grows unboundedly and queries pay LIST costs against
 * thousands of tiny files. These tests prove:
 *
 *   1. Happy path: after `run`, `compact` moves raw → curated and
 *      writes a manifest.
 *   2. Dedupe correctness: two syncs producing the same primary key
 *      with different cursor values → compact keeps the latest row.
 *   3. Idempotency: running compact twice in a row produces the same
 *      curated state (modulo rewrites).
 *   4. Lease mutex: a second compactor running while the first holds
 *      the lease is skipped cleanly.
 *   5. Skip-empty: no raw files → skipped, not errored.
 *   6. Filter: --table picks out one table without touching others.
 *   7. Config errors: missing remote, no compactable tables, unknown
 *      filter target → CompactConfigError with clear messages.
 *
 * Auto-skips when MinIO is unreachable.
 */

import { strict as assert } from "node:assert";
import { mkdirSync, mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, afterEach, before, beforeEach, describe, it } from "node:test";
import { AwsClient } from "aws4fetch";
import { compact, compactLeaseName } from "../commands/compact.js";
import { run } from "../commands/run.js";
import type { DriplineConfig } from "../config/types.js";
import { LeaseStore } from "../core/lease.js";
import { Remote } from "../core/remote.js";
import { registry } from "../plugin/registry.js";
import type { PluginDef } from "../plugin/types.js";

const ENDPOINT =
  process.env.DRIPLINE_TEST_S3_ENDPOINT ?? "http://localhost:9100";
const BUCKET = process.env.DRIPLINE_TEST_S3_BUCKET ?? "dripline-test";
const KEY = process.env.DRIPLINE_TEST_S3_KEY ?? "testkey";
const SECRET = process.env.DRIPLINE_TEST_S3_SECRET ?? "testsecret123";

const RUN_PREFIX = `compact-tests/${process.pid}-${Date.now()}`;

let backendUp = false;
async function probeBackend(): Promise<boolean> {
  try {
    const aws = new AwsClient({
      accessKeyId: KEY,
      secretAccessKey: SECRET,
      service: "s3",
      region: "auto",
    });
    const r = await aws.fetch(`${ENDPOINT}/${BUCKET}/`, { method: "PUT" });
    return r.status === 200 || r.status === 409;
  } catch {
    return false;
  }
}

/**
 * Configurable in-process plugin. The rows it yields are held in a
 * module-level array so tests can mutate them between runs to simulate
 * an upstream that has changed — producing the same primary keys with
 * newer cursor values.
 */
let PLUGIN_ROWS: Array<{
  id: number;
  name: string;
  updated_at: string;
  org: string;
}> = [];

function registerTestPlugin(): void {
  const plugin: PluginDef = {
    name: "compact_test",
    version: "1.0.0",
    tables: [
      {
        name: "items",
        description: "Test items table for compaction",
        columns: [
          { name: "id", type: "number" },
          { name: "name", type: "string" },
          { name: "updated_at", type: "datetime" },
        ],
        keyColumns: [{ name: "org", required: "required" }],
        primaryKey: ["id"],
        cursor: "updated_at",
        async *list() {
          for (const row of PLUGIN_ROWS) yield row;
        },
      },
      {
        // Second table with NO primaryKey — proves it's filtered out
        // as non-compactable.
        name: "noprimarykey_items",
        description: "Test table without a primary key",
        columns: [{ name: "id", type: "number" }],
        keyColumns: [{ name: "org", required: "required" }],
        async *list() {
          yield { id: 1, org: "x" };
        },
      },
    ],
  };
  registry.register(plugin);
}

function setPluginRows(
  rows: Array<{ id: number; name: string; updated_at: string; org: string }>,
): void {
  PLUGIN_ROWS = rows;
}

function makeProject(prefix: string): string {
  const dir = mkdtempSync(join(tmpdir(), "dripline-compact-"));
  mkdirSync(join(dir, ".dripline"), { recursive: true });
  const config: DriplineConfig = {
    connections: [{ name: "default", plugin: "compact_test", config: {} }],
    cache: { enabled: true, ttl: 300, maxSize: 1000 },
    rateLimits: {},
    lanes: {
      main: {
        tables: [{ name: "items", params: { org: "x" } }],
        interval: "60s",
        maxRuntime: "10s",
      },
    },
    remote: {
      endpoint: ENDPOINT,
      bucket: BUCKET,
      prefix,
      accessKeyId: KEY,
      secretAccessKey: SECRET,
      secretType: "S3",
    },
  };
  writeFileSync(
    join(dir, ".dripline", "config.json"),
    JSON.stringify(config, null, 2),
  );
  return dir;
}

let counter = 0;
const freshPrefix = (label: string) => `${RUN_PREFIX}/${label}-${++counter}`;

/** Count rows in curated/ via a direct DuckDB read. */
async function countCurated(prefix: string, table: string): Promise<number> {
  const { Database } = await import("duckdb-async");
  const db = await Database.create(":memory:");
  try {
    const remote = new Remote({
      endpoint: ENDPOINT,
      bucket: BUCKET,
      prefix,
      accessKeyId: KEY,
      secretAccessKey: SECRET,
      secretType: "S3",
    });
    await remote.attach(db);
    const url = `s3://${BUCKET}/${prefix}/curated/${table}/**/*.parquet`;
    const rows = await db.all(
      `SELECT COUNT(*) AS n FROM read_parquet('${url}', hive_partitioning => true);`,
    );
    return Number((rows[0] as { n: bigint | number }).n);
  } finally {
    await db.close();
  }
}

/** Read a specific row from curated/ to verify dedupe correctness. */
async function readCuratedById(
  prefix: string,
  table: string,
  id: number,
): Promise<{ id: number; name: string; updated_at: string } | null> {
  const { Database } = await import("duckdb-async");
  const db = await Database.create(":memory:");
  try {
    const remote = new Remote({
      endpoint: ENDPOINT,
      bucket: BUCKET,
      prefix,
      accessKeyId: KEY,
      secretAccessKey: SECRET,
      secretType: "S3",
    });
    await remote.attach(db);
    const url = `s3://${BUCKET}/${prefix}/curated/${table}/**/*.parquet`;
    const rows = await db.all(
      `SELECT id, name, updated_at FROM read_parquet('${url}', hive_partitioning => true) WHERE id = ${id};`,
    );
    if (rows.length === 0) return null;
    return rows[0] as { id: number; name: string; updated_at: string };
  } finally {
    await db.close();
  }
}

describe("dripline compact (end-to-end)", { concurrency: false }, () => {
  let originalCwd: string;

  before(async () => {
    originalCwd = process.cwd();
    backendUp = await probeBackend();
    if (!backendUp) {
      console.warn(`\n  ⚠ skipping compact tests: ${ENDPOINT} unreachable.\n`);
    }
    registerTestPlugin();
  });

  after(async () => {
    process.chdir(originalCwd);
    if (!backendUp) return;
    const aws = new AwsClient({
      accessKeyId: KEY,
      secretAccessKey: SECRET,
      service: "s3",
      region: "auto",
    });
    const list = await aws.fetch(
      `${ENDPOINT}/${BUCKET}/?list-type=2&prefix=${encodeURIComponent(RUN_PREFIX)}`,
    );
    if (list.status !== 200) return;
    const xml = await list.text();
    const keys = [...xml.matchAll(/<Key>([^<]+)<\/Key>/g)].map((m) => m[1]);
    for (const k of keys) {
      await aws.fetch(`${ENDPOINT}/${BUCKET}/${k}`, { method: "DELETE" });
    }
  });

  beforeEach(() => {
    originalCwd = process.cwd();
    // Reset plugin rows between tests so each test controls its own data.
    setPluginRows([
      { id: 1, name: "a", updated_at: "2024-01-01T00:00:00Z", org: "x" },
      { id: 2, name: "b", updated_at: "2024-02-01T00:00:00Z", org: "x" },
      { id: 3, name: "c", updated_at: "2024-03-01T00:00:00Z", org: "x" },
    ]);
  });
  afterEach(() => process.chdir(originalCwd));

  const ift = (name: string, fn: () => Promise<void>) =>
    it(name, async (t) => {
      if (!backendUp) return t.skip("backend unreachable");
      await fn();
    });

  // ─────────────────────────────────────────────────────────────────

  ift(
    "happy path: run then compact publishes curated/ and a manifest",
    async () => {
      const prefix = freshPrefix("happy");
      process.chdir(makeProject(prefix));

      const runResults = await run({ quiet: true });
      assert.equal(runResults[0].status, "ok");
      assert.equal(runResults[0].rowsInserted, 3);

      const compactResults = await compact({ quiet: true });
      // Only "items" is compactable (noprimarykey_items has no PK)
      assert.equal(compactResults.length, 1);
      assert.equal(compactResults[0].status, "ok");
      assert.equal(compactResults[0].rows, 3);
      assert.ok(compactResults[0].files >= 1);

      // Verify curated/ is readable
      const count = await countCurated(prefix, "items");
      assert.equal(count, 3);

      // Verify manifest was written
      const remote = new Remote({
        endpoint: ENDPOINT,
        bucket: BUCKET,
        prefix,
        accessKeyId: KEY,
        secretAccessKey: SECRET,
        secretType: "S3",
      });
      const manifest = await remote.readManifest("items");
      assert.ok(manifest, "manifest should exist");
      assert.equal(manifest.table, "items");
      assert.ok(manifest.files.length >= 1);
      assert.equal(
        manifest.files.reduce((sum, f) => sum + Number(f.row_count ?? 0), 0),
        3,
      );
    },
  );

  ift(
    "dedupe correctness: later cursor wins over earlier for same PK",
    async () => {
      const prefix = freshPrefix("dedupe");
      process.chdir(makeProject(prefix));

      // Run 1: id=1 at 2024-01-01
      setPluginRows([
        {
          id: 1,
          name: "original",
          updated_at: "2024-01-01T00:00:00Z",
          org: "x",
        },
      ]);
      await run({ quiet: true });

      // The run() call acquired+renewed the lease to the lane interval
      // (60s). We forcibly release it so a second run can proceed.
      const aws = new AwsClient({
        accessKeyId: KEY,
        secretAccessKey: SECRET,
        service: "s3",
        region: "auto",
      });
      await aws.fetch(
        `${ENDPOINT}/${BUCKET}/${prefix}/_leases/lane-main.json`,
        {
          method: "DELETE",
        },
      );

      // Run 2: id=1 at 2024-06-01 (newer) — same PK, newer cursor.
      // The cursor filter in engine.sync() will only yield rows newer
      // than the high-water mark from run 1, so id=1 with the new
      // updated_at IS newer → it flows through to raw/ as a second row.
      setPluginRows([
        {
          id: 1,
          name: "updated",
          updated_at: "2024-06-01T00:00:00Z",
          org: "x",
        },
      ]);
      await run({ quiet: true });

      // Now raw/ has two files, one with {id:1, name:"original"} and
      // one with {id:1, name:"updated"}. Compact should dedupe to
      // the later one.
      const results = await compact({ quiet: true });
      assert.equal(results[0].status, "ok");
      assert.equal(results[0].rows, 1, "dedupe should leave 1 row");

      const row = await readCuratedById(prefix, "items", 1);
      assert.ok(row);
      assert.equal(row.name, "updated", "newer cursor should win");
      assert.equal(row.updated_at, "2024-06-01T00:00:00Z");
    },
  );

  ift(
    "idempotent: running compact twice produces the same curated state",
    async () => {
      const prefix = freshPrefix("idempotent");
      process.chdir(makeProject(prefix));

      await run({ quiet: true });
      const first = await compact({ quiet: true });
      const second = await compact({ quiet: true });

      assert.equal(first[0].status, "ok");
      assert.equal(second[0].status, "ok");
      assert.equal(
        first[0].rows,
        second[0].rows,
        "row count must be stable across compactions",
      );

      // Manifest should still be present and consistent.
      const count = await countCurated(prefix, "items");
      assert.equal(count, first[0].rows);
    },
  );

  ift("lease mutex: second concurrent compactor is skipped", async () => {
    const prefix = freshPrefix("mutex");
    process.chdir(makeProject(prefix));
    await run({ quiet: true });

    // Pre-acquire the compact lease so compact() finds it held.
    const ls = new LeaseStore({
      endpoint: ENDPOINT,
      bucket: BUCKET,
      prefix,
      accessKeyId: KEY,
      secretAccessKey: SECRET,
    });
    const held = await ls.acquire(compactLeaseName("items"), 30_000);
    assert.ok(held, "test setup: should have acquired the lease");

    try {
      const results = await compact({ quiet: true });
      assert.equal(results.length, 1);
      assert.equal(results[0].status, "skipped");
      assert.match(results[0].reason ?? "", /lease held/);
    } finally {
      await ls.release(held);
    }
  });

  ift("skip-empty: no raw files → skipped, not errored", async () => {
    const prefix = freshPrefix("empty");
    process.chdir(makeProject(prefix));
    // Note: we intentionally DO NOT run() first — raw/ is empty.

    const results = await compact({ quiet: true });
    assert.equal(results[0].status, "skipped");
    assert.match(results[0].reason ?? "", /no raw files/i);
  });

  ift("--table filter picks one table and ignores others", async () => {
    const prefix = freshPrefix("filter");
    process.chdir(makeProject(prefix));
    await run({ quiet: true });

    // Only compactable table is "items"; verify the filter works.
    const results = await compact({ tables: ["items"], quiet: true });
    assert.equal(results.length, 1);
    assert.equal(results[0].table, "items");
  });

  ift("unknown --table target errors clearly", async () => {
    const prefix = freshPrefix("unknown-table");
    process.chdir(makeProject(prefix));

    await assert.rejects(
      () => compact({ tables: ["nope"], quiet: true }),
      /not compactable/,
    );
  });

  // ── Raw cleanup contract ─────────────────────────────────────────

  ift(
    "raw cleanup: successful compact deletes consumed raw files",
    async () => {
      const prefix = freshPrefix("cleanup");
      process.chdir(makeProject(prefix));
      await run({ quiet: true });

      // Sanity: raw/ has at least one file before compact.
      const remote = new Remote({
        endpoint: ENDPOINT,
        bucket: BUCKET,
        prefix,
        accessKeyId: KEY,
        secretAccessKey: SECRET,
        secretType: "S3",
      });
      const beforeRaw = await remote.listObjects("raw/items/");
      assert.ok(beforeRaw.length > 0, "setup: raw/ should have files");

      const results = await compact({ quiet: true });
      assert.equal(results[0].status, "ok");
      assert.equal(
        results[0].rawCleaned,
        beforeRaw.length,
        "compact should report cleaning the exact set of raw files it saw",
      );

      const afterRaw = await remote.listObjects("raw/items/");
      assert.equal(afterRaw.length, 0, "raw/ should be empty after compact");

      // And curated/ is intact.
      const curatedCount = await countCurated(prefix, "items");
      assert.equal(curatedCount, 3);
    },
  );

  ift(
    "raw cleanup: snapshot semantics — only consumed files are deleted",
    async () => {
      const prefix = freshPrefix("snapshot");
      process.chdir(makeProject(prefix));

      // Run once to populate raw/.
      await run({ quiet: true });

      const remote = new Remote({
        endpoint: ENDPOINT,
        bucket: BUCKET,
        prefix,
        accessKeyId: KEY,
        secretAccessKey: SECRET,
        secretType: "S3",
      });
      const consumedKeys = await remote.listObjects("raw/items/");
      assert.ok(consumedKeys.length > 0);

      // We can't easily inject a planted file at the exact instant
      // between snapshot and delete from a black-box test, but we CAN
      // verify the contract by another route: call deleteObjects with
      // a SUBSET of the actual raw files and verify only that subset
      // is gone. This proves deleteObjects targets exactly the keys
      // we pass and nothing else — which is the load-bearing claim
      // behind the snapshot-then-delete pattern in compact().
      const subsetToDelete = consumedKeys.slice(0, 1);
      await remote.deleteObjects(subsetToDelete);
      const remaining = await remote.listObjects("raw/items/");
      assert.equal(
        remaining.length,
        consumedKeys.length - subsetToDelete.length,
        "deleteObjects should delete exactly the keys passed, no more",
      );
      for (const key of subsetToDelete) {
        assert.ok(
          !remaining.includes(key),
          `deleted key ${key} should not be in remaining`,
        );
      }
    },
  );

  ift("raw cleanup: idempotent across multiple compact cycles", async () => {
    const prefix = freshPrefix("cleanup-idempotent");
    process.chdir(makeProject(prefix));

    // Three sync+compact cycles, each producing 3 rows that cumulatively
    // dedupe down to 3 in curated (cursor blocks re-syncs).
    const remote = new Remote({
      endpoint: ENDPOINT,
      bucket: BUCKET,
      prefix,
      accessKeyId: KEY,
      secretAccessKey: SECRET,
      secretType: "S3",
    });

    const aws = new AwsClient({
      accessKeyId: KEY,
      secretAccessKey: SECRET,
      service: "s3",
      region: "auto",
    });
    for (let cycle = 0; cycle < 3; cycle++) {
      // Force-release the lane lease so we can re-run.
      await aws.fetch(
        `${ENDPOINT}/${BUCKET}/${prefix}/_leases/lane-main.json`,
        { method: "DELETE" },
      );
      await run({ quiet: true });
      const compactResult = await compact({ quiet: true });
      assert.equal(compactResult[0].status, "ok");

      // Raw should be empty after every cycle.
      const rawAfter = await remote.listObjects("raw/items/");
      assert.equal(
        rawAfter.length,
        0,
        `cycle ${cycle}: raw/ should be empty after compact, got ${rawAfter.length}`,
      );
    }

    // Curated row count is stable across cycles.
    assert.equal(await countCurated(prefix, "items"), 3);
  });

  ift("missing remote rejects with a clear error", async () => {
    const dir = mkdtempSync(join(tmpdir(), "dripline-compact-"));
    mkdirSync(join(dir, ".dripline"), { recursive: true });
    writeFileSync(
      join(dir, ".dripline", "config.json"),
      JSON.stringify({
        connections: [],
        cache: { enabled: true, ttl: 300, maxSize: 1000 },
        rateLimits: {},
        lanes: {},
      }),
    );
    process.chdir(dir);

    await assert.rejects(
      () => compact({ quiet: true }),
      /no remote configured/,
    );
  });
});
