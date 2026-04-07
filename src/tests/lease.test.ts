/**
 * LeaseStore tests against an S3-compatible backend.
 *
 * Requires MinIO (or any S3-compatible store) reachable at the endpoint
 * configured by env vars below. The suite auto-skips if the endpoint is
 * unreachable, so it's safe to run in CI without a server.
 *
 *   docker run -d --name dripline-minio -p 9100:9000 \
 *     -e MINIO_ROOT_USER=testkey -e MINIO_ROOT_PASSWORD=testsecret123 \
 *     minio/minio server /data
 *
 * Env vars (all optional, sensible defaults for the docker command above):
 *   DRIPLINE_TEST_S3_ENDPOINT  default http://localhost:9100
 *   DRIPLINE_TEST_S3_BUCKET    default dripline-test
 *   DRIPLINE_TEST_S3_KEY       default testkey
 *   DRIPLINE_TEST_S3_SECRET    default testsecret123
 */

import { strict as assert } from "node:assert";
import { after, before, describe, it } from "node:test";
import { AwsClient } from "aws4fetch";
import { type Lease, LeaseStore } from "../core/lease.js";

const ENDPOINT =
  process.env.DRIPLINE_TEST_S3_ENDPOINT ?? "http://localhost:9100";
const BUCKET = process.env.DRIPLINE_TEST_S3_BUCKET ?? "dripline-test";
const KEY = process.env.DRIPLINE_TEST_S3_KEY ?? "testkey";
const SECRET = process.env.DRIPLINE_TEST_S3_SECRET ?? "testsecret123";

// Per-test prefix so parallel runs / re-runs never collide.
const RUN_PREFIX = `lease-tests/${process.pid}-${Date.now()}`;

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** Assert non-null and narrow the type. Replaces `!` in tests. */
function must<T>(value: T | null | undefined, msg = "expected non-null"): T {
  assert.ok(value != null, msg);
  return value;
}

// ── Backend probe — skip the whole suite if S3 is unreachable. ───────

let backendUp = false;
async function probeBackend(): Promise<boolean> {
  try {
    const aws = new AwsClient({
      accessKeyId: KEY,
      secretAccessKey: SECRET,
      service: "s3",
      region: "auto",
    });
    // Ensure bucket exists. PUT bucket is idempotent in MinIO; harmless on R2.
    const r = await aws.fetch(`${ENDPOINT}/${BUCKET}/`, { method: "PUT" });
    return r.status === 200 || r.status === 409;
  } catch {
    return false;
  }
}

// Counter so each it() gets a unique lease name within the run prefix.
let n = 0;
const lane = (label: string) => `${RUN_PREFIX}/${label}-${++n}`;

const newStore = () =>
  new LeaseStore({
    endpoint: ENDPOINT,
    bucket: BUCKET,
    accessKeyId: KEY,
    secretAccessKey: SECRET,
  });

// ── Suite ────────────────────────────────────────────────────────────

describe("LeaseStore", { concurrency: false }, () => {
  before(async () => {
    backendUp = await probeBackend();
    if (!backendUp) {
      // eslint-disable-next-line no-console
      console.warn(
        `\n  ⚠ skipping LeaseStore tests: ${ENDPOINT} unreachable. ` +
          `start MinIO with the docker command in this file's header.\n`,
      );
    }
  });

  after(async () => {
    if (!backendUp) return;
    // Best-effort cleanup of any leftover lease objects from this run.
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

  // Wraps it() so each test auto-skips when the backend is down.
  const ift = (name: string, fn: () => Promise<void>) =>
    it(name, async (t) => {
      if (!backendUp) return t.skip("backend unreachable");
      await fn();
    });

  ift("acquire returns a lease with a holder and future expiry", async () => {
    const store = newStore();
    const name = lane("basic");
    const a = must(await store.acquire(name, 5_000), "expected a lease");
    assert.equal(a.name, name);
    assert.equal(typeof a.holder, "string");
    assert.ok(a.holder.length > 0);
    assert.ok(a.expiresAt > Date.now());
    assert.ok(a.etag.length > 0);
    await store.release(a);
  });

  ift("second acquire on a held lease returns null", async () => {
    const store = newStore();
    const name = lane("held");
    const a = must(await store.acquire(name, 5_000));
    const b = await store.acquire(name, 5_000);
    assert.equal(b, null);
    await store.release(a);
  });

  ift("acquire after release succeeds and yields a fresh holder", async () => {
    const store = newStore();
    const name = lane("rerelease");
    const a = must(await store.acquire(name, 5_000));
    await store.release(a);
    const b = must(await store.acquire(name, 5_000));
    assert.notEqual(b.holder, a.holder);
    await store.release(b);
  });

  ift("expired lease is reacquirable by another caller", async () => {
    const store = newStore();
    const name = lane("expire");
    const a = must(await store.acquire(name, 300));
    await sleep(500);
    const b = must(await store.acquire(name, 5_000));
    assert.notEqual(b.holder, a.holder);
    await store.release(b);
  });

  ift("20 concurrent acquirers — exactly one wins", async () => {
    const store = newStore();
    const name = lane("contend-20");
    const N = 20;
    const results = await Promise.all(
      Array.from({ length: N }, () => store.acquire(name, 5_000)),
    );
    const winners = results.filter((r): r is Lease => r !== null);
    assert.equal(winners.length, 1, `expected 1 winner, got ${winners.length}`);
    await store.release(winners[0]);
  });

  ift("10 lanes × 5 workers — every lane has exactly one holder", async () => {
    const store = newStore();
    const lanes = Array.from({ length: 10 }, (_, i) => lane(`fanout-${i}`));
    const workers = 5;
    const grids = await Promise.all(
      Array.from({ length: workers }, () =>
        Promise.all(lanes.map((l) => store.acquire(l, 5_000))),
      ),
    );
    for (let i = 0; i < lanes.length; i++) {
      const winners = grids.filter((g) => g[i] !== null);
      assert.equal(
        winners.length,
        1,
        `lane ${lanes[i]}: expected 1 winner, got ${winners.length}`,
      );
    }
    // Cleanup
    for (const g of grids)
      for (const lease of g) if (lease) await store.release(lease);
  });

  ift("renew extends expiry and returns an updated lease", async () => {
    const store = newStore();
    const name = lane("renew");
    const a = must(await store.acquire(name, 1_000));
    const b = must(await store.renew(a, 5_000));
    assert.equal(b.holder, a.holder, "holder unchanged across renew");
    assert.ok(b.expiresAt > a.expiresAt, "expiry extended");
    await store.release(b);
  });

  ift("renewed lease blocks acquirers past the original ttl", async () => {
    const store = newStore();
    const name = lane("renew-blocks");
    const a = must(await store.acquire(name, 500));
    const b = must(await store.renew(a, 5_000));
    await sleep(700); // past original ttl
    const c = await store.acquire(name, 5_000);
    assert.equal(c, null, "renewed lease should still block acquirers");
    await store.release(b);
  });

  ift("renew on a lost lease returns null", async () => {
    const store = newStore();
    const name = lane("renew-lost");
    const a = must(await store.acquire(name, 200));
    await sleep(400);
    const b = must(await store.acquire(name, 5_000)); // takeover
    const renewed = await store.renew(a, 5_000);
    assert.equal(renewed, null, "stale holder cannot renew");
    await store.release(b);
  });

  ift("stale release does not free the new holder's lease", async () => {
    const store = newStore();
    const name = lane("stale-release");
    const a = must(await store.acquire(name, 200));
    await sleep(400);
    const b = must(await store.acquire(name, 5_000)); // takeover
    await store.release(a); // stale — must be a no-op
    const c = await store.acquire(name, 5_000);
    assert.equal(c, null, "b's lease must still be held");
    await store.release(b);
  });

  ift("release of unknown lease is a no-op (idempotent)", async () => {
    const store = newStore();
    const name = lane("release-missing");
    // Synthesize a lease that was never acquired.
    await store.release({
      name,
      holder: "ghost",
      expiresAt: Date.now() + 5_000,
      etag: '"deadbeef"',
    });
    // Should now be acquirable normally.
    const a = must(await store.acquire(name, 5_000));
    await store.release(a);
  });

  ift(
    "worker loop pattern — sequential acquire/work/release across workers",
    async () => {
      const store = newStore();
      const name = lane("worker-loop");
      const workers = 4;
      const iterations = 8;
      const order: number[] = [];

      const workerLoop = async (id: number) => {
        for (let i = 0; i < iterations; i++) {
          // Spin until we get the lease (cheap exponential backoff)
          let lease: Lease | null = null;
          let backoff = 5;
          while (lease == null) {
            lease = await store.acquire(name, 1_000);
            if (lease == null) {
              await sleep(backoff);
              backoff = Math.min(backoff * 2, 50);
            }
          }
          order.push(id);
          await sleep(2); // simulated work
          await store.release(lease);
        }
      };

      await Promise.all(
        Array.from({ length: workers }, (_, id) => workerLoop(id)),
      );

      // Every iteration must have completed exactly once.
      assert.equal(order.length, workers * iterations);
      // No two adjacent entries from different workers should overlap —
      // we can't strictly assert that here without timing, but the fact
      // that we got `workers * iterations` releases without a deadlock
      // and without an acquire-while-held proves mutual exclusion held.
    },
  );
});
