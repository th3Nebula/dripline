# dripline 💧

Query mode for agents.

Turn any API, CLI, or cloud service into a SQL table. Install a plugin, write a query, get rows. It's just SQL, DuckDB under the hood.

```bash
npm install -g dripline
```

## Quick Start

```bash
dripline init
dripline plugin install git:github.com/Michaelliv/dripline#plugins/docker
dripline query "SELECT name, image, state FROM docker_containers"
```

## Plugins

All plugins install via `dripline plugin install git:github.com/Michaelliv/dripline#plugins/<name>`.

| Plugin | Tables | Source |
|--------|--------|--------|
| **github** | repos, issues, pull_requests, stargazers | GitHub API |
| **docker** | containers, images, volumes, networks | Docker CLI |
| **brew** | formulae, casks, outdated, services | Homebrew |
| **ps** | processes, ports | ps, lsof |
| **git** | commits, branches, tags, remotes, status | Git CLI |
| **system-profiler** | software, hardware, network_interfaces, storage, displays | macOS |
| **pi** | sessions, messages, tool_calls, costs, prompt, generate | pi coding agent |
| **kubectl** | pods, services, deployments, nodes, namespaces, configmaps, secrets, ingresses | Kubernetes |
| **npm** | packages, outdated, global, scripts | npm CLI |
| **spotlight** | search, apps, recent | macOS Spotlight |
| **skills-sh** | search | skills.sh registry |
| **cloudflare** | workers, zones, dns_records, pages_projects, pages_deployments, d1_databases, kv_namespaces, r2_buckets, queues, dns_lookup, domain_check | Cloudflare API |
| **vercel** | projects, deployments, domains, env_vars | Vercel API |

## Examples

```sql
-- github: repos by stars
SELECT name, stargazers_count, language
FROM github_repos WHERE owner = 'torvalds'
ORDER BY stargazers_count DESC LIMIT 5;

-- k8s: pods with restarts
SELECT name, namespace, status, restarts
FROM k8s_pods WHERE restarts > 0
ORDER BY restarts DESC;

-- cloudflare: is your domain available?
SELECT domain, available FROM cf_domain_check
WHERE name_prefix = 'myproject' AND tlds = 'com,dev,sh,io,ai';

-- pi: how much have you spent per model?
SELECT model, COUNT(*) as sessions, ROUND(SUM(total_cost), 2) as cost
FROM pi_sessions GROUP BY model ORDER BY cost DESC;

-- skills.sh: top react skills
SELECT name, source, installs FROM skills_search
WHERE query = 'react' ORDER BY installs DESC LIMIT 5;

-- vercel: recent deployments
SELECT name, state, git_commit_message FROM vercel_deployments
WHERE project_name = 'my-blog' LIMIT 5;

-- join API data with a local CSV
SELECT r.name, r.stargazers_count, s.revenue
FROM github_repos r
JOIN read_csv_auto('./revenue.csv') s ON r.name = s.repo
WHERE r.owner = 'torvalds';

-- generate structured data with AI, query it with SQL
SELECT data->>'name' as name, CAST(data->>'age' AS INT) as age
FROM pi_generate
WHERE prompt = 'generate 5 fictional engineers with name, age, city';
```

## Writing a Plugin

Plugins are sync generators. Wrap an API with `syncGet`/`syncGetPaginated` or a local CLI with `syncExec`.

```typescript
import type { DriplinePluginAPI } from "dripline";
import { syncGetPaginated } from "dripline";

export default function(dl: DriplinePluginAPI) {
  dl.setName("orders");
  dl.setVersion("1.0.0");

  // Declare connection config — env vars override config.json values
  dl.setConnectionSchema({
    api_key: { type: "string", required: true, env: "ORDERS_API_KEY" },
  });

  dl.registerTable("orders", {
    columns: [
      { name: "id", type: "number" },
      { name: "customer", type: "string" },
      { name: "total", type: "number" },
      { name: "created_date", type: "datetime" },
    ],

    // Key columns are extracted from WHERE clauses and passed to list() as quals.
    // This is how plugins filter at the source instead of fetching everything.
    //
    //   required  — query MUST include this in WHERE or list() won't be called
    //   optional  — passed to list() if present, ignored if not
    //   any_of    — at least one column in the any_of group must be present
    //
    // Rule of thumb: if the underlying API/CLI accepts a filter parameter,
    // declare it as a key column so the engine can push it down.
    keyColumns: [
      { name: "org_id", required: "required" },
      { name: "created_date", required: "optional" },
    ],

    *list(ctx) {
      const orgId = ctx.quals.find(q => q.column === "org_id")?.value;
      const date = ctx.quals.find(q => q.column === "created_date")?.value;

      // Build API URL with filters pushed down from SQL
      let url = `https://api.example.com/orgs/${orgId}/orders`;
      if (date) url += `?created=${date}`;

      const headers = { Authorization: `Bearer ${ctx.connection.config.api_key}` };
      const data = syncGetPaginated(url, headers);
      for (const d of data) {
        yield { id: d.id, customer: d.customer, total: d.total, created_date: d.created_at };
      }
    },
  });
}
```

**How key columns work:** when you query `SELECT * FROM orders WHERE org_id = 'acme' AND created_date = '2026-03-01'`, the engine extracts both quals and passes them to `list()` via `ctx.quals`. The plugin uses them to filter at the source — fetching only matching rows instead of everything. Columns NOT in `keyColumns` are filtered by DuckDB after all rows are materialized. For large tables, declaring filter-friendly columns as optional key columns is the difference between milliseconds and timeouts.

`syncExec` supports parsers: `json`, `jsonlines`, `csv`, `tsv`, `lines`, `kv`, `raw`.

See [plugins/](plugins/) for full examples.

## For Agents

Every command supports `--json`. Use `dripline tables --json` for full schemas.

A [pi](https://github.com/badlogic/pi-mono) package is included that injects available tables into the agent context on session start:

```bash
pi install git:github.com/Michaelliv/dripline
```

## CLI Reference

```bash
dripline                              # interactive REPL
dripline query "<sql>"                # execute a query (alias: q)
dripline query "<sql>" -o json        # output as json, csv, or line
dripline tables                       # list all tables and columns
dripline tables --json                # full schema as json
dripline plugin install <source>      # install from git/npm/local path
dripline plugin list                  # list installed plugins
dripline plugin remove <name>         # remove a plugin
dripline connection add <n> -p <plugin> -s key=val  # add connection
dripline connection list              # list connections
dripline init                         # create .dripline/ directory
```

REPL commands: `.tables`, `.inspect <table>`, `.connections`, `.output <format>`, `.help`, `.quit`.

## SDK

```typescript
import { Dripline } from "dripline";
import githubPlugin from "dripline-plugin-github";

const dl = await Dripline.create({
  plugins: [githubPlugin],
  connections: [{ name: "gh", plugin: "github", config: { token: "ghp_xxx" } }],
});

const repos = await dl.query("SELECT name FROM github_repos WHERE owner = 'torvalds' LIMIT 5");
await dl.close();
```

## Configuration

`.dripline/config.json`:

```json
{
  "connections": [{ "name": "gh", "plugin": "github", "config": { "token": "ghp_xxx" } }],
  "cache": { "enabled": true, "ttl": 300, "maxSize": 1000 },
  "rateLimits": { "github": { "maxPerSecond": 5 } }
}
```

Env vars override config. Plugins declare env var names in their connection schema (e.g. `GITHUB_TOKEN`).

## Development

```bash
npm install
npm run dev -- query "SELECT 1"
npm test
npm run check
```

## License

MIT
