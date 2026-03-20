# dripline

Query cloud APIs using SQL. One drip at a time.

## Install

```bash
npm install -g dripline
```

## Quick Start

```bash
# Initialize a project
dripline init

# Query GitHub repos (works without config for public APIs)
dripline query "SELECT name, stargazers_count, language
  FROM github_repos
  WHERE owner = 'torvalds'
  ORDER BY stargazers_count DESC
  LIMIT 10"

# Add auth for higher rate limits
echo '{"connections":[{"name":"gh","plugin":"github","config":{"token":"ghp_xxx"}}]}' > .dripline/config.json

# Start interactive shell
dripline
```

## Commands

| Command | Description |
|---------|-------------|
| `dripline` | Start interactive REPL |
| `dripline query <sql>` | Execute a SQL query |
| `dripline q <sql>` | Shorthand for query |
| `dripline repl` | Start interactive REPL |
| `dripline init` | Create `.dripline/` in current directory |
| `dripline onboard` | Add dripline instructions to CLAUDE.md |

### Query Options

| Flag | Description |
|------|-------------|
| `-o, --output <format>` | Output format: `table` (default), `json`, `csv`, `line` |
| `--json` | Output as JSON (same as `-o json`) |
| `-q, --quiet` | Suppress extra output |

### REPL Commands

| Command | Description |
|---------|-------------|
| `.tables` | List all available tables |
| `.inspect <table>` | Show table columns and key columns |
| `.connections` | List configured connections |
| `.output <format>` | Change output format |
| `.cache clear` | Clear the query cache |
| `.help` | Show help |
| `.quit` | Exit |

## Plugins

### GitHub

Tables: `github_repos`, `github_issues`, `github_pull_requests`, `github_stargazers`

```sql
-- Top repos by stars
SELECT name, stargazers_count, language
FROM github_repos WHERE owner = 'Michaelliv'
ORDER BY stargazers_count DESC;

-- Open issues
SELECT number, title, state
FROM github_issues WHERE owner = 'Michaelliv' AND repo = 'napkin';

-- Join repos with their issues
SELECT r.name, COUNT(i.id) as issue_count
FROM github_repos r
JOIN github_issues i ON r.name = i.repo
WHERE r.owner = 'Michaelliv' AND i.owner = 'Michaelliv'
GROUP BY r.name;
```

## Writing a Plugin

```typescript
import type { DriplinePluginAPI } from "dripline";
import { syncGetPaginated } from "dripline/plugins/utils/http";

export default function(dl: DriplinePluginAPI) {
  dl.setName("myplugin");
  dl.setVersion("0.1.0");
  dl.setConnectionSchema({
    api_key: { type: "string", required: true, description: "API key" },
  });

  dl.registerTable("my_table", {
    columns: [
      { name: "id", type: "number" },
      { name: "name", type: "string" },
    ],
    keyColumns: [
      { name: "org", required: "required" },
    ],
    *list(ctx) {
      const org = ctx.quals.find(q => q.column === "org")?.value;
      if (!org) return;
      const headers = { Authorization: `Bearer ${ctx.connection.config.api_key}` };
      const data = syncGetPaginated(`https://api.example.com/${org}/items`, headers);
      for (const item of data) {
        yield { id: item.id, name: item.name };
      }
    },
  });
}
```

### Installing Plugins

```bash
dripline plugin install npm:@dripline/aws       # from npm
dripline plugin install git:github.com/user/repo # from git
dripline plugin install ./my-plugin.ts           # local file
dripline plugin list                             # show all plugins
dripline plugin remove my-plugin                 # uninstall
```

Key concepts:
- **Plugin function** receives `DriplinePluginAPI` — register tables, set config schema
- **Columns** define what users can SELECT
- **Key columns** become required/optional WHERE parameters — pushed down to the API
- **`list`** is a sync generator that yields rows
- **`get`** (optional) handles single-item lookups
- HTTP calls use `execFileSync("curl", ...)` — sync is required by SQLite
- Plugins auto-discovered from `.dripline/plugins/` (project) and `~/.dripline/plugins/` (global)

## Configuration

`.dripline/config.json`:

```json
{
  "connections": [
    {
      "name": "my_github",
      "plugin": "github",
      "config": { "token": "ghp_your_token_here" }
    }
  ],
  "cache": {
    "enabled": true,
    "ttl": 300,
    "maxSize": 1000
  },
  "rateLimits": {
    "github": { "maxPerSecond": 5 }
  }
}
```

## For Agents

Every command supports `--json` for structured output:

```bash
dripline query "SELECT name FROM github_repos WHERE owner = 'x'" --json
```

Run `dripline onboard` to add usage instructions to your agent's context file.

## License

MIT
