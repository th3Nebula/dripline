1/ if there's code mode, there's also query mode.

introducing dripline, turns any api, cli, or cloud service into a sql table. install a plugin, agent writes a query, get rows back. joins, aggregations, window functions and duckdb handles the rest.

ultra easy to extend, in the repo - 13 plugins, 61 tables. github, docker, pi, brew, kubectl, cloudflare, vercel, and more.

obligatory @mariozechner pi extension that injects all available tables into your agent's context so it knows what it can query:

pi install npm:dripline

💧🧵

[snippet 1]
dripline plugin install git:github.com/Michaelliv/dripline#plugins/docker

dripline query "SELECT name, image, state FROM docker_containers"

┌───────────────┬──────────────────┬─────────┐
│ name          │ image            │ state   │
├───────────────┼──────────────────┼─────────┤
│ my-api        │ node:22-alpine   │ running │
│ postgres      │ postgres:16      │ running │
│ redis-cache   │ redis:7          │ running │
└───────────────┴──────────────────┴─────────┘

---

2/ how it works: plugins are sync generators. they call an api or shell out to a cli, yield rows. dripline materializes them into duckdb temp tables. then you write normal sql.

two patterns — wrap an api with syncGet, or wrap a local cli with syncExec:

[snippet 2]
import { syncExec } from "dripline";

export default function(dl) {
  dl.setName("brew");

  dl.registerTable("brew_formulae", {
    columns: [
      { name: "name", type: "string" },
      { name: "version", type: "string" },
    ],
    *list() {
      const { rows: [data] } = syncExec("brew",
        ["info", "--json=v2", "--installed"],
        { parser: "json" }
      );
      for (const f of data.formulae) {
        yield { name: f.name, version: f.installed[0].version };
      }
    },
  });
}

---

3/ the pi plugin is my favorite. it reads all your @mariozechner pi session files and turns them into queryable tables.

how much have i spent per model? one query.

[snippet 3]
SELECT model, COUNT(*) as sessions,
       ROUND(SUM(total_cost), 2) as cost
FROM pi_sessions
GROUP BY model ORDER BY cost DESC;

┌─────────────────────────┬──────────┬─────────┐
│ model                   │ sessions │ cost    │
├─────────────────────────┼──────────┼─────────┤
│ claude-opus-4           │ 82       │ 1037.56 │
│ gpt-5                   │ 7        │ 994.87  │
│ claude-opus-4-5         │ 32       │ 173.75  │
│ gpt-5-codex             │ 10       │ 124.15  │
│ claude-sonnet-4         │ 3        │ 5.16    │
└─────────────────────────┴──────────┴─────────┘

---

4/ what tools does pi actually use the most?

[snippet 4]
SELECT tool_name, COUNT(*) as calls
FROM pi_tool_calls
GROUP BY tool_name
ORDER BY calls DESC;

┌─────────────────┬───────┐
│ tool_name       │ calls │
├─────────────────┼───────┤
│ bash            │ 18283 │
│ edit            │ 6013  │
│ read            │ 5312  │
│ write           │ 1692  │
│ AskUserQuestion │ 108   │
│ show_widget     │ 66    │
└─────────────────┴───────┘

bash wins by a landslide. obviously.

---

5/ total damage across 194 sessions:

[snippet 5]
SELECT ROUND(SUM(total_cost), 2) as total_spend,
       COUNT(*) as sessions
FROM pi_sessions;

┌─────────────┬──────────┐
│ total_spend │ sessions │
├─────────────┼──────────┤
│ 2758.03     │ 194      │
└─────────────┴──────────┘

daily breakdown:

SELECT SUBSTR(started_at, 1, 10) as day,
       COUNT(*) as sessions,
       ROUND(SUM(total_cost), 2) as cost
FROM pi_sessions
GROUP BY day ORDER BY day DESC LIMIT 7;

┌────────────┬──────────┬────────┐
│ day        │ sessions │ cost   │
├────────────┼──────────┼────────┤
│ 2026-03-20 │ 2        │ 169.60 │
│ 2026-03-18 │ 4        │ 10.45  │
│ 2026-03-17 │ 4        │ 37.13  │
│ 2026-03-16 │ 11       │ 12.75  │
│ 2026-03-15 │ 2        │ 179.17 │
│ 2026-03-14 │ 5        │ 863.13 │
│ 2026-03-13 │ 14       │ 101.57 │
└────────────┴──────────┴────────┘

yes, that's $863 in one day. no regrets.

---

6/ it gets weirder. pi_prompt lets you send prompts to pi and get responses back as sql rows. pi_generate generates structured data with ai and returns it as queryable json.

[snippet 6]
SELECT data->>'name' as name,
       CAST(data->>'age' AS INT) as age,
       data->>'city' as city
FROM pi_generate
WHERE prompt = 'generate 5 fictional engineers
  with name, age, city';

┌────────────────┬─────┬──────────┐
│ name           │ age │ city     │
├────────────────┼─────┼──────────┤
│ Talia Vasquez  │ 29  │ Portland │
│ Jun Nakamura   │ 34  │ Tokyo    │
│ Elise Fournier │ 41  │ Lyon     │
│ Kofi Mensah    │ 26  │ Accra    │
│ Darya Sokolova │ 37  │ Berlin   │
└────────────────┴─────┴──────────┘

---

7/ the @CloudflareDev plugin uses their 1.1.1.1 dns api for domain availability checks. no auth needed.

is your project name taken?

[snippet 7]
SELECT domain, available
FROM cf_domain_check
WHERE name_prefix = 'dripline'
  AND tlds = 'com,dev,sh,io,ai';

┌──────────────┬───────────┐
│ domain       │ available │
├──────────────┼───────────┤
│ dripline.com │ false     │
│ dripline.dev │ true      │
│ dripline.sh  │ true      │
│ dripline.io  │ false     │
│ dripline.ai  │ false     │
└──────────────┴───────────┘

dripline.dev is available btw 👀

---

8/ or query your actual cloudflare infra. workers, zones, pages, d1, kv, r2 — all as tables.

[snippet 8]
SELECT name, status, plan FROM cf_zones;

┌────────────────┬────────┬──────────────┐
│ name           │ status │ plan         │
├────────────────┼────────┼──────────────┤
│ myapp.dev      │ active │ Free Website │
│ coolproject.sh │ active │ Free Website │
└────────────────┴────────┴──────────────┘

---

9/ shoutout @nichochar — the skills.sh plugin queries the skills registry with sql. what are the most popular react skills?

[snippet 9]
SELECT name, source, installs
FROM skills_search
WHERE query = 'react'
ORDER BY installs DESC LIMIT 5;

┌─────────────────────────────┬──────────────────────────┬──────────┐
│ name                        │ source                   │ installs │
├─────────────────────────────┼──────────────────────────┼──────────┤
│ vercel-react-best-practices │ vercel-labs/agent-skills │ 231411   │
│ vercel-react-native-skills  │ vercel-labs/agent-skills │ 65689    │
│ react:components            │ google-labs-code/stitch  │ 18740    │
│ react-native-best-practices │ callstackincubator       │ 7909     │
│ react-doctor                │ millionco/react-doctor   │ 6193     │
└─────────────────────────────┴──────────────────────────┴──────────┘

---

10/ vercel plugin auto-detects your auth from vercel login. deployment history as sql.

[snippet 10]
SELECT name, state, target, git_commit_message
FROM vercel_deployments
WHERE project_name = 'my-blog'
LIMIT 3;

┌─────────┬───────┬────────────┬─────────────────────────────┐
│ name    │ state │ target     │ git_commit_message          │
├─────────┼───────┼────────────┼─────────────────────────────┤
│ my-blog │ READY │ production │ feat: add dark mode support │
│ my-blog │ READY │ production │ fix: mobile nav overflow    │
│ my-blog │ READY │ preview    │ wip: auth flow              │
└─────────┴───────┴────────────┴─────────────────────────────┘

---

11/ k8s too. pods, services, deployments, nodes, configmaps, secrets, ingresses — all queryable.

[snippet 11]
SELECT name, namespace, status, ready, restarts
FROM k8s_pods WHERE restarts > 0
ORDER BY restarts DESC;

┌──────────────────┬─────────────┬─────────┬───────┬──────────┐
│ name             │ namespace   │ status  │ ready │ restarts │
├──────────────────┼─────────────┼─────────┼───────┼──────────┤
│ kube-scheduler   │ kube-system │ Running │ 1/1   │ 9        │
│ kube-controller  │ kube-system │ Running │ 1/1   │ 9        │
│ api-gateway      │ default     │ Running │ 1/1   │ 3        │
└──────────────────┴─────────────┴─────────┴───────┴──────────┘

---

12/ 13 plugins. 57 tables. all installable from one repo:

dripline plugin install git:github.com/Michaelliv/dripline#plugins/<name>

github · docker · brew · ps · git · system-profiler · pi · kubectl · npm · spotlight · skills-sh · cloudflare · vercel

writing a new plugin is ~30 lines. wrap any cli or api, yield rows, done.

github.com/Michaelliv/dripline 💧
