# dripline

Query cloud APIs using SQL.

## Commands

```bash
bun run dev -- --help     # Run CLI in dev mode
bun run build             # Compile TypeScript (for npm)
bun run build:bun         # Compile native binary
bun test                  # Run tests
bun run format            # Format with Biome
bun run lint              # Lint with Biome
bun run check             # Format + lint
```

## Architecture

- **CLI framework**: Commander
- **Output**: chalk via `src/utils/output.ts` (success/info/warn/error helpers)
- **Storage**: git-native JSON files in `.dripline/`, one file per record
- **IDs**: nanoid(8)

### Layers

- `src/main.ts` — CLI entry point, Commander setup
- `src/commands/` — one file per command
- `src/store.ts` — git-native storage (find root, read/write JSON records)
- `src/types.ts` — domain types
- `src/utils/` — output helpers, exit codes

## Key Patterns

### Output Triple

Every command supports three output modes:
- **Human** (default): chalk-formatted with icons
- **`--json`**: structured JSON for agents
- **`--quiet`**: minimal/no output

### Exit Codes

- `0` — success
- `1` — general error
- `2` — user error (bad input)
- `3` — not found (.dripline/ missing)

### Adding a New Command

1. Create `src/commands/<name>.ts`
2. Export an async function: `export async function myCmd(args, options)`
3. Import in `src/main.ts`
4. Add Commander subcommand in `src/main.ts`
