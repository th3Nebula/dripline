import { existsSync, readdirSync, readFileSync, statSync } from "node:fs";
import { join, resolve } from "node:path";
import { homedir } from "node:os";
import { pathToFileURL } from "node:url";
import type { PluginDef } from "./types.js";
import { registry } from "./registry.js";
import { resolvePluginExport } from "./api.js";
import { findConfigDir } from "../config/loader.js";

export async function loadPluginFromPath(path: string): Promise<PluginDef> {
  const absPath = resolve(path);
  const mod = await import(pathToFileURL(absPath).href);
  const pluginId = absPath.replace(/.*\//, "").replace(/\.(ts|js)$/, "");
  return resolvePluginExport(mod.default, pluginId);
}

export async function loadBuiltinPlugins(): Promise<void> {
  const pluginsDir = new URL("../plugins", import.meta.url);
  const dirPath = pluginsDir.pathname;
  if (!existsSync(dirPath)) return;

  const files = readdirSync(dirPath).filter(
    (f) =>
      (f.endsWith(".ts") || f.endsWith(".js")) &&
      !f.endsWith(".test.ts") &&
      !f.startsWith("_"),
  );

  for (const file of files) {
    const mod = await import(pathToFileURL(join(dirPath, file)).href);
    if (mod.default) {
      try {
        const pluginId = file.replace(/\.(ts|js)$/, "");
        const plugin = resolvePluginExport(mod.default, pluginId);
        registry.register(plugin);
      } catch {
      }
    }
  }
}

async function loadFromDirectory(dir: string): Promise<PluginDef[]> {
  const plugins: PluginDef[] = [];
  if (!existsSync(dir)) return plugins;

  const entries = readdirSync(dir);

  for (const entry of entries) {
    const fullPath = join(dir, entry);
    const stat = statSync(fullPath);

    if (stat.isFile() && (entry.endsWith(".ts") || entry.endsWith(".js")) && !entry.endsWith(".test.ts")) {
      try {
        plugins.push(await loadPluginFromPath(fullPath));
      } catch {
      }
    } else if (stat.isDirectory()) {
      const indexTs = join(fullPath, "index.ts");
      const indexJs = join(fullPath, "index.js");
      if (existsSync(indexTs)) {
        try { plugins.push(await loadPluginFromPath(indexTs)); } catch {}
      } else if (existsSync(indexJs)) {
        try { plugins.push(await loadPluginFromPath(indexJs)); } catch {}
      }

      const pkgJson = join(fullPath, "package.json");
      if (existsSync(pkgJson)) {
        try {
          const pkg = JSON.parse(readFileSync(pkgJson, "utf-8"));
          const pluginPaths: string[] = pkg.dripline?.plugins ?? [];
          for (const p of pluginPaths) {
            try { plugins.push(await loadPluginFromPath(join(fullPath, p))); } catch {}
          }
        } catch {}
      }
    }
  }

  return plugins;
}

export async function loadPluginsFromConfig(configDir: string): Promise<void> {
  const pluginsFile = join(configDir, "plugins.json");
  if (!existsSync(pluginsFile)) return;

  try {
    const data = JSON.parse(readFileSync(pluginsFile, "utf-8"));
    const entries: Array<{ path: string }> = data.plugins ?? data;

    for (const entry of entries) {
      const p = typeof entry === "string" ? entry : entry.path;
      try {
        const plugin = await loadPluginFromPath(p);
        registry.register(plugin);
      } catch {
      }
    }
  } catch {
  }
}

export async function loadAllPlugins(): Promise<void> {
  const loaded = new Set<string>();

  function registerIfNew(plugin: PluginDef) {
    if (!loaded.has(plugin.name)) {
      registry.register(plugin);
      loaded.add(plugin.name);
    }
  }

  await loadBuiltinPlugins();
  for (const p of registry.listPlugins()) {
    loaded.add(p.name);
  }

  const configDir = findConfigDir();
  if (configDir) {
    const projectPluginsDir = join(configDir, "plugins");
    const projectPlugins = await loadFromDirectory(projectPluginsDir);
    for (const p of projectPlugins) registerIfNew(p);

    await loadPluginsFromConfig(configDir);
    for (const p of registry.listPlugins()) loaded.add(p.name);
  }

  const globalDir = join(homedir(), ".dripline", "plugins");
  const globalPlugins = await loadFromDirectory(globalDir);
  for (const p of globalPlugins) registerIfNew(p);
}
