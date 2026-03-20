import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import type { OutputOptions } from "../utils/output.js";
import { output, success } from "../utils/output.js";

const INSTRUCTIONS = `
<dripline>
Use \`dripline\` to query cloud APIs using SQL. Data is stored in \`.dripline/\` as JSON files, tracked by git.

<commands>
- \`dripline init\` - Initialize a dripline project
- \`dripline query <sql>\` - Run a SQL query against cloud APIs
- \`dripline plugin install <name>\` - Install a plugin
- \`dripline plugin list\` - List installed plugins
- \`dripline connection add <plugin> <name>\` - Add a connection
</commands>

<rules>
- ALWAYS use \`--json\` flag to get structured output for parsing
- All data lives in \`.dripline/\` and should be committed to git
</rules>
</dripline>
`.trim();

const MARKER = "<dripline>";

export async function onboard(
  _args: string[],
  options: OutputOptions,
): Promise<void> {
  const cwd = process.cwd();
  const claudeMd = join(cwd, "CLAUDE.md");
  const agentsMd = join(cwd, "AGENTS.md");

  let targetFile: string;
  if (existsSync(claudeMd)) {
    targetFile = claudeMd;
  } else if (existsSync(agentsMd)) {
    targetFile = agentsMd;
  } else {
    targetFile = claudeMd;
  }

  let existingContent = "";
  if (existsSync(targetFile)) {
    existingContent = readFileSync(targetFile, "utf-8");
  }

  if (existingContent.includes(MARKER)) {
    output(options, {
      json: () => ({
        success: true,
        file: targetFile,
        message: "already_onboarded",
      }),
      human: () => success(`Already onboarded (${targetFile})`),
    });
    return;
  }

  if (existingContent) {
    writeFileSync(
      targetFile,
      `${existingContent.trimEnd()}\n\n${INSTRUCTIONS}\n`,
    );
  } else {
    writeFileSync(targetFile, `${INSTRUCTIONS}\n`);
  }

  output(options, {
    json: () => ({ success: true, file: targetFile }),
    human: () => success(`Added dripline instructions to ${targetFile}`),
  });
}
