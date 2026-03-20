import { initStore } from "../store.js";
import type { OutputOptions } from "../utils/output.js";
import { cmd, hint, output, success } from "../utils/output.js";

const COLLECTIONS = ["plugins", "connections"];

export async function init(
  _args: string[],
  options: OutputOptions,
): Promise<void> {
  const root = initStore(COLLECTIONS);

  output(options, {
    json: () => ({ success: true, path: root }),
    human: () => {
      success(`Initialized .dripline/ in ${process.cwd()}`);
      hint("Next: run the onboard command to teach your agent about dripline");
      console.log(`  ${cmd("dripline onboard")}`);
    },
  });
}
