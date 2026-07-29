import { join } from "node:path";
import { homedir, platform } from "node:os";

import { NodeStorage } from "../storage/node/index";
import type { PolarisRuntime } from "./types";

function defaultRoot(): string {
  switch (platform()) {
    case "darwin":
      return join(
        homedir(),
        "Library",
        "Application Support",
        "polaris",
      );
    case "win32":
      return join(
        process.env.APPDATA || join(homedir(), "AppData", "Roaming"),
        "polaris",
      );
    default:
      return process.env.XDG_DATA_HOME
        ? join(process.env.XDG_DATA_HOME, "polaris")
        : join(homedir(), ".local", "share", "polaris");
  }
}

export const nodeRuntime: PolarisRuntime = {
  resolveApiKey(explicit) {
    return explicit ?? process.env.POLARIS_API_KEY;
  },

  resolveRoot(explicit) {
    if (explicit) return explicit;
    if (process.env.POLARIS_ROOT) return process.env.POLARIS_ROOT;
    if (process.env.POLARIS_DATASET_DOWNLOAD_DIR) {
      return process.env.POLARIS_DATASET_DOWNLOAD_DIR;
    }
    return defaultRoot();
  },

  async createStorage(root) {
    return new NodeStorage(root);
  },
};
