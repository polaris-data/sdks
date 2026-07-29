import { mkdir, stat, readdir, readFile as fsReadFile, writeFile as fsWriteFile } from "node:fs/promises";
import { join, dirname } from "node:path";

import type { IStorage } from "../interface";
import type { StorageLayout } from "../../storage";

// ---------------------------------------------------------------------------
// NodeStorage - Node.js Implementation
// ---------------------------------------------------------------------------

/**
 * Node.js storage implementation using fs/promises.
 * Wraps existing Node.js file system operations without breaking changes.
 */
export class NodeStorage implements IStorage {
  private root: string;

  constructor(root?: string) {
    // Use provided root or current working directory
    this.root = root ?? process.cwd();
  }

  async exists(path: string): Promise<boolean> {
    try {
      await stat(path);
      return true;
    } catch {
      return false;
    }
  }

  async readFile(path: string): Promise<Uint8Array> {
    const buffer = await fsReadFile(path);
    return new Uint8Array(buffer.buffer, buffer.byteOffset, buffer.byteLength);
  }

  async writeFile(path: string, data: Uint8Array): Promise<void> {
    await fsWriteFile(path, Buffer.from(data));
  }

  async mkdir(path: string): Promise<void> {
    await mkdir(path, { recursive: true });
  }

  async readdir(path: string): Promise<string[]> {
    const entries = await readdir(path, { withFileTypes: true });
    return entries.map((entry) => entry.name);
  }

  join(...parts: string[]): string {
    return join(...parts);
  }

  dirname(path: string): string {
    return dirname(path);
  }

  async ensureLayout(root: string): Promise<StorageLayout> {
    const dataDir = this.join(root, "data");
    const tmpDir = this.join(root, "tmp");
    const cacheDir = this.join(root, "cache");

    await Promise.all([
      this.mkdir(dataDir),
      this.mkdir(tmpDir),
      this.mkdir(cacheDir),
    ]);

    return { root, dataDir, tmpDir, cacheDir };
  }
}