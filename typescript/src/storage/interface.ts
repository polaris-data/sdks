import type { StorageLayout } from "../storage";

// ---------------------------------------------------------------------------
// Storage Abstraction Interface
// ---------------------------------------------------------------------------

/**
 * Universal storage interface abstracting file system operations.
 * Implemented differently for Node.js (fs/promises) and browser (IndexedDB/OPFS).
 */
export interface IStorage {
  /**
   * Check if a file exists at the given path.
   */
  exists(path: string): Promise<boolean>;

  /**
   * Read file contents as Uint8Array (binary data).
   */
  readFile(path: string): Promise<Uint8Array>;

  /**
   * Write binary data to a file.
   */
  writeFile(path: string, data: Uint8Array): Promise<void>;

  /**
   * Create a directory recursively (like mkdir -p).
   */
  mkdir(path: string): Promise<void>;

  /**
   * List contents of a directory.
   * Returns array of file/directory names.
   */
  readdir(path: string): Promise<string[]>;

  /**
   * Join path segments into a single path.
   * Platform-aware (Node.js uses OS-specific separator, browser uses virtual paths).
   */
  join(...parts: string[]): string;

  /**
   * Get the directory name of a path.
   */
  dirname(path: string): string;

  /**
   * Ensure the standard sub-directory tree exists and return the layout.
   * Creates data/, tmp/, and cache/ directories under the root.
   */
  ensureLayout(root: string): Promise<StorageLayout>;
}

// ---------------------------------------------------------------------------
// Shared Utilities
// ---------------------------------------------------------------------------

/**
 * Read a text file and return its contents as a string.
 * Convenience wrapper around readFile() + TextDecoder.
 */
export async function readTextFile(storage: IStorage, path: string): Promise<string> {
  const data = await storage.readFile(path);
  return new TextDecoder().decode(data);
}

/**
 * Write a text file.
 * Convenience wrapper around writeFile() + TextEncoder.
 */
export async function writeTextFile(storage: IStorage, path: string, text: string): Promise<void> {
  const data = new TextEncoder().encode(text);
  await storage.writeFile(path, data);
}

/**
 * Check if a directory exists and create it if it doesn't.
 * Convenience wrapper around exists() + mkdir().
 */
export async function ensureDir(storage: IStorage, path: string): Promise<void> {
  const exists = await storage.exists(path);
  if (!exists) {
    await storage.mkdir(path);
  }
}
