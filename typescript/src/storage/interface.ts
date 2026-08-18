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
