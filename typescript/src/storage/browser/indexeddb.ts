import type { IStorage } from "../interface";
import type { StorageLayout } from "../../storage";

// TypeScript types for IndexedDB
interface IDBDatabase {
  close(): void;
  createObjectStore(name: string): IDBObjectStore;
  objectStoreNames: {
    contains(name: string): boolean;
  };
  transaction(storeNames: string | string[], mode: IDBTransactionMode): IDBTransaction;
}

interface IDBTransaction extends EventTarget {
  objectStore(name: string): IDBObjectStore;
  oncomplete: ((this: IDBTransaction, ev: Event) => any) | null;
  onabort: ((this: IDBTransaction, ev: Event) => any) | null;
  onerror: ((this: IDBTransaction, ev: Event) => any) | null;
}

interface IDBObjectStore {
  get(key: string): IDBRequest;
  put(value: unknown, key?: string): IDBRequest;
  openKeyCursor(range?: IDBKeyRange): IDBRequest;
  delete(key: string): IDBRequest;
}

interface IDBRequest<T = any> extends EventTarget {
  result: T;
  error: DOMException | null;
  readyState: "pending" | "done";
  onsuccess: ((this: IDBRequest, ev: Event) => any) | null;
  onerror: ((this: IDBRequest, ev: Event) => any) | null;
}

interface IDBOpenDBRequest extends IDBRequest {
  onupgradeneeded: ((this: IDBOpenDBRequest, ev: IDBVersionChangeEvent) => any) | null;
  onblocked: ((this: IDBOpenDBRequest, ev: Event) => any) | null;
}

interface IDBVersionChangeEvent extends Event {
  oldVersion: number;
  newVersion: number | null;
}

interface IDBKeyRange {
  lowerBound(key: string): IDBKeyRange;
}

declare const IDBKeyRange: {
  lowerBound(key: string): IDBKeyRange;
};

interface IDBFactory {
  open(name: string, version?: number): IDBOpenDBRequest;
}

declare const indexedDB: IDBFactory;

type IDBTransactionMode = "readonly" | "readwrite";

// ---------------------------------------------------------------------------
// BrowserStorage - IndexedDB Implementation
// ---------------------------------------------------------------------------

/**
 * Browser storage implementation using IndexedDB.
 * Provides file system abstraction for browser environments.
 */
export class BrowserStorage implements IStorage {
  private static readonly DB_NAME = "polaris-storage";
  private static readonly DB_VERSION = 1;
  private static readonly STORE_NAME = "files";

  private db: IDBDatabase | null = null;
  private cache: Map<string, Uint8Array> = new Map();
  private readonly maxCacheSize = 50 * 1024 * 1024; // 50MB cache limit

  constructor() {
    // Constructor is synchronous, DB initialization happens on first access
  }

  private async ensureDb(): Promise<IDBDatabase> {
    if (this.db) {
      return this.db;
    }

    return new Promise((resolve, reject) => {
      const request = indexedDB.open(BrowserStorage.DB_NAME, BrowserStorage.DB_VERSION);

      request.onerror = () => {
        reject(new Error(`Failed to open IndexedDB: ${request.error?.message}`));
      };

      request.onsuccess = () => {
        this.db = request.result;
        resolve(this.db!);
      };

      request.onupgradeneeded = (event) => {
        const db = (event.target as IDBOpenDBRequest).result;
        if (!db.objectStoreNames.contains(BrowserStorage.STORE_NAME)) {
          db.createObjectStore(BrowserStorage.STORE_NAME);
        }
      };
    });
  }

  async exists(path: string): Promise<boolean> {
    try {
      const db = await this.ensureDb();
      return new Promise((resolve, reject) => {
        const transaction = db.transaction(BrowserStorage.STORE_NAME, "readonly");
        const store = transaction.objectStore(BrowserStorage.STORE_NAME);
        const request = store.get(path);

        request.onsuccess = () => resolve(request.result !== undefined);
        request.onerror = () => reject(new Error(`Exists check failed: ${request.error?.message}`));
      });
    } catch (error) {
      return false;
    }
  }

  async readFile(path: string): Promise<Uint8Array> {
    // Check cache first
    const cached = this.cache.get(path);
    if (cached) {
      return cached;
    }

    const db = await this.ensureDb();
    return new Promise((resolve, reject) => {
      const transaction = db.transaction(BrowserStorage.STORE_NAME, "readonly");
      const store = transaction.objectStore(BrowserStorage.STORE_NAME);
      const request = store.get(path);

      request.onsuccess = () => {
        const data = request.result;
        if (data === undefined) {
          reject(new Error(`File not found: ${path}`));
          return;
        }

        const uint8Array = new Uint8Array(data);
        // Cache the result
        this.cacheWithLimit(path, uint8Array);
        resolve(uint8Array);
      };

      request.onerror = () => reject(new Error(`Read failed: ${request.error?.message}`));
    });
  }

  async writeFile(path: string, data: Uint8Array): Promise<void> {
    const db = await this.ensureDb();
    return new Promise((resolve, reject) => {
      const transaction = db.transaction(BrowserStorage.STORE_NAME, "readwrite");
      const store = transaction.objectStore(BrowserStorage.STORE_NAME);
      const request = store.put(data, path);

      request.onsuccess = () => {
        // Update cache
        this.cacheWithLimit(path, data);
        resolve();
      };

      request.onerror = () => reject(new Error(`Write failed: ${request.error?.message}`));
    });
  }

  async mkdir(path: string): Promise<void> {
    // In IndexedDB, directories are implicit - files are stored with full paths
    // We just need to ensure the parent "directory" marker exists
    const db = await this.ensureDb();
    return new Promise((resolve, reject) => {
      const transaction = db.transaction(BrowserStorage.STORE_NAME, "readwrite");
      const store = transaction.objectStore(BrowserStorage.STORE_NAME);
      const request = store.put(new Uint8Array([0]), `${path}/.dir`);

      request.onsuccess = () => resolve();
      request.onerror = () => reject(new Error(`Mkdir failed: ${request.error?.message}`));
    });
  }

  async readdir(path: string): Promise<string[]> {
    const db = await this.ensureDb();
    return new Promise((resolve, reject) => {
      const transaction = db.transaction(BrowserStorage.STORE_NAME, "readonly");
      const store = transaction.objectStore(BrowserStorage.STORE_NAME);
      const request = store.openKeyCursor(IDBKeyRange.lowerBound(path));

      const results: string[] = [];

      request.onsuccess = () => {
        const cursor = request.result;
        if (cursor) {
          const key = cursor.key as string;
          // Check if this key is a direct child of the requested path
          if (key.startsWith(path)) {
            const relativePath = key.slice(path.length + 1);
            const firstSegment = relativePath.split("/")[0];
            if (firstSegment && !results.includes(firstSegment)) {
              results.push(firstSegment);
            }
          }
          cursor.continue();
        } else {
          resolve(results);
        }
      };

      request.onerror = () => reject(new Error(`Readdir failed: ${request.error?.message}`));
    });
  }

  join(...parts: string[]): string {
    // Simple path joining for browser virtual paths
    return parts
      .filter(Boolean)
      .map((part, i) => {
        // Remove leading/trailing slashes, except for the first part
        return i === 0 ? part.replace(/\/+$/, "") : part.replace(/^\/+|\/+$/g, "");
      })
      .join("/");
  }

  dirname(path: string): string {
    // Simple dirname implementation for browser virtual paths
    const segments = path.split("/");
    segments.pop(); // Remove the last segment
    return segments.join("/");
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

  // ---------------------------------------------------------------------
  // Cache management
  // ---------------------------------------------------------------------

  private cacheWithLimit(path: string, data: Uint8Array): void {
    // Add to cache
    this.cache.set(path, data);

    // Check cache size limit
    let totalSize = 0;
    for (const [, value] of this.cache) {
      totalSize += value.length;
    }

    // If over limit, remove oldest entries
    if (totalSize > this.maxCacheSize) {
      const entries = Array.from(this.cache.entries());
      entries.sort((a, b) => a[0].localeCompare(b[0])); // Simple sorting by path

      for (const [key, value] of entries) {
        if (totalSize <= this.maxCacheSize) break;
        this.cache.delete(key);
        totalSize -= value.length;
      }
    }
  }

  /**
   * Clear the file cache.
   */
  clearCache(): void {
    this.cache.clear();
  }
}
