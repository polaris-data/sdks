import type { IStorage } from "./storage/interface";

// ---------------------------------------------------------------------------
// StorageLayout
// ---------------------------------------------------------------------------

export interface StorageLayout {
  readonly root: string;
  readonly dataDir: string;
  readonly tmpDir: string;
  readonly cacheDir: string;
}

// ---------------------------------------------------------------------------
// Layout bootstrapping (using storage interface)
// ---------------------------------------------------------------------------

/** Ensure the standard sub-directory tree exists and return the layout. */
export async function ensureLayout(storage: IStorage, root: string): Promise<StorageLayout> {
  return await storage.ensureLayout(root);
}

// ---------------------------------------------------------------------------
// Path helpers
// ---------------------------------------------------------------------------

const SNAPSHOT_EXT = ".jsonl.zst";

export interface SnapshotKeyParts {
  readonly tier: string;
  readonly source: string;
  readonly market: string;
  readonly date: string;
  readonly timeSuffix?: string;
  readonly hour?: number;
  readonly opaqueKey: string;
  readonly filename: string;
}

/** Parse an opaque Polaris snapshot key into its storage path components. */
export function parseSnapshotKey(key: string): SnapshotKeyParts {
  const opaqueKey = key.endsWith(SNAPSHOT_EXT)
    ? key.slice(0, -SNAPSHOT_EXT.length)
    : key;
  const parts = opaqueKey.split("-").filter(Boolean);
  if (parts.length < 6) {
    throw new Error(`Invalid snapshot key: ${key}`);
  }

  const tier = parts[0];
  const source = parts[1];
  if (!tier || !source) throw new Error(`Invalid snapshot key: ${key}`);

  for (let dateIndex = parts.length - 3; dateIndex >= 3; dateIndex -= 1) {
    const date = parts.slice(dateIndex, dateIndex + 3).join("-");
    if (!isStrictIsoDate(date)) continue;

    const market = parts.slice(2, dateIndex).join("-");
    if (!market) continue;

    const suffixParts = parts.slice(dateIndex + 3);
    const timeSuffix =
      suffixParts.length === 1 && /^\d+$/.test(suffixParts[0])
        ? suffixParts[0]
        : undefined;
    const parsedTime = timeSuffix ? parseSnapshotTimeSuffix(timeSuffix) : undefined;

    return {
      tier,
      source,
      market,
      date,
      timeSuffix,
      hour: parsedTime?.hour,
      opaqueKey,
      filename: `${opaqueKey}${SNAPSHOT_EXT}`,
    };
  }

  throw new Error(`Invalid snapshot key: ${key}`);
}

export function inferSnapshotStartMs(
  key: string,
  dateText?: string,
): number | undefined {
  const parsed = parseSnapshotKey(key);
  const resolvedDate = dateText ?? parsed.date;
  if (!parsed.timeSuffix) return undefined;

  const time = parseSnapshotTimeSuffix(parsed.timeSuffix);
  if (!time || !isStrictIsoDate(resolvedDate)) return undefined;

  return Date.UTC(
    Number.parseInt(resolvedDate.slice(0, 4), 10),
    Number.parseInt(resolvedDate.slice(5, 7), 10) - 1,
    Number.parseInt(resolvedDate.slice(8, 10), 10),
    time.hour,
    time.minute,
    time.second,
  );
}

export function inferSnapshotEndMs(
  key: string,
  dateText?: string,
): number | undefined {
  const parsed = parseSnapshotKey(key);
  if (!parsed.timeSuffix) return undefined;

  const startMs = inferSnapshotStartMs(key, dateText ?? parsed.date);
  if (startMs === undefined) return undefined;

  // Legacy hour-only suffixes imply one-hour standardized coverage.
  if (parsed.timeSuffix.length === 1 || parsed.timeSuffix.length === 2) {
    return startMs + 3_600_000;
  }
  return undefined;
}

/** Path for a downloaded snapshot file in the Rust-style `data/` tree. */
export function dataFilePath(dataDir: string, key: string): string {
  const parsed = parseSnapshotKey(key);
  // Use storage interface for path joining when available
  // For now, use simple path construction (will be storage-aware in context)
  return `${dataDir}/${parsed.tier}/${parsed.source}/${parsed.market}/${parsed.date}/${parsed.filename}`;
}

/** Return `true` when the file at `path` exists (using storage interface). */
export async function fileExists(storage: IStorage, path: string): Promise<boolean> {
  return await storage.exists(path);
}

function parseSnapshotTimeSuffix(
  suffix: string,
): { hour: number; minute: number; second: number } | undefined {
  if (!/^\d+$/.test(suffix)) return undefined;

  let hour: number;
  let minute: number;
  let second: number;

  if (suffix.length === 1 || suffix.length === 2) {
    hour = Number.parseInt(suffix, 10);
    minute = 0;
    second = 0;
  } else if (suffix.length === 4) {
    hour = Number.parseInt(suffix.slice(0, 2), 10);
    minute = Number.parseInt(suffix.slice(2, 4), 10);
    second = 0;
  } else if (suffix.length === 6) {
    hour = Number.parseInt(suffix.slice(0, 2), 10);
    minute = Number.parseInt(suffix.slice(2, 4), 10);
    second = Number.parseInt(suffix.slice(4, 6), 10);
  } else {
    return undefined;
  }

  if (
    hour < 0 || hour > 23 ||
    minute < 0 || minute > 59 ||
    second < 0 || second > 59
  ) {
    return undefined;
  }

  return { hour, minute, second };
}

function isStrictIsoDate(value: string): boolean {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) return false;
  const parsed = new Date(`${value}T00:00:00Z`);
  return !Number.isNaN(parsed.getTime()) && parsed.toISOString().startsWith(value);
}
