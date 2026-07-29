import type { IStorage } from "../storage/interface";

export interface PolarisRuntime {
  resolveApiKey(explicit?: string): string | undefined;
  resolveRoot(explicit?: string): string;
  createStorage(root: string): Promise<IStorage>;
}
