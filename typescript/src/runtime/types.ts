import type { IStorage } from "../storage/interface";

export interface WebSocketEventMap {
  open: unknown;
  message: { data: unknown };
  close: { code?: number; reason?: string };
  error: unknown;
}

export interface WebSocketLike {
  readonly readyState: number;
  send(data: string): void;
  close(code?: number, reason?: string): void;
  addEventListener<K extends keyof WebSocketEventMap>(
    type: K,
    listener: (event: WebSocketEventMap[K]) => void,
  ): void;
}

export interface PolarisRuntime {
  resolveApiKey(explicit?: string): string | undefined;
  resolveRoot(explicit?: string): string;
  createStorage(root: string): Promise<IStorage>;
  createWebSocket(url: string): WebSocketLike;
}
