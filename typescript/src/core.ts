import { isInteger, parse } from "lossless-json";

import {
  AccessDeniedError,
  NotFoundError,
  PolarisError,
  RateLimitedError,
  StreamConnectionError,
  StreamDecodeError,
  StreamProtocolError,
  UnauthorizedError,
} from "./errors";
import type { TableDataMessage } from "./generated";
import wasmImage from "./wasm-image";
import { makeCore } from "./wasm-fallback.mjs";

interface Exports {
  readonly memory: { readonly buffer: ArrayBuffer };
  polaris_v1_alloc(size: number): number;
  polaris_v1_free(ptr: number, size: number): void;
  polaris_v1_result_len(): number;
  polaris_v1_request(ptr: number, size: number): number;
  polaris_v1_cursor_create(): number;
  polaris_v1_cursor_remember(cursor: number, ptr: number, size: number): boolean | number;
  polaris_v1_cursor_destroy(cursor: number): void;
  polaris_v1_session_create(ptr: number, size: number): number;
  polaris_v1_session_subscribe(session: number): number;
  polaris_v1_session_receive(session: number, ptr: number, size: number): number;
  polaris_v1_session_advance(session: number): void;
  polaris_v1_session_destroy(session: number): void;
  polaris_v1_ohlcv_finish(ptr: number, size: number): number;
}

interface Fault {
  kind: string;
  message: string;
  status_code?: number;
  code?: string;
  resolution?: string;
  body?: string;
  reset_at?: string;
}

export interface PreparedRequest {
  url: string;
  authorization?: string;
}

type StreamFrame = TableDataMessage | { type: "ack" | "pong"; request_id?: string };

const encoder = new TextEncoder();
const decoder = new TextDecoder();

function sdkError(fault: Fault): PolarisError {
  const status = fault.status_code;
  const body = fault.body;
  let error: PolarisError;
  switch (fault.kind) {
    case "unauthorized": error = new UnauthorizedError(fault.message, status, body); break;
    case "access_denied": error = new AccessDeniedError(fault.message, status, body); break;
    case "not_found": error = new NotFoundError(fault.message, status, body); break;
    case "rate_limited": error = new RateLimitedError(fault.message, status, body, fault.reset_at); break;
    case "stream_connection": error = new StreamConnectionError(fault.message); break;
    case "stream_protocol": error = new StreamProtocolError(fault.message, fault.code); break;
    case "stream_decode": error = new StreamDecodeError(fault.message); break;
    default: error = new PolarisError(fault.message, status, body);
  }
  return Object.assign(error, { code: fault.code, resolution: fault.resolution });
}

function decode<T>(text: string): T {
  const payload = parse(text, null, {
    parseNumber: (token) => {
      const number = Number(token);
      return isInteger(token) && !Number.isSafeInteger(number) ? BigInt(token) : number;
    },
  }) as { ok: T } | { error: Fault };
  if ("error" in payload) throw sdkError(payload.error);
  return payload.ok;
}

export class Core {
  constructor(private readonly exports: Exports) {}

  private input<T>(value: string, fn: (ptr: number, size: number) => T): T {
    const bytes = encoder.encode(value);
    const ptr = this.exports.polaris_v1_alloc(bytes.length);
    try {
      new Uint8Array(this.exports.memory.buffer, ptr, bytes.length).set(bytes);
      return fn(ptr, bytes.length);
    } finally {
      this.exports.polaris_v1_free(ptr, bytes.length);
    }
  }

  private output(ptr: number): string {
    const size = this.exports.polaris_v1_result_len();
    try {
      return decoder.decode(new Uint8Array(this.exports.memory.buffer, ptr, size));
    } finally {
      this.exports.polaris_v1_free(ptr, size);
    }
  }

  request<T>(payload: object): T {
    return this.input(JSON.stringify(payload), (ptr, size) =>
      decode<T>(this.output(this.exports.polaris_v1_request(ptr, size))),
    );
  }

  finishOhlcv(buckets: ReadonlyArray<object>): import("./types").OhlcvBar[] {
    return this.input(JSON.stringify(buckets), (ptr, size) =>
      decode(this.output(this.exports.polaris_v1_ohlcv_finish(ptr, size))),
    );
  }

  cursors() {
    const handle = this.exports.polaris_v1_cursor_create();
    if (!handle) throw new PolarisError("Could not allocate cursor tracker");
    return {
      remember: (cursor: string) => {
        const valid = this.input(cursor, (ptr, size) =>
          this.exports.polaris_v1_cursor_remember(handle, ptr, size),
        );
        if (!valid) throw new PolarisError("Data page repeated a cursor");
      },
      close: () => this.exports.polaris_v1_cursor_destroy(handle),
    };
  }

  session(payload: object) {
    const handle = this.input(JSON.stringify(payload), (ptr, size) =>
      this.exports.polaris_v1_session_create(ptr, size),
    );
    if (!handle) throw new StreamProtocolError("Invalid table subscriptions");
    return {
      subscribe: () => this.output(this.exports.polaris_v1_session_subscribe(handle)),
      receive: (message: string) => this.input(message, (ptr, size) =>
        decode<StreamFrame>(this.output(this.exports.polaris_v1_session_receive(handle, ptr, size))),
      ),
      advance: () => this.exports.polaris_v1_session_advance(handle),
      close: () => this.exports.polaris_v1_session_destroy(handle),
    };
  }
}

let ready: Promise<Core> | undefined;

async function loadCore(): Promise<Core> {
  if (typeof WebAssembly !== "undefined") {
    try {
      const bytes = Uint8Array.from(atob(wasmImage), (char) => char.charCodeAt(0));
      const { instance } = await WebAssembly.instantiate(bytes);
      return new Core(instance.exports as unknown as Exports);
    } catch (error) {
      // Browser CSP produces a CompileError even for a valid embedded module.
      if (!(error instanceof WebAssembly.CompileError)) throw error;
    }
  }
  return getSyncCore();
}

export function getCore(): Promise<Core> {
  return ready ??= loadCore();
}

let synchronous: Core | undefined;
export function getSyncCore(): Core {
  return synchronous ??= new Core(makeCore() as Exports);
}
