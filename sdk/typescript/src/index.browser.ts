import { BasePolarisClient } from "./client";
import { browserRuntime } from "./runtime/browser";
import type { PolarisClientOptions } from "./types";

export class PolarisClient extends BasePolarisClient {
  constructor(options: PolarisClientOptions = {}) {
    super(options, browserRuntime);
  }
}

export * from "./public";
