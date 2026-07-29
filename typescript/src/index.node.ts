import { BasePolarisClient } from "./client";
import { nodeRuntime } from "./runtime/node";
import type { PolarisClientOptions } from "./types";

export class PolarisClient extends BasePolarisClient {
  constructor(options: PolarisClientOptions = {}) {
    super(options, nodeRuntime);
  }
}

export * from "./public";
