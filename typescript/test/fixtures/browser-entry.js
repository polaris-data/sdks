import { PolarisClient } from "polaris-data";

const client = new PolarisClient({
  apiKey: "test-key",
  datasetRoot: "/ignored-in-browser",
});

void client;
