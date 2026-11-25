# Browser usage

Bundlers select the browser entry for `polaris-data`. It uses browser `fetch` and `WebSocket`, with an optional custom `fetch` in the client options. The Zig core loads on the first request. Browsers that block Wasm under their Content Security Policy use JavaScript generated from the same core.

```typescript
import { PolarisClient } from "polaris-data";

const client = new PolarisClient();
try {
  console.log(await client.coverage({ database: "standard", table: "trades" }));
} finally {
  client.close();
}
```

Browser API keys are visible to the browser user. Use an appropriately scoped key when authenticating and configure CORS for requests to your own proxy or API origin.
