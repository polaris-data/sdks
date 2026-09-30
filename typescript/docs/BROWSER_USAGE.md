# Browser usage

The browser entry uses `fetch` for historical queries and the browser's `WebSocket` for realtime streams. The SDK no longer stores historical snapshots in IndexedDB.

```ts
import { PolarisClient } from "polaris-data";

const client = new PolarisClient({ apiKey: "polaris_key_your_key" });
const rows = await client.trades({ source: "binance", market: "BTC-USDT" });
console.log(rows);
client.close();
```

Historical methods return flat API rows as arrays and follow cursor pages. Use inclusive Unix-millisecond `start` and `end` bounds where needed. `rawChannel` queries one source and native channel through `/raw`; the client converts its millisecond bounds to ISO date-times. `l2Snapshots` returns reconstructed top-25 books.

`events`, `replay`, `listSnapshots`, `getSnapshotDownloadUrls`, and `propammQuoteLadders` are no longer available. `OrderbookBuilder` remains available for realtime event envelopes.

Browser API keys are visible to the browser user. Configure CORS and key scope for your application accordingly.
