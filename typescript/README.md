# polaris-data

TypeScript SDK for the Polaris market data API in Node.js 18+ and browsers.

```sh
npm install polaris-data
```

```typescript
import { PolarisClient } from "polaris-data";

const client = new PolarisClient({ apiKey: "your-polaris-api-key" });
try {
  const page = await client.data({
    table: "standard.trades",
    filters: { "market.eq": "BTC-USDT" },
    limit: 10,
  });
  console.log(page.data, page.next_cursor);
} finally {
  client.close();
}
```

Use `coverage()` to discover tables, `dataRows()` to iterate across pages, `sql({ query })` for analytics, and `candles()` for aligned DEX pool candles. `stream({ subscriptions })` yields table pages with opaque resume cursors. Unquoted integers outside the safe integer range are returned as `bigint`.

See the [main SDK guide](https://github.com/polaris-data/sdks#readme), [migration guide](https://github.com/polaris-data/sdks/blob/main/MIGRATION.md), and [browser guide](https://github.com/polaris-data/sdks/blob/main/typescript/docs/BROWSER_USAGE.md).
