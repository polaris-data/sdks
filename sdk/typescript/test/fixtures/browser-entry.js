import { OhlcvAggregator, OrderbookBuilder, PolarisClient } from "polaris-data";

globalThis.runPolarisFixture = async (baseUrl) => {
  const client = new PolarisClient({ apiKey: "fixture-key", baseUrl });
  try {
    const health = await client.health();
    const ids = [];
    for await (const row of client.dataRows({ table: "standard.trades" })) ids.push(row.id);
    const cursors = [];
    const stream = client.stream({ subscriptions: [{ table: "standard.trades" }] });
    for await (const page of stream) cursors.push(page.cursor);
    const bars = new OhlcvAggregator("1m");
    bars.add(60_001, 101, 2);
    bars.add(100, 100, 1);
    const books = new OrderbookBuilder();
    const snapshot = { timestamp: 1, source: "fixture", market: "BTC", type: "orderbook" };
    books.update({ ...snapshot, data: { bids: [[99, 1]], asks: [[101, 2]] } });
    books.update({ ...snapshot, type: "orderbook_delta", data: { bids: [[99, 0], [98, 3]] } });
    return { health, ids, cursors, bars: bars.finish(), book: books.snapshot("fixture", "BTC") };
  } finally {
    client.close();
  }
};
