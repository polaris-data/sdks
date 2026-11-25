# Polaris SDKs

Zig, Rust, Python, and TypeScript clients for the Polaris market data API. The SDKs share a Zig core and generate their typed methods from the [production API contract](https://api.polaris.supply/openapi.json).

## Install

```sh
cargo add polaris-data
pip install polaris-data
npm install polaris-data
```

For Zig 0.17.1, add the [Zig source archive](https://github.com/polaris-data/sdks/releases) to your project with `zig fetch --save=polaris_data <archive-url>`, then import the `polaris_data` module in your build:

```zig
const polaris = b.dependency("polaris_data", .{ .target = target, .optimize = optimize });
exe.root_module.addImport("polaris_data", polaris.module("polaris_data"));
```

## Quickstart

```python
from polaris_data import PolarisClient

with PolarisClient() as client:
    print(client.coverage(database="standard", table="trades"))
    print(client.sql("SELECT 1")["data"])
```

```typescript
import { PolarisClient } from "polaris-data";

const client = new PolarisClient();
try {
  const coverage = await client.coverage({ database: "standard", table: "trades" });
  console.log(coverage.tables);
} finally {
  client.close();
}
```

```rust
use polaris_data::{CoverageQuery, blocking::PolarisClient};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = PolarisClient::builder().build()?;
    let coverage = client.coverage(CoverageQuery {
        database: Some("standard".into()),
        table: Some("trades".into()),
    })?;
    println!("{:?}", coverage.tables);
    Ok(())
}
```

```zig
const std = @import("std");
const polaris = @import("polaris_data");

pub fn main() !void {
    const client = polaris.Client.init(.{});
    var result = try client.coverage(std.heap.page_allocator, .{
        .database = "standard",
        .table = "trades",
    });
    defer result.deinit();
    switch (result.result) {
        .ok => |coverage| std.debug.print("{d} matching tables\n", .{coverage.total}),
        .fault => |fault| std.debug.print("{s}\n", .{fault.message}),
    }
}
```

## Data and streams

| Method | What it returns |
| --- | --- |
| `health`, `openapi` | Service health and the current OpenAPI document |
| `coverage` | Table inventory, column types, and observed time bounds |
| `data` | One stored-row page with `has_more` and `next_cursor` |
| `data_rows` / `dataRows` | Rows fetched lazily across cursor pages |
| `sql` | Query ID, typed column metadata, and positional rows |
| `candles` | Sparse DEX pool execution-price candles |
| `stream` | Realtime table pages with resume cursors |

Set `POLARIS_API_KEY` or supply a key explicitly in Rust, Python, and Node. Zig and browser applications pass the key in the client options. `health`, `openapi`, and `coverage` are public. `sql` supports anonymous recent-history queries. `data` and `candles` require a key.

Use exact `database.table` identifiers with `data`, for example `standard.trades`. Filters use column names or suffixes such as `market.eq`, `price.gte`, and `price.lte`. Supply scalar values as strings to preserve their exact spelling. Repeat filters when using a continuation cursor. `data` time bounds accept inclusive RFC 3339 timestamps or Unix milliseconds. `candles` takes aligned Unix milliseconds with an exclusive end and requires a manager address for Uniswap V4. Stored row fields, including quoted decimals and 64-bit integers, retain the service's representation. TypeScript returns an unquoted integer as a `bigint` when it exceeds the safe integer range.

Stream subscriptions specify a table, optional filters, and an optional resume cursor. Each page contains `table`, `filters`, `data`, `cursor`, and `caught_up`. Save a page’s cursor after processing all of its rows. Reconnects can replay a page. Close the stream to interrupt a waiting read.

```python
with PolarisClient() as client:
    page = client.data("standard.trades", filters={"market.eq": "BTC-USDT"}, limit=10)
    print(page["data"])

    with client.stream([{"table": "standard.trades"}]) as pages:
        page = next(pages)
        print(page["data"], page["cursor"])
```

See the [migration guide](MIGRATION.md) for replacements for retired routes and the [browser guide](typescript/docs/BROWSER_USAGE.md) for browser setup. The [pinned notebooks](notebooks/README.md) show historical analyses and saved outputs.

## Development and release

Install Zig 0.17.1 and Binaryen 133, or run `python3 tools/install-toolchain.py --binaryen` and add the printed paths to your shell.

```sh
python3 tools/generate.py --check
zig build test
python3 tools/package-zig.py source-archive --check
cargo test --workspace --locked
uv sync --locked
uv run --no-sync pytest
npm ci --prefix typescript
npm test --prefix typescript
```

Edit the checked-in [OpenAPI snapshot](schema/openapi.json) and [SDK overlay](schema/sdk.json) when the contract changes. Run `python3 tools/generate.py` and commit the generated bindings alongside their inputs. Rust and Python builds from a source checkout use Zig. Published artifacts bundle the native core for standard targets. The npm build produces one Wasm image and a JavaScript fallback from that image.

Run **Check SDK release candidate** on the commit intended for release. Its `sdk-release-smoke` environment uses `POLARIS_SDK_SMOKE_API_KEY`, `POLARIS_SMOKE_CANDLE_QUERY`, and `POLARIS_SMOKE_STREAM_SUBSCRIPTION`. Set the last two as JSON objects using a known production pool and a subscription that will deliver a page during the smoke test. After the workflow passes, the separate `rust-v*`, `python-v*`, `typescript-v*`, and `zig-v*` tags identify the registry packages or Zig release archive.
