# Browser Usage Guide

The Polaris TypeScript SDK now supports browser environments, enabling client-side market data processing and visualization.

## Platform Support

The SDK automatically detects the environment and uses the appropriate storage implementation:
- **Node.js**: Uses file system storage (fs/promises)
- **Browser**: Uses IndexedDB for data storage

## Browser Compatibility

### Supported Browsers
- Chrome/Edge 90+
- Firefox 90+
- Safari 15+
- Other modern browsers with IndexedDB support

### Requirements
- Modern browser with IndexedDB support
- ES2020+ JavaScript support
- Secure context (HTTPS) for most features

## Installation

```bash
npm install polaris-data
# or
yarn add polaris-data
# or
pnpm add polaris-data
```

## Basic Usage

The API is identical between Node.js and browser environments. The SDK automatically handles platform detection.

### Initialization

```typescript
import { PolarisClient } from "polaris-data";

// The SDK automatically detects browser environment
const client = new PolarisClient({
  apiKey: "your-api-key",
});
```

### Historical Data Queries

```typescript
// Fetch historical trades (works in both Node.js and browser)
const trades = await client.trades({
  exchange: 'binance',
  symbol: 'BTC-USDT',
  from: '2024-01-01',
  to: '2024-01-02',
});

console.log(`Fetched ${trades.length} trades`);
```

### All SDK Methods Work in Browser

```typescript
// Market data queries
const events = await client.events({ /* ... */ });
const l2Snapshots = await client.l2Snapshots({ /* ... */ });
const bbo = await client.bbo({ /* ... */ });
const fundingRates = await client.fundingRates({ /* ... */ });

// Aggregated data
const ohlcv = await client.ohlcv({ /* ... */ });
const volume = await client.volume({ /* ... */ });
const vwap = await client.vwap({ /* ... */ });

// Catalog and discovery
const catalog = await client.catalog();
const snapshots = await client.listSnapshots({ /* ... */ });
```

## Storage Management

### Browser Storage (IndexedDB)

The SDK uses IndexedDB for browser storage with these features:

- **Automatic Management**: Storage is handled automatically
- **Efficient Caching**: In-memory cache for frequently accessed data
- **Compression**: Zstandard compression for efficient storage
- **Virtual File System**: Path-based organization like Node.js

### Storage Capacity

Browser storage limits vary by browser:
- Chrome/Edge: ~60% of free disk space
- Firefox: ~50% of free disk space
- Safari: ~1GB (varies by device)

The SDK manages storage automatically and handles quota limits gracefully.

### Clearing Storage

To clear all cached data:

```typescript
// Reload the page to clear IndexedDB
location.reload();

// Or use browser dev tools:
// 1. Open DevTools (F12)
// 2. Go to Application > Storage > IndexedDB
// 3. Right-click and delete "polaris-storage"
```

## Bundler Configuration

### Webpack

No special configuration needed. The SDK includes proper conditional exports.

### Vite

No special configuration needed. Works out of the box.

### esbuild

```javascript
esbuild.build({
  entryPoints: ['src/index.ts'],
  bundle: true,
  platform: 'browser',
  target: 'es2020',
  format: 'esm',
  outfile: 'dist/bundle.js',
});
```

### TypeScript Configuration

Ensure your `tsconfig.json` includes:

```json
{
  "compilerOptions": {
    "target": "ES2020",
    "module": "ES2020",
    "moduleResolution": "bundler",
    "lib": ["ES2020", "DOM"]
  }
}
```

## Performance Considerations

### Browser Performance

- **IndexedDB Operations**: Slightly slower than Node.js file system
- **Caching**: In-memory cache improves repeated access
- **Decompression**: Zstd decompression in browser is efficient
- **Data Transfer**: Snapshot downloads cached locally after first fetch

### Optimization Tips

1. **Use Appropriate Time Ranges**: Smaller time ranges are faster
2. **Reuse Client**: Keep client instance for multiple queries
3. **Parallel Queries**: Browser handles concurrent requests well
4. **Monitor Storage**: Check IndexedDB usage for large datasets

## Error Handling

```typescript
try {
  const trades = await client.trades({
    exchange: 'binance',
    symbol: 'BTC-USDT',
    from: '2024-01-01',
    to: '2024-01-02',
  });
} catch (error) {
  if (error instanceof PolarisError) {
    console.error('Polaris error:', error.message);
  } else if (error instanceof UnauthorizedError) {
    console.error('Authentication failed');
  } else if (error instanceof NotFoundError) {
    console.error('Data not found');
  }
}
```

## Migration from Node.js

### No Changes Required

Existing Node.js code works without modifications:

```typescript
// This code works in both Node.js and browser
import { PolarisClient } from "polaris-data";

const client = new PolarisClient({
  apiKey: "your-api-key",
  datasetRoot: "./data", // Only used in Node.js
});

const data = await client.trades({ /* ... */ });
```

### Browser-Specific Notes

- **`datasetRoot` Option**: Ignored in browser (uses IndexedDB)
- **Environment Variables**: Browser builds do not read `POLARIS_API_KEY`; pass `apiKey` explicitly
- **File Paths**: Virtual paths used internally in browser

## Examples

### Real-Time Chart Application

```typescript
import { PolarisClient } from "polaris-data";

const client = new PolarisClient({
  apiKey: "your-api-key",
});

async function loadChartData() {
  const ohlcv = await client.ohlcv({
    exchange: 'binance',
    symbol: 'BTC-USDT',
    from: '2024-01-01',
    to: '2024-01-31',
    interval: '1h',
  });

  return ohlcv.map(bar => ({
    time: bar.timestamp,
    open: bar.open,
    high: bar.high,
    low: bar.low,
    close: bar.close,
  }));
}

// Use with your charting library
const chartData = await loadChartData();
updateChart(chartData);
```

### Market Analysis Dashboard

```typescript
import { PolarisClient } from "polaris-data";

const client = new PolarisClient({
  apiKey: "your-api-key",
});

async function loadMarketAnalysis() {
  const [
    catalog,
    recentTrades,
    fundingRates,
  ] = await Promise.all([
    client.catalog(),
    client.trades({
      exchange: 'binance',
      symbol: 'BTC-USDT',
      from: new Date(Date.now() - 3600000), // Last hour
      to: new Date(),
    }),
    client.fundingRates({
      exchange: 'binance',
      symbol: 'BTC-USDT',
      from: '2024-01-01',
      to: '2024-01-31',
    }),
  ]);

  return { catalog, recentTrades, fundingRates };
}

const analysis = await loadMarketAnalysis();
updateDashboard(analysis);
```

## Troubleshooting

### Common Issues

**Issue**: "IndexedDB is not defined"
- **Solution**: Ensure you're using a modern browser with IndexedDB support

**Issue**: "Storage quota exceeded"
- **Solution**: Clear browser data or reduce dataset size

**Issue**: Slow initial load
- **Solution**: First download caches locally; subsequent loads are faster

**Issue**: Network errors during download
- **Solution**: Check network connection and API key validity

### Debug Mode

Enable detailed logging:

```typescript
const client = new PolarisClient({
  apiKey: "your-api-key",
  // Add logging for debugging
});
```

### Browser DevTools

Use browser DevTools to inspect:
- **Network**: API requests and downloads
- **Application > Storage**: IndexedDB contents
- **Console**: Error messages and warnings
- **Performance**: Operation timing

## Security Considerations

### API Keys in Browser

- **Use HTTPS**: Always use secure contexts
- **Environment Variables**: Not available in browser, use explicit options
- **Key Management**: Consider backend proxy for production apps

### CORS

The SDK uses standard `fetch` API. Ensure CORS is configured if needed.

## Limitations

### Browser-Specific Limitations

- **No Direct File System**: Uses IndexedDB instead
- **Storage Quotas**: Browser storage limits apply
- **Performance**: Slightly slower than Node.js for large datasets
- **Environment**: No `process.env` in browser

### Unsupported Features

- **`datasetRoot` Option**: Only applies to Node.js
- **Custom File Paths**: Virtual paths used internally
- **Direct File Access**: IndexedDB abstraction layer

## Next Steps

- **Examples**: See `/examples` directory for complete examples
- **API Documentation**: Full API reference available
- **GitHub Issues**: Report browser-specific issues
- **Community**: Join discussions for browser use cases

## Support

For browser-specific issues:
- Check browser compatibility
- Verify IndexedDB is enabled
- Review console errors
- Check network requests in DevTools

The SDK aims to provide identical functionality in both Node.js and browser environments. Report any discrepancies as bugs.
