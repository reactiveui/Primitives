# Stock levels

A counter and a shelf display share the current stock level. Run the example to follow a sale, a reorder alert, a barcode scan and a delivery manifest.

| Scenario | What the example checks |
| --- | --- |
| [Share the shelf count](StockLevels/Program.cs) | The counter sees all changes. A late display receives the latest count through `Replay(1)`. The async calculation collects running totals with a cancellation token. |
| [Wait before reordering](StockLevels/ReorderAlerts.cs) | Combine the shelf count with its reorder limit. Advance a virtual clock through the quiet period and clear pending subscriptions when the buyer closes the display. |
| [Scan a barcode](StockLevels/BarcodeScanner.cs) | Use the generated event wrapper, pass the barcode to R3 and back, and dispose the subscription before the next scan. |
| [Read an unreliable manifest](StockLevels/ManifestRetry.cs) | Create a fresh read on each retry and recover from an offline carrier with a cached count. |

Run either project from `src`. Both use [the same stock calculations](StockLevels/Program.cs).

```shell
dotnet run --project examples/StockLevels -f net11.0
dotnet run --project examples/StockLevels.Reactive -f net11.0
```

Use `-f net10.0` to run either build on .NET 10.
