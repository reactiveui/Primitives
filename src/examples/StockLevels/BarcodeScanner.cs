// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.ObservableEvents;
using ReactiveUI.Primitives.R3Bridge;

namespace StockLevels;

/// <summary>Exposes a barcode reader's .NET event and passes it through the R3 adapter.</summary>
public sealed class BarcodeScanner
{
    /// <summary>Occurs when a barcode is scanned.</summary>
    public event EventHandler<BarcodeScannedEventArgs>? Scanned;

    /// <summary>Checks that disposing the generated event subscription detaches the scanner.</summary>
    /// <exception cref="InvalidOperationException">The scanner did not detach after the first barcode.</exception>
    internal static void CheckSubscription()
    {
        const string FirstLabel = "BIN-A";
        var scanner = new BarcodeScanner();
        List<string> labels = [];
        var values = scanner.Events().Scanned.Map(static args => args.Barcode).AsR3Observable().AsPrimitivesSignal();
        using (values.Subscribe(labels.Add))
        {
            scanner.Scanned?.Invoke(scanner, new(FirstLabel));
        }

        scanner.Scanned?.Invoke(scanner, new("BIN-B"));
        if (labels is not [FirstLabel])
        {
            throw new InvalidOperationException("Only the subscribed barcode should reach the stock count.");
        }
    }
}
