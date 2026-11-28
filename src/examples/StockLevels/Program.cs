// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async;

namespace StockLevels;

/// <summary>Tracks shelf stock for a live display and totals a delivery manifest.</summary>
internal static class Program
{
    /// <summary>Runs the live and asynchronous stock calculations.</summary>
    /// <returns>Completion after both calculations agree with their expected results.</returns>
    internal static async Task Main()
    {
        ShowLiveStock();
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(1));
        await CountManifestAsync(timeout.Token);
        ReorderAlerts.CheckQuietPeriod();
        BarcodeScanner.CheckSubscription();
        await ManifestRetry.CheckRetryAsync(timeout.Token);
    }

    /// <summary>Shares each stock change and replays the latest count when a second display connects.</summary>
    /// <exception cref="InvalidOperationException">A display missed an expected stock level.</exception>
    private static void ShowLiveStock()
    {
        const int OpeningStock = 10;
        const int Delivered = 5;
        const int Sold = 2;
        const int AfterDelivery = OpeningStock + Delivered;
        const int AfterSale = AfterDelivery - Sold;
        const int ClosingStock = AfterSale - 1;
        using Signal<int> adjustments = new();
        var levels = adjustments.Where(static quantity => quantity != 0)
            .Fold(OpeningStock, static (stock, quantity) => stock + quantity)
            .Replay(1);
        List<int> counter = [];
        List<int> shelf = [];
        using var counterSubscription = levels.Subscribe(counter.Add);
        using var connection = levels.Connect();
        adjustments.OnNext(Delivered);
        adjustments.OnNext(0);
        adjustments.OnNext(-Sold);
        using var shelfSubscription = levels.Subscribe(shelf.Add);
        adjustments.OnNext(-1);
        adjustments.OnCompleted();

        if (counter is not [AfterDelivery, AfterSale, ClosingStock] || shelf is not [AfterSale, ClosingStock])
        {
            throw new InvalidOperationException("The late display should start at 13 and finish at 12.");
        }
    }

    /// <summary>Totals the quantities at the even-numbered stops on a delivery route.</summary>
    /// <param name="cancellationToken">Cancels the manifest read.</param>
    /// <returns>Completion after the running totals reach the final delivery.</returns>
    /// <exception cref="InvalidOperationException">The manifest produced unexpected running totals.</exception>
    private static async Task CountManifestAsync(CancellationToken cancellationToken)
    {
        const int StopCount = 6;
        const int StopInterval = 2;
        const int ItemsPerBox = 10;
        const int FirstTotal = 20;
        const int SecondTotal = 60;
        const int FinalTotal = 120;
        var totals = await SignalAsync.Range(1, StopCount)
            .Where(static stop => stop % StopInterval == 0)
            .Select(static stop => stop * ItemsPerBox)
            .Fold(0, static (total, quantity) => total + quantity)
            .ToListAsync(cancellationToken);

        if (totals is not [FirstTotal, SecondTotal, FinalTotal])
        {
            throw new InvalidOperationException("The three stops should deliver 120 items in total.");
        }
    }
}
