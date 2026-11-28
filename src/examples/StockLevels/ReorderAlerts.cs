// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;

namespace StockLevels;

/// <summary>Waits for shelf counts to settle before telling a buyer to reorder.</summary>
internal static class ReorderAlerts
{
    /// <summary>Combines the current limit and count on a clock that the example controls.</summary>
    /// <exception cref="InvalidOperationException">The buyer received a missing, premature or cancelled alert.</exception>
    internal static void CheckQuietPeriod()
    {
        const int ReorderAt = 6;
        const int FirstCount = 5;
        const int Confirmed = 4;
        var interval = TimeSpan.FromSeconds(1);
        var clock = new StockClock();
        using Signal<int> counts = new();
        using Signal<int> limits = new();
        using Pocket subscriptions = new();
        List<int> reorder = [];
        var alerts = counts.SyncLatest(limits, static (count, limit) => (Count: count, Limit: limit))
            .Keep(static pair => pair.Count <= pair.Limit)
            .Map(static pair => pair.Count)
            .DistinctUntilChanged()
            .Calm(interval, clock);
        subscriptions.Add(alerts.Subscribe(reorder.Add));
        limits.OnNext(ReorderAt);
        counts.OnNext(FirstCount);
        counts.OnNext(Confirmed);
        if (reorder.Count != 0)
        {
            throw new InvalidOperationException("A count reached the buyer before the quiet period ended.");
        }

        clock.AdvanceBy(interval);
        counts.OnNext(FirstCount);
        subscriptions.Clear();
        clock.AdvanceBy(interval);
        if (reorder is not [Confirmed])
        {
            throw new InvalidOperationException("The buyer should receive only the settled count.");
        }
    }
}
