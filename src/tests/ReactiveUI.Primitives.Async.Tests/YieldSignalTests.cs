// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests yielding to the scheduler captured during subscription.</summary>
public sealed class YieldSignalTests
{
    /// <summary>Each value waits for the captured scheduler before reaching its observer.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task SubscribeAsync_CustomScheduler_DefersValuesUntilScheduled()
    {
        ManualTaskScheduler scheduler = new();
        DirectSource<int> source = new();
        List<int> values = [];
        CallbackWitnessAsync<int> observer = new((value, _) =>
        {
            values.Add(value);
            return default;
        });
        var subscribing = Task.Factory.StartNew(
            static state =>
            {
                var (source, observer) = ((DirectSource<int>, CallbackWitnessAsync<int>))state!;
                return ((IObservableAsync<int>)source).Yield().SubscribeAsync(observer, CancellationToken.None);
            },
            (source, observer),
            CancellationToken.None,
            TaskCreationOptions.DenyChildAttach,
            scheduler);
        scheduler.RunNext();
        var scheduledSubscription = await subscribing;
        await using var subscription = await scheduledSubscription;

        var pending = source.EmitNext(1);
        await Assert.That(values).IsEmpty();
        await Assert.That(scheduler.PendingCount).IsEqualTo(1);

        scheduler.RunNext();
        await pending;

        await Assert.That(values).IsCollectionEqualTo([1]);
    }
}
