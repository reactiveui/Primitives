// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Slow observer and resubscription soak checks.</summary>
public sealed partial class ObserverNotificationDispatcherTests
{
    /// <summary>The number of notifications in the reconnect storm.</summary>
    private const int SoakNotificationCount = 10_000;

    /// <summary>The maximum queued event count per observer.</summary>
    private const int SoakObserverCapacity = 8;

    /// <summary>Checks a slow observer remains bounded and new subscriptions recover after overflow.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SlowObserverStormDisconnectsAndResubscribeRecovers()
    {
        var scheduler = new ControlledObserverScheduler();
        using var dispatcher = new ObserverNotificationDispatcher<int>(scheduler);
        var slow = new RecordingObserver<int>();
        using var slowSubscription = dispatcher.Subscribe(
            slow,
            new(SoakObserverCapacity, SoakObserverCapacity, ObserverNotificationOverflowMode.Disconnect));

        for (var index = 0; index < SoakNotificationCount; index++)
        {
            _ = dispatcher.PublishEvent(index, OneByte);
        }

        await Assert.That(scheduler.PendingCount).IsLessThanOrEqualTo(OneItem);
        scheduler.RunAll();
        await Assert.That(slow.Error).IsTypeOf<ObserverNotificationOverflowException>();
        await Assert.That(slow.Values.Count).IsLessThanOrEqualTo(SoakObserverCapacity);
        await Assert.That(dispatcher.SubscriptionCount).IsEqualTo(None);

        var resumed = new RecordingObserver<int>();
        using var resumedSubscription = dispatcher.Subscribe(
            resumed,
            new(SoakObserverCapacity, SoakObserverCapacity, ObserverNotificationOverflowMode.Disconnect));
        _ = dispatcher.PublishEvent(SoakNotificationCount, OneByte);
        scheduler.RunAll();

        await Assert.That(resumed.Values).Count().IsEqualTo(OneItem);
        await Assert.That(resumed.Values[0]).IsEqualTo(SoakNotificationCount);
        await Assert.That(resumed.Error).IsNull();
    }
}
