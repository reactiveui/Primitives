// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Verifies scheduler selection by time-operator aliases.</summary>
public partial class RxNamesTests
{
    /// <summary>Verifies absolute-time overloads use the default scheduler when no scheduler is supplied.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task AbsoluteTimeOperatorsUseDefaultScheduler()
    {
        var dueTime = DateTimeOffset.UnixEpoch;
        AwaitableWitness<int> delayedScalar = new();
        AwaitableWitness<int> delayedRange = new();
        AwaitableWitness<int> delayedSubscriptionScalar = new();
        AwaitableWitness<int> delayedSubscriptionRange = new();
        AwaitableWitness<int> delayedExplicitRange = new();
        AwaitableWitness<int> delayedSubscriptionExplicitRange = new();
        AwaitableWitness<int> timeout = new();
        AwaitableWitness<int> explicitTimeout = new();
        const ISequencer? defaultScheduler = null;

        using var delayScalarSubscription = Signal.Emit(One)
            .Delay(dueTime)
            .Subscribe(delayedScalar);
        using var delayRangeSubscription = Signal.Sequence(Two, Two)
            .Delay(dueTime)
            .Subscribe(delayedRange);
        using var delayExplicitRangeSubscription = Signal.Sequence(Two, Two)
            .Delay(dueTime, defaultScheduler)
            .Subscribe(delayedExplicitRange);
        using var subscriptionScalarSubscription = Signal.Emit(One)
            .DelaySubscription(dueTime)
            .Subscribe(delayedSubscriptionScalar);
        using var subscriptionRangeSubscription = Signal.Sequence(Two, Two)
            .DelaySubscription(dueTime)
            .Subscribe(delayedSubscriptionRange);
        using var subscriptionExplicitRangeSubscription = Signal.Sequence(Two, Two)
            .DelaySubscription(dueTime, defaultScheduler)
            .Subscribe(delayedSubscriptionExplicitRange);
        using var timeoutSubscription = Signal.Silent<int>()
            .Timeout(dueTime)
            .Subscribe(timeout);
        using var explicitTimeoutSubscription = Signal.Silent<int>()
            .Timeout(dueTime, defaultScheduler)
            .Subscribe(explicitTimeout);

        await delayedScalar.ValueCountReaching(One);
        await delayedRange.ValueCountReaching(Two);
        await delayedExplicitRange.ValueCountReaching(Two);
        await delayedSubscriptionScalar.ValueCountReaching(One);
        await delayedSubscriptionRange.ValueCountReaching(Two);
        await delayedSubscriptionExplicitRange.ValueCountReaching(Two);
        var timedOut = await timeout.FirstError;
        var explicitlyTimedOut = await explicitTimeout.FirstError;

        await Assert.That(delayedScalar.Values.SequenceEqual([One])).IsTrue();
        await Assert.That(delayedRange.Values.SequenceEqual([Two, Three])).IsTrue();
        await Assert.That(delayedExplicitRange.Values.SequenceEqual([Two, Three])).IsTrue();
        await Assert.That(delayedSubscriptionScalar.Values.SequenceEqual([One])).IsTrue();
        await Assert.That(delayedSubscriptionRange.Values.SequenceEqual([Two, Three])).IsTrue();
        await Assert.That(delayedSubscriptionExplicitRange.Values.SequenceEqual([Two, Three])).IsTrue();
        await Assert.That(timedOut).IsTypeOf<TimeoutException>();
        await Assert.That(explicitlyTimedOut).IsTypeOf<TimeoutException>();
    }

    /// <summary>Time aliases construct signals when no sequencer is supplied.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task TimeOperatorsAcceptDefaultSequencer()
    {
        await Assert.That(Signal.Sequence(One, Three).Delay(TimeSpan.FromTicks(DueTicks))).IsNotNull();
        await Assert.That(Signal.FromEnumerable(_oneToThree).Timeout(TimeSpan.FromTicks(DueTicks))).IsNotNull();
        await Assert.That(Signal.FromEnumerable(_oneToThree).Sample(TimeSpan.FromTicks(DueTicks))).IsNotNull();
    }
}
