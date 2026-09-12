// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Verifies scheduler selection by time-operator aliases.</summary>
public partial class RxNamesTests
{
    /// <summary>Absolute-time overloads select the thread-pool sequencer when no scheduler is supplied.</summary>
    /// <param name="explicitNull">True to supply a null scheduler explicitly.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task AbsoluteTimeOperatorsUseDefaultScheduler(bool explicitNull)
    {
        var dueTime = DateTimeOffset.UnixEpoch;
        IObservable<int>[] sources = [Signal.Emit(One), Signal.Sequence(Two, Two)];
        foreach (var source in sources)
        {
            var delayed = explicitNull ? source.Delay(dueTime, null) : source.Delay(dueTime);
            var delay = await Assert.That(delayed).IsTypeOf<LinqExtensions.AbsoluteShiftSignal<int>>().And.IsNotNull();
            await Assert.That(delay.Scheduler).IsSameReferenceAs(ThreadPoolSequencer.Instance);

            var delayedSubscription = explicitNull
                ? source.DelaySubscription(dueTime, null)
                : source.DelaySubscription(dueTime);
            var subscription = await Assert.That(delayedSubscription)
                .IsTypeOf<LinqExtensions.AbsoluteDelayStartSignal<int>>().And.IsNotNull();
            await Assert.That(subscription.Scheduler).IsSameReferenceAs(ThreadPoolSequencer.Instance);
        }

        var expiring = explicitNull
            ? Signal.Silent<int>().Timeout(dueTime, null)
            : Signal.Silent<int>().Timeout(dueTime);
        var timeout = await Assert.That(expiring).IsTypeOf<LinqExtensions.AbsoluteExpireSignal<int>>().And.IsNotNull();
        await Assert.That(timeout.Scheduler).IsSameReferenceAs(ThreadPoolSequencer.Instance);
    }

    /// <summary>Absolute delays emit scalar and range values in order, then complete, when the due time arrives.</summary>
    /// <param name="range">True to delay a range; false to delay a scalar value.</param>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task AbsoluteDelays_ClockReachesDueTime_EmitValuesAndComplete(bool range)
    {
        VirtualClock clock = new(DateTimeOffset.UnixEpoch);
        var dueTime = clock.Now.AddTicks(DueTicks);
        var source = range ? Signal.Sequence(Two, Two) : Signal.Emit(One);
        RecordingWitness<int> delayed = new();
        RecordingWitness<int> delayedSubscription = new();
        using var delayHandle = source.Delay(dueTime, clock).Subscribe(delayed);
        using var subscriptionHandle = source.DelaySubscription(dueTime, clock).Subscribe(delayedSubscription);

        await Assert.That(delayed.Values).IsEmpty();
        await Assert.That(delayedSubscription.Values).IsEmpty();
        await Assert.That(delayed.Completed).IsEqualTo(0);
        await Assert.That(delayedSubscription.Completed).IsEqualTo(0);

        clock.AdvanceBy(TimeSpan.FromTicks(DueTicks));

        int[] expected = range ? [Two, Three] : [One];
        await Assert.That(delayed.Values.SequenceEqual(expected)).IsTrue();
        await Assert.That(delayedSubscription.Values.SequenceEqual(expected)).IsTrue();
        await Assert.That(delayed.Completed).IsEqualTo(1);
        await Assert.That(delayedSubscription.Completed).IsEqualTo(1);
        await Assert.That(delayed.Errors).IsEmpty();
        await Assert.That(delayedSubscription.Errors).IsEmpty();
    }

    /// <summary>An absolute timeout emits one error when the clock reaches its due time.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task AbsoluteTimeout_ClockReachesDueTime_EmitsOneError()
    {
        VirtualClock clock = new(DateTimeOffset.UnixEpoch);
        RecordingWitness<int> observer = new();
        using var subscription = Signal.Silent<int>()
            .Timeout(clock.Now.AddTicks(DueTicks), clock)
            .Subscribe(observer);

        await Assert.That(observer.Errors).IsEmpty();
        clock.AdvanceBy(TimeSpan.FromTicks(DueTicks));

        await Assert.That(observer.Errors).HasSingleItem();
        await Assert.That(observer.Errors[0]).IsTypeOf<TimeoutException>();
        await Assert.That(observer.Values).IsEmpty();
        await Assert.That(observer.Completed).IsEqualTo(0);

        clock.AdvanceBy(TimeSpan.FromTicks(DueTicks));
        await Assert.That(observer.Errors).HasSingleItem();
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
