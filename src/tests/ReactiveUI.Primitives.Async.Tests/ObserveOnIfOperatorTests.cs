// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests conditional context-switching wrapper selection.</summary>
public class ObserveOnIfOperatorTests
{
    /// <summary>The value the sources emit.</summary>
    private const int Sentinel = 42;

    /// <summary>Verifies that a false condition makes the forced-yield <see cref="AsyncContext"/> overload a no-op.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenObserveOnIfAsyncContextConditionFalseWithForcedYielding_ThenReturnsSourceUnchanged()
    {
        var source = SignalAsync.Return(Sentinel);

        var observed = source.ObserveOnIf(false, AsyncContext.Default, true);

        await Assert.That(observed).IsSameReferenceAs(source);
        await Assert.That(await observed.FirstAsync()).IsEqualTo(Sentinel);
    }

    /// <summary>A true condition selects the context-switching wrapper.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenObserveOnIfAsyncContextConditionTrueWithForcedYielding_ThenWrapsSource()
    {
        var source = SignalAsync.Return(Sentinel);

        var observed = source.ObserveOnIf(true, AsyncContext.Default, true);

        await Assert.That(ReferenceEquals(observed, source)).IsFalse();
        await Assert.That(observed).IsTypeOf<WitnessOnSignal<int>>();
    }

    /// <summary>Verifies that a false condition makes the forced-yield <see cref="TaskScheduler"/> overload a no-op.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenObserveOnIfTaskSchedulerConditionFalseWithForcedYielding_ThenReturnsSourceUnchanged()
    {
        var source = SignalAsync.Return(Sentinel);

        var observed = source.ObserveOnIf(false, TaskScheduler.Default, true);

        await Assert.That(observed).IsSameReferenceAs(source);
        await Assert.That(await observed.FirstAsync()).IsEqualTo(Sentinel);
    }

    /// <summary>A true condition selects the scheduler context wrapper.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenObserveOnIfTaskSchedulerConditionTrueWithForcedYielding_ThenWrapsSource()
    {
        var source = SignalAsync.Return(Sentinel);

        var observed = source.ObserveOnIf(true, TaskScheduler.Default, true);

        await Assert.That(ReferenceEquals(observed, source)).IsFalse();
        await Assert.That(observed).IsTypeOf<WitnessOnSignal<int>>();
    }
}
