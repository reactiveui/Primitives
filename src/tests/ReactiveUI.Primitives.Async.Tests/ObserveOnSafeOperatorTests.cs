// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests optional context targets and forced-yield wrapper selection.</summary>
public class ObserveOnSafeOperatorTests
{
    /// <summary>The value the sources emit.</summary>
    private const int Sentinel = 42;

    /// <summary>A null context preserves the source instance.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenObserveOnSafeAsyncContextIsNullWithForcedYielding_ThenReturnsSourceUnchanged()
    {
        var source = SignalAsync.Return(Sentinel);

        var observed = source.ObserveOnSafe((AsyncContext?)null, true);

        await Assert.That(observed).IsSameReferenceAs(source);
        await Assert.That(await observed.FirstAsync()).IsEqualTo(Sentinel);
    }

    /// <summary>A supplied context selects the context-switching wrapper.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenObserveOnSafeAsyncContextSuppliedWithForcedYielding_ThenWrapsSource()
    {
        var source = SignalAsync.Return(Sentinel);

        var observed = source.ObserveOnSafe(AsyncContext.Default, true);

        await Assert.That(ReferenceEquals(observed, source)).IsFalse();
        await Assert.That(observed).IsTypeOf<WitnessOnSignal<int>>();
    }

    /// <summary>Verifies that a null <see cref="TaskScheduler"/> makes the forced-yield overload a no-op.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenObserveOnSafeTaskSchedulerIsNullWithForcedYielding_ThenReturnsSourceUnchanged()
    {
        var source = SignalAsync.Return(Sentinel);

        var observed = source.ObserveOnSafe((TaskScheduler?)null, true);

        await Assert.That(observed).IsSameReferenceAs(source);
        await Assert.That(await observed.FirstAsync()).IsEqualTo(Sentinel);
    }

    /// <summary>A supplied scheduler selects the context-switching wrapper.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenObserveOnSafeTaskSchedulerSuppliedWithForcedYielding_ThenWrapsSource()
    {
        var source = SignalAsync.Return(Sentinel);

        var observed = source.ObserveOnSafe(TaskScheduler.Default, true);

        await Assert.That(ReferenceEquals(observed, source)).IsFalse();
        await Assert.That(observed).IsTypeOf<WitnessOnSignal<int>>();
    }
}
