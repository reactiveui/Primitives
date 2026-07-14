// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>
/// Tests for the forced-yield overloads of the <c>ObserveOnSafe</c> parity helper — the ones that take an
/// explicit <c>forceYielding</c> flag alongside an optional <see cref="AsyncContext"/> or
/// <see cref="TaskScheduler"/>. A <see langword="null"/> target must hand back the source untouched;
/// a supplied target must wrap it.
/// </summary>
public class ObserveOnSafeOperatorTests
{
    /// <summary>The value the sources emit.</summary>
    private const int Sentinel = 42;

    /// <summary>Verifies that a null <see cref="AsyncContext"/> makes the forced-yield overload a no-op:
    /// the very same sequence instance comes back rather than a context-switching wrapper.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenObserveOnSafeAsyncContextIsNullWithForcedYielding_ThenReturnsSourceUnchanged()
    {
        var source = SignalAsync.Return(Sentinel);

        var observed = source.ObserveOnSafe((AsyncContext?)null, true);

        await Assert.That(observed).IsSameReferenceAs(source);
        await Assert.That(await observed.FirstAsync()).IsEqualTo(Sentinel);
    }

    /// <summary>Verifies that a supplied <see cref="AsyncContext"/> makes the forced-yield overload wrap the
    /// source in a context-switching sequence that still forwards the value.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenObserveOnSafeAsyncContextSuppliedWithForcedYielding_ThenWrapsAndForwards()
    {
        var source = SignalAsync.Return(Sentinel);

        var observed = source.ObserveOnSafe(AsyncContext.Default, true);

        await Assert.That(ReferenceEquals(observed, source)).IsFalse();
        await Assert.That(await observed.FirstAsync()).IsEqualTo(Sentinel);
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

    /// <summary>Verifies that a supplied <see cref="TaskScheduler"/> makes the forced-yield overload wrap the
    /// source in a context-switching sequence that still forwards the value.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenObserveOnSafeTaskSchedulerSuppliedWithForcedYielding_ThenWrapsAndForwards()
    {
        var source = SignalAsync.Return(Sentinel);

        var observed = source.ObserveOnSafe(TaskScheduler.Default, true);

        await Assert.That(ReferenceEquals(observed, source)).IsFalse();
        await Assert.That(await observed.FirstAsync()).IsEqualTo(Sentinel);
    }
}
