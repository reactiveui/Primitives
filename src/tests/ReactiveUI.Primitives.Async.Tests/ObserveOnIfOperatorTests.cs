// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>
/// Tests for the forced-yield overloads of the <c>ObserveOnIf</c> parity helper — the ones that take an
/// explicit <c>forceYielding</c> flag. A false condition must hand the source back untouched; a true
/// condition must wrap it in a context-switching sequence.
/// </summary>
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

    /// <summary>Verifies that a true condition makes the forced-yield <see cref="AsyncContext"/> overload wrap
    /// the source in a context-switching sequence that still forwards the value.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenObserveOnIfAsyncContextConditionTrueWithForcedYielding_ThenWrapsAndForwards()
    {
        var source = SignalAsync.Return(Sentinel);

        var observed = source.ObserveOnIf(true, AsyncContext.Default, true);

        await Assert.That(ReferenceEquals(observed, source)).IsFalse();
        await Assert.That(await observed.FirstAsync()).IsEqualTo(Sentinel);
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

    /// <summary>Verifies that a true condition makes the forced-yield <see cref="TaskScheduler"/> overload wrap
    /// the source in a context-switching sequence that still forwards the value.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenObserveOnIfTaskSchedulerConditionTrueWithForcedYielding_ThenWrapsAndForwards()
    {
        var source = SignalAsync.Return(Sentinel);

        var observed = source.ObserveOnIf(true, TaskScheduler.Default, true);

        await Assert.That(ReferenceEquals(observed, source)).IsFalse();
        await Assert.That(await observed.FirstAsync()).IsEqualTo(Sentinel);
    }
}
