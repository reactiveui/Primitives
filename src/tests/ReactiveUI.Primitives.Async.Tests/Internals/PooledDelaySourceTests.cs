// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Internals;

namespace ReactiveUI.Primitives.Async.Tests.Internals;

/// <summary>Tests for <see cref="PooledDelaySource"/>, the pooled <c>IValueTaskSource</c> backing
/// <c>DelayAsync</c> on non-System <see cref="TimeProvider"/> implementations.</summary>
public class PooledDelaySourceTests
{
    /// <summary>The delay used by the happy-path test.</summary>
    private static readonly TimeSpan ShortDelay = TimeSpan.FromMilliseconds(20);

    /// <summary>Verifies that a pre-cancelled token fails the source immediately with
    /// <see cref="OperationCanceledException"/> — the BeginAsync early-return path.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPreCancelledToken_ThenFailsWithOperationCanceled()
    {
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();
        var source = PooledDelaySource.Rent();

        var task = source.BeginAsync(ShortDelay, new NonSystemTimeProvider(), cts.Token);

        var ex = await Assert.ThrowsAsync<OperationCanceledException>(async () => await task);
        await Assert.That(ex).IsNotNull();
    }

    /// <summary>Verifies that the timer fires on a normal token and the source completes successfully.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTimerFires_ThenSourceCompletes()
    {
        var source = PooledDelaySource.Rent();

        var start = TimeProvider.System.GetTimestamp();
        await source.BeginAsync(ShortDelay, new NonSystemTimeProvider(), CancellationToken.None);
        var elapsed = TimeProvider.System.GetElapsedTime(start);

        // Verify the delay actually happened — the source's timer-fired path completed it.
        await Assert.That(elapsed).IsGreaterThanOrEqualTo(TimeSpan.FromMilliseconds(5));
    }

    /// <summary>Verifies that a token cancelled mid-flight propagates an <see cref="OperationCanceledException"/>.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTokenCancelledMidFlight_ThenFailsWithOperationCanceled()
    {
        using var cts = new CancellationTokenSource();
        var source = PooledDelaySource.Rent();
        var task = source.BeginAsync(TimeSpan.FromSeconds(10), new NonSystemTimeProvider(), cts.Token);

        await cts.CancelAsync();

        var ex = await Assert.ThrowsAsync<OperationCanceledException>(async () => await task);
        await Assert.That(ex).IsNotNull();
    }

    /// <summary>Custom <see cref="TimeProvider"/> that is NOT <see cref="TimeProvider.System"/>; ensures BeginAsync
    /// uses the non-System path. Delegates timer creation to System under the hood.</summary>
    private sealed class NonSystemTimeProvider : TimeProvider
    {
        /// <inheritdoc/>
        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period) =>
            System.CreateTimer(callback, state, dueTime, period);
    }
}
