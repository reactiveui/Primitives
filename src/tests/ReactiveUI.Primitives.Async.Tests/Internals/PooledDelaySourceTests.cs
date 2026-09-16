// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Tests.Internals;

/// <summary>Tests for <see cref = "PooledDelaySource"/>, the pooled <c>IValueTaskSource</c> backing <c>DelayAsync</c> on non-System <see cref = "TimeProvider"/> implementations.</summary>
public class PooledDelaySourceTests
{
    /// <summary>The delay used by the happy-path test.</summary>
    private static readonly TimeSpan ShortDelay = TimeSpan.FromMilliseconds(20);

    /// <summary>A delay long enough that it can only end through cancellation.</summary>
    private static readonly TimeSpan DelayOutlivingTheTest = TimeSpan.FromSeconds(10);

    /// <summary>Verifies that a pre-cancelled token fails the source immediately with <see cref = "OperationCanceledException"/> - the BeginAsync early-return path.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPreCancelledToken_ThenFailsWithOperationCanceled()
    {
        using CancellationTokenSource cts = new();
        await cts.CancelAsync();
        var source = PooledDelaySource.Rent();
        var task = source.BeginAsync(ShortDelay, new ManualTimeProvider(), cts.Token);
        var ex = await Assert.That(async () => await task).ThrowsExactly<OperationCanceledException>();
        await Assert.That(ex).IsNotNull();
    }

    /// <summary>Verifies that the timer fires on a normal token and the source completes successfully.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTimerFires_ThenSourceCompletes()
    {
        var source = PooledDelaySource.Rent();
        ManualTimeProvider time = new();
        var pending = source.BeginAsync(ShortDelay, time, CancellationToken.None);
        var timer = await time.NextTimerAsync();
        await Assert.That(pending.IsCompleted).IsFalse();
        await Assert.That(timer.DueTime).IsEqualTo(ShortDelay);
        timer.Fire();
        await pending;
    }

    /// <summary>A callback fired during timer creation completes before cancellation registration.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WhenTimerFiresDuringCreation_ThenCompletesSynchronously()
    {
        using CancellationTokenSource cancellation = new();
        var source = PooledDelaySource.Rent();
        var pending = source.BeginAsync(ShortDelay, new ImmediateTimeProvider(), cancellation.Token);
        await Assert.That(pending.IsCompletedSuccessfully).IsTrue();
        await cancellation.CancelAsync();
        await pending;
    }

    /// <summary>Verifies that a token cancelled mid-flight propagates an <see cref = "OperationCanceledException"/>.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTokenCancelledMidFlight_ThenFailsWithOperationCanceled()
    {
        using CancellationTokenSource cts = new();
        var source = PooledDelaySource.Rent();
        var task = source.BeginAsync(DelayOutlivingTheTest, new ManualTimeProvider(), cts.Token);
        await cts.CancelAsync();
        var ex = await Assert.That(async () => await task).ThrowsExactly<OperationCanceledException>();
        await Assert.That(ex).IsNotNull();
    }

    /// <summary>The first terminal event owns the result even when the second event runs before consumption.</summary>
    /// <param name="cancelFirst">Whether cancellation precedes the timer callback.</param>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task WhenTimerAndCancellationBothFire_ThenFirstEventWins(bool cancelFirst)
    {
        ManualTimeProvider time = new();
        using CancellationTokenSource cancellation = new();
        var source = PooledDelaySource.Rent();
        var pending = source.BeginAsync(ShortDelay, time, cancellation.Token);
        var timer = await time.NextTimerAsync();
        if (cancelFirst)
        {
            await cancellation.CancelAsync();
            timer.Fire();
            await Assert.That(async () => await pending).ThrowsExactly<OperationCanceledException>();
        }
        else
        {
            timer.Fire();
            await cancellation.CancelAsync();
            await pending;
        }
    }

    /// <summary>A provider that invokes its callback inside timer creation.</summary>
    private sealed class ImmediateTimeProvider : TimeProvider
    {
        /// <inheritdoc/>
        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            callback(state);
            return new ManualTimeProvider.ManualTimer(callback, state, dueTime);
        }
    }
}
