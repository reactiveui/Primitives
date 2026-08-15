// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>
/// Tests for the <c>Expire</c> operator — the inter-element deadline that completes the sequence with a
/// <see cref="TimeoutException"/>. Covers the deadline firing, and the tick of a deadline that was rearmed
/// by a source value arriving after the sequence had already expired.
/// </summary>
public class ExpireOperatorTests
{
    /// <summary>Seconds a test waits for a completion to arrive.</summary>
    private const int WaitTimeoutSeconds = 5;

    /// <summary>The inter-element deadline used by the tests.</summary>
    private static readonly TimeSpan DueTime = TimeSpan.FromMilliseconds(20);

    /// <summary>Maximum time a test waits for a completion to arrive.</summary>
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(WaitTimeoutSeconds);

    /// <summary>How long a test waits to prove a swallowed timer tick never produces a second completion.</summary>
    private static readonly TimeSpan SecondTickSettleWindow = TimeSpan.FromMilliseconds(250);

    /// <summary>Verifies that a source which never produces a value trips the deadline and the sequence completes with a <see cref="TimeoutException"/>.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenNoValueArrivesWithinTheDeadline_ThenCompletesWithTimeoutException()
    {
        TaskCompletionSource<Result> completed = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await SignalAsync.Never<int>().Expire(DueTime).SubscribeAsync(
            static (_, _) => default,
            null,
            result =>
            {
                _ = completed.TrySetResult(result);
                return default;
            });

        var result = await completed.Task.WaitAsync(WaitTimeout);

        await Assert.That(result.IsFailure).IsTrue();
        await Assert.That(result.Exception is TimeoutException).IsTrue();
    }

    /// <summary>
    /// Verifies that the deadline tick is swallowed once the sequence has already expired. A source value that
    /// arrives after the timeout rearms the deadline, so the timer fires a second time; that tick must not turn
    /// into a second <see cref="TimeoutException"/> for a sequence that is already finished.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTheRearmedDeadlineTicksAfterExpiry_ThenNoSecondCompletionIsSent()
    {
        RearmableTimeProvider timeProvider = new();
        var source = Signal.Create<int>();
        List<Result> completions = [];
        TaskCompletionSource firstCompletion = new(TaskCreationOptions.RunContinuationsAsynchronously);

        SignalAsyncExtensions.TimeoutSignal<int> expiring = new(source.Values, DueTime, timeProvider);

        await using var sub = await expiring.SubscribeAsync(
            static (_, _) => default,
            null,
            result =>
            {
                completions.Add(result);
                IgnoredResult.Of(firstCompletion.TrySetResult());
                return default;
            });

        timeProvider.FireAll();
        await firstCompletion.Task.WaitAsync(WaitTimeout);

        await Assert.That(completions).Count().IsEqualTo(1);
        await Assert.That(completions[0].IsFailure).IsTrue();
        await Assert.That(completions[0].Exception is TimeoutException).IsTrue();

        // The late value rearms the deadline; the rearmed tick has to be ignored.
        await source.OnNextAsync(1, CancellationToken.None);
        timeProvider.FireAll();

        var secondArrived = await AsyncTestHelpers.WaitForConditionAsync(
            () => completions.Count > 1,
            SecondTickSettleWindow);

        await Assert.That(secondArrived).IsFalse();
        await Assert.That(completions).Count().IsEqualTo(1);
    }

    /// <summary>
    /// A <see cref="TimeProvider"/> whose timers stay armed: <see cref="FireAll"/> invokes every live timer's
    /// callback each time it is called, so a test can replay the tick of a deadline that the operator rearmed.
    /// </summary>
    private sealed class RearmableTimeProvider : TimeProvider
    {
        /// <summary>Protects timer collection access.</summary>
        private readonly Lock _gate = new();

        /// <summary>The timers created by this provider.</summary>
        private readonly List<RearmableTimer> _timers = [];

        /// <summary>Creates a timer that only fires when the test says so.</summary>
        /// <param name="callback">The callback to invoke when the timer is fired.</param>
        /// <param name="state">The state object passed to the callback.</param>
        /// <param name="dueTime">The initial delay (ignored; the test drives the tick).</param>
        /// <param name="period">The interval (ignored; the test drives the tick).</param>
        /// <returns>A manually fired <see cref="ITimer"/> instance.</returns>
        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            _ = dueTime;
            _ = period;
            RearmableTimer timer = new(callback, state);
            lock (_gate)
            {
                _timers.Add(timer);
            }

            return timer;
        }

        /// <summary>Fires every timer this provider has handed out and has not seen disposed.</summary>
        internal void FireAll()
        {
            RearmableTimer[] timers;
            lock (_gate)
            {
                timers = [.. _timers];
            }

            foreach (var timer in timers)
            {
                timer.Fire();
            }
        }

        /// <summary>A timer that can be fired any number of times until it is disposed.</summary>
        /// <param name="callback">The callback to invoke.</param>
        /// <param name="state">The state object passed to the callback.</param>
        private sealed class RearmableTimer(TimerCallback callback, object? state) : ITimer
        {
            /// <summary>Non-zero once the timer has been disposed.</summary>
            private int _disposed;

            /// <summary>Reports whether the timer is still live; the new deadline is irrelevant to the test.</summary>
            /// <param name="dueTime">The due time (ignored).</param>
            /// <param name="period">The period (ignored).</param>
            /// <returns><see langword="true"/> when the timer is still active.</returns>
            public bool Change(TimeSpan dueTime, TimeSpan period)
            {
                _ = dueTime;
                _ = period;
                return Volatile.Read(ref _disposed) == 0;
            }

            /// <summary>Marks the timer as disposed.</summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Dispose() => Interlocked.Exchange(ref _disposed, 1);

            /// <summary>Marks the timer as disposed.</summary>
            /// <returns>A completed <see cref="ValueTask"/>.</returns>
            public ValueTask DisposeAsync()
            {
                Dispose();
                return default;
            }

            /// <summary>Invokes the callback unless the timer has been disposed.</summary>
            internal void Fire()
            {
                if (Volatile.Read(ref _disposed) != 0)
                {
                    return;
                }

                callback(state);
            }
        }
    }
}
