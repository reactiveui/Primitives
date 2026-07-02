// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Disposables;
using ReactiveUI.Primitives.Reactive.Concurrency;

namespace ReactiveUI.Primitives.Reactive.Tests;

/// <summary>Tests for <see cref="WasmScheduler"/>.</summary>
public sealed class WasmSchedulerTests
{
    /// <summary>State payload used to verify state threading.</summary>
    private const int StatePayload = 42;

    /// <summary>Minimum periodic ticks a test observes before disposing.</summary>
    private const int MinimumTicks = 2;

    /// <summary>Expected values produced by an immediate burst, used to verify FIFO order.</summary>
    private static readonly int[] ExpectedBurst = [1, 2, 3];

    /// <summary>Longest a test waits for scheduled work before failing.</summary>
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Verifies the shared instance is a singleton.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task DefaultReturnsSingleton() =>
        await Assert.That(WasmScheduler.Default).IsSameReferenceAs(WasmScheduler.Default);

    /// <summary>Verifies scheduling rejects null actions.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ScheduleRejectsNullAction()
    {
        var scheduler = WasmScheduler.Default;

        await Assert.That(() => scheduler.Schedule(0, null!)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => scheduler.Schedule(0, TimeSpan.FromMilliseconds(1), null!))
            .ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => scheduler.SchedulePeriodic(0, TimeSpan.FromMilliseconds(1), null!))
            .ThrowsExactly<ArgumentNullException>();
    }

    /// <summary>Verifies periodic scheduling rejects a negative period.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task SchedulePeriodicRejectsNegativePeriod() =>
        await Assert.That(() => WasmScheduler.Default.SchedulePeriodic(0, TimeSpan.FromMilliseconds(-1), static s => s))
            .ThrowsExactly<ArgumentOutOfRangeException>();

    /// <summary>Verifies immediate work executes with the scheduler and state passed through.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ImmediateScheduleExecutes()
    {
        TaskCompletionSource<int> executed = new(TaskCreationOptions.RunContinuationsAsynchronously);

        _ = WasmScheduler.Default.Schedule(StatePayload, (scheduler, state) =>
        {
            _ = scheduler;
            _ = executed.TrySetResult(state);
            return Disposable.Empty;
        });

        await Assert.That(await executed.Task.WaitAsync(WaitTimeout)).IsEqualTo(StatePayload);
    }

    /// <summary>Verifies a burst of immediate work executes in FIFO order.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ImmediateBurstExecutesInOrder()
    {
        TaskCompletionSource<bool> done = new(TaskCreationOptions.RunContinuationsAsynchronously);
        List<int> values = [];

        foreach (var value in ExpectedBurst)
        {
            _ = WasmScheduler.Default.Schedule(value, (scheduler, state) =>
            {
                _ = scheduler;
                values.Add(state);
                if (values.Count == ExpectedBurst.Length)
                {
                    _ = done.TrySetResult(true);
                }

                return Disposable.Empty;
            });
        }

        _ = await done.Task.WaitAsync(WaitTimeout);
        await Assert.That(values).IsEquivalentTo(ExpectedBurst, EqualityComparer<int>.Default);
    }

    /// <summary>Verifies a disposed immediate work item never runs while later work still does.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task DisposedImmediateItemIsSkipped()
    {
        TaskCompletionSource<bool> markerRan = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var cancelledRan = false;

        var cancelled = WasmScheduler.Default.Schedule(0, (_, _) =>
        {
            cancelledRan = true;
            return Disposable.Empty;
        });
        cancelled.Dispose();
        _ = WasmScheduler.Default.Schedule(0, (_, _) =>
        {
            _ = markerRan.TrySetResult(true);
            return Disposable.Empty;
        });

        _ = await markerRan.Task.WaitAsync(WaitTimeout);
        await Assert.That(cancelledRan).IsFalse();
    }

    /// <summary>Verifies delayed work executes and zero due time uses the immediate path.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task DelayedScheduleExecutes()
    {
        TaskCompletionSource<bool> delayed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource<bool> immediate = new(TaskCreationOptions.RunContinuationsAsynchronously);

        _ = WasmScheduler.Default.Schedule(0, TimeSpan.FromMilliseconds(50), (_, _) =>
        {
            _ = delayed.TrySetResult(true);
            return Disposable.Empty;
        });
        _ = WasmScheduler.Default.Schedule(0, TimeSpan.Zero, (_, _) =>
        {
            _ = immediate.TrySetResult(true);
            return Disposable.Empty;
        });

        await Assert.That(await delayed.Task.WaitAsync(WaitTimeout)).IsTrue();
        await Assert.That(await immediate.Task.WaitAsync(WaitTimeout)).IsTrue();
    }

    /// <summary>Verifies disposing a delayed work item before it is due cancels it.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task DisposedDelayedItemDoesNotRun()
    {
        var ran = false;

        var subscription = WasmScheduler.Default.Schedule(0, TimeSpan.FromMilliseconds(100), (_, _) =>
        {
            ran = true;
            return Disposable.Empty;
        });
        subscription.Dispose();

        await Task.Delay(TimeSpan.FromMilliseconds(250));
        await Assert.That(ran).IsFalse();
    }

    /// <summary>Verifies periodic work ticks repeatedly, threads state, and stops on dispose.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task SchedulePeriodicTicksAndStopsOnDispose()
    {
        TaskCompletionSource<bool> reachedTwo = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var count = 0;

        var subscription = WasmScheduler.Default.SchedulePeriodic(0, TimeSpan.FromMilliseconds(10), state =>
        {
            count = state + 1;
            if (count >= MinimumTicks)
            {
                _ = reachedTwo.TrySetResult(true);
            }

            return count;
        });

        _ = await reachedTwo.Task.WaitAsync(WaitTimeout);
        subscription.Dispose();
        var snapshot = Volatile.Read(ref count);

        await Task.Delay(TimeSpan.FromMilliseconds(100));
        await Assert.That(Volatile.Read(ref count)).IsEqualTo(snapshot);
    }

    /// <summary>Verifies a zero period is clamped instead of rejected and still ticks.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task SchedulePeriodicClampsZeroPeriod()
    {
        TaskCompletionSource<bool> ticked = new(TaskCreationOptions.RunContinuationsAsynchronously);

        var subscription = WasmScheduler.Default.SchedulePeriodic(0, TimeSpan.Zero, state =>
        {
            _ = ticked.TrySetResult(true);
            return state;
        });

        await Assert.That(await ticked.Task.WaitAsync(WaitTimeout)).IsTrue();
        subscription.Dispose();
    }
}
