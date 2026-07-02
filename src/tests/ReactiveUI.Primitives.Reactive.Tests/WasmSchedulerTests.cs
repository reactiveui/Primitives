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
        // Park the single event-loop drain on a gate item so the dispose below is guaranteed to happen before the
        // cancelled item is ever run. Without this the immediate drain races the synchronous Dispose on a
        // multi-threaded runtime (it never can on single-threaded WebAssembly, which the type targets).
        TaskCompletionSource gateEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        using ManualResetEventSlim release = new(false);
        _ = WasmScheduler.Default.Schedule(0, (_, _) =>
        {
            _ = gateEntered.TrySetResult();
            _ = release.Wait(WaitTimeout);
            return Disposable.Empty;
        });

        await gateEntered.Task.WaitAsync(WaitTimeout);

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

        // Let the drain proceed: it runs the gate, then the (now disposed) cancelled item, then the marker.
        release.Set();

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

    /// <summary>Verifies disposing a fresh scheduler releases its drain timer and is idempotent.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task DisposeReleasesDrainTimerAndIsIdempotent()
    {
        var scheduler = (WasmScheduler)Activator.CreateInstance(typeof(WasmScheduler), nonPublic: true)!;

        scheduler.Dispose();

        await Assert.That(scheduler.Dispose).ThrowsNothing();
    }

    /// <summary>Verifies disposing a delayed work item twice is idempotent.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task DisposedDelayedItemDisposeIsIdempotent()
    {
        var subscription = WasmScheduler.Default.Schedule(0, TimeSpan.FromMinutes(1), static (_, _) => Disposable.Empty);

        subscription.Dispose();

        await Assert.That(subscription.Dispose).ThrowsNothing();
    }

    /// <summary>Verifies disposing a periodic work item twice is idempotent.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task DisposedPeriodicItemDisposeIsIdempotent()
    {
        var subscription = WasmScheduler.Default.SchedulePeriodic(0, TimeSpan.FromMinutes(1), static state => state);

        subscription.Dispose();

        await Assert.That(subscription.Dispose).ThrowsNothing();
    }

    /// <summary>Verifies an action that cancels its own item before returning still has its returned disposable released.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task SelfCancellingImmediateActionDisposesReturnedDisposable()
    {
        TaskCompletionSource returnedDisposed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var returned = Disposable.Create(() => returnedDisposed.TrySetResult());

        // Holder to capture subscription in closure (works around race condition on net8.0/Linux)
        var holder = new Holder { Subscription = null };
        holder.Subscription = WasmScheduler.Default.Schedule(0, (_, _) =>
        {
            // Cancel while running: the run/cancel handshake must dispose the disposable the action returns next.
            holder.Subscription?.Dispose();
            return returned;
        });

        await returnedDisposed.Task.WaitAsync(WaitTimeout);
        await Assert.That(returnedDisposed.Task.IsCompletedSuccessfully).IsTrue();
    }

    /// <summary>Verifies that scheduling with a null action returns proper exception.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ScheduleWithNullActionThrows()
    {
        await Assert.That(() => WasmScheduler.Default.Schedule(0, null!)).Throws<ArgumentNullException>();
    }

    /// <summary>Verifies that scheduling delayed with a null action returns proper exception.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ScheduleDelayedWithNullActionThrows()
    {
        await Assert.That(() => WasmScheduler.Default.Schedule(0, TimeSpan.FromMilliseconds(100), null!)).Throws<ArgumentNullException>();
    }

    /// <summary>Verifies that scheduling periodic with negative period throws.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task SchedulePeriodicWithNegativePeriodThrows()
    {
        await Assert.That(() => WasmScheduler.Default.SchedulePeriodic(0, TimeSpan.FromMilliseconds(-1), static s => s)).Throws<ArgumentOutOfRangeException>();
    }

    /// <summary>Verifies that scheduling periodic with null action returns proper exception.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task SchedulePeriodicWithNullActionThrows()
    {
        await Assert.That(() => WasmScheduler.Default.SchedulePeriodic(0, TimeSpan.FromMilliseconds(100), null!)).Throws<ArgumentNullException>();
    }

    /// <summary>Simple holder to work around race condition in closure.</summary>
    private sealed class Holder
    {
        /// <summary>Gets or sets the subscription held by this holder.</summary>
        public IDisposable? Subscription { get; set; }
    }
}
