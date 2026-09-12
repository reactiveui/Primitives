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

    /// <summary>Periodic ticks a test drives before disposing; more than one, so threaded state is observable.</summary>
    private const int PeriodicTickCount = 2;

    /// <summary>Items queued ahead of a single drain pass in the exactly-once dispatch test.</summary>
    private const int BatchItemCount = 2000;

    /// <summary>Sentinel <see cref="Array.FindIndex{T}(T[], Predicate{T})"/> returns when no element matches.</summary>
    private const int NoMatch = -1;

    /// <summary>Expected values produced by an immediate burst, used to verify FIFO order.</summary>
    private static readonly int[] ExpectedBurst = [1, 2, 3];

    /// <summary>Due time of a work item that is expected to run after its delay elapses.</summary>
    private static readonly TimeSpan DelayedDueTime = TimeSpan.FromMilliseconds(50);

    /// <summary>A positive due time or period, so a null action is the only invalid argument under test.</summary>
    private static readonly TimeSpan ValidInterval = TimeSpan.FromMilliseconds(100);

    /// <summary>A due time no test waits out, so the only run a delayed item sees is the one the test drives.</summary>
    private static readonly TimeSpan UnreachableDueTime = TimeSpan.FromHours(1);

    /// <summary>A period no test waits out, so the only tick a periodic item sees is the one the test drives.</summary>
    private static readonly TimeSpan UnreachablePeriod = TimeSpan.FromHours(1);

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
        await Assert.That(() => scheduler.Schedule(0, ValidInterval, null!))
            .ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => scheduler.SchedulePeriodic(0, ValidInterval, null!))
            .ThrowsExactly<ArgumentNullException>();
    }

    /// <summary>Verifies periodic scheduling rejects a negative period.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task SchedulePeriodicRejectsNegativePeriod() =>
        await Assert
            .That(static () =>
                WasmScheduler.Default.SchedulePeriodic(0, TimeSpan.FromMilliseconds(-1), static s => s))
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

        await Assert.That(await executed.Task).IsEqualTo(StatePayload);
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

        _ = await done.Task;
        await Assert.That(values).IsEquivalentTo(ExpectedBurst, EqualityComparer<int>.Default);
    }

    /// <summary>Verifies an item disposed while it waits in the ready queue is skipped while later work still runs.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task DisposedImmediateItemIsSkipped()
    {
        using var scheduler = CreateIsolatedScheduler();
        var cancelledRan = false;
        var markerRan = false;
        WasmScheduler.StatefulWorkItem<int> cancelled = new(scheduler, 0, (_, _) =>
        {
            cancelledRan = true;
            return Disposable.Empty;
        });
        WasmScheduler.StatefulWorkItem<int> marker = new(scheduler, 0, (_, _) =>
        {
            markerRan = true;
            return Disposable.Empty;
        });

        // Queue both without arming a drain, so the dispose lands while a drain has reached neither item.
        scheduler.QueueReady(cancelled);
        scheduler.QueueReady(marker);
        cancelled.Dispose();

        scheduler.RunReadyBatch();

        await Assert.That(cancelledRan).IsFalse();
        await Assert.That(markerRan).IsTrue();
    }

    /// <summary>Verifies delayed work executes and zero due time uses the immediate path.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task DelayedScheduleExecutes()
    {
        TaskCompletionSource<bool> delayed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource<bool> immediate = new(TaskCreationOptions.RunContinuationsAsynchronously);

        _ = WasmScheduler.Default.Schedule(0, DelayedDueTime, (_, _) =>
        {
            _ = delayed.TrySetResult(true);
            return Disposable.Empty;
        });
        _ = WasmScheduler.Default.Schedule(0, TimeSpan.Zero, (_, _) =>
        {
            _ = immediate.TrySetResult(true);
            return Disposable.Empty;
        });

        await Assert.That(await delayed.Task).IsTrue();
        await Assert.That(await immediate.Task).IsTrue();
    }

    /// <summary>Verifies disposing a delayed work item before it is due cancels it, so a late callback is dropped.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task DisposedDelayedItemDoesNotRun()
    {
        var ran = false;
        var subscription = (WasmScheduler.StatefulWorkItem<int>)WasmScheduler.Default.Schedule(
            0,
            UnreachableDueTime,
            (_, _) =>
            {
                ran = true;
                return Disposable.Empty;
            });

        subscription.Dispose();

        // The due time never elapses on its own, so this is the run a timer callback already in flight would deliver.
        subscription.Run();

        await Assert.That(subscription.IsDisposed).IsTrue();
        await Assert.That(ran).IsFalse();
    }

    /// <summary>Verifies periodic work ticks repeatedly, threads state, and stops on dispose.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task SchedulePeriodicTicksAndStopsOnDispose()
    {
        var count = 0;
        var subscription = (WasmScheduler.PeriodicWorkItem<int>)WasmScheduler.Default.SchedulePeriodic(
            0,
            UnreachablePeriod,
            state =>
            {
                count = state + 1;
                return count;
            });

        for (var tick = 0; tick < PeriodicTickCount; tick++)
        {
            subscription.Tick();
        }

        // The count only reaches the tick total if each tick received the state the previous one returned.
        await Assert.That(count).IsEqualTo(PeriodicTickCount);

        subscription.Dispose();
        subscription.Tick();

        await Assert.That(count).IsEqualTo(PeriodicTickCount);
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

        await Assert.That(await ticked.Task).IsTrue();
        subscription.Dispose();
    }

    /// <summary>Verifies disposing a fresh scheduler releases its drain timer and is idempotent.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task DisposeReleasesDrainTimerAndIsIdempotent()
    {
        var scheduler = CreateIsolatedScheduler();

        scheduler.Dispose();

        await Assert.That(scheduler.Dispose).ThrowsNothing();
    }

    /// <summary>Verifies disposing a delayed work item twice is idempotent.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task DisposedDelayedItemDisposeIsIdempotent()
    {
        var subscription =
            WasmScheduler.Default.Schedule(0, UnreachableDueTime, static (_, _) => Disposable.Empty);

        subscription.Dispose();

        await Assert.That(subscription.Dispose).ThrowsNothing();
    }

    /// <summary>Verifies disposing a periodic work item twice is idempotent.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task DisposedPeriodicItemDisposeIsIdempotent()
    {
        var subscription = WasmScheduler.Default.SchedulePeriodic(0, UnreachablePeriod, static state => state);

        subscription.Dispose();

        await Assert.That(subscription.Dispose).ThrowsNothing();
    }

    /// <summary>Verifies an action that cancels its own item before returning still has its returned disposable released.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task SelfCancellingImmediateActionDisposesReturnedDisposable()
    {
        using var scheduler = CreateIsolatedScheduler();
        BooleanDisposable returned = new();

        // Build the item and publish the handle it cancels through before anything can run it. Scheduling normally
        // arms the drain inside Schedule and only then returns the handle, so the action is free to run first and
        // find nothing to cancel; enqueueing by hand is the same path with that window closed.
        WasmScheduler.StatefulWorkItem<int>? subscription = null;
        WasmScheduler.StatefulWorkItem<int> item = new(
            scheduler,
            StatePayload,
            (_, _) =>
            {
                // Cancel while running: the run/cancel handshake must dispose the disposable the action returns next.
                subscription!.Dispose();
                return returned;
            });

        subscription = item;
        scheduler.QueueReady(item);
        scheduler.RunReadyBatch();

        await Assert.That(returned.IsDisposed).IsTrue();
    }

    /// <summary>Verifies that scheduling with a null action returns proper exception.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ScheduleWithNullActionThrows() =>
        await Assert.That(static () => WasmScheduler.Default.Schedule(0, null!)).Throws<ArgumentNullException>();

    /// <summary>Verifies that scheduling delayed with a null action returns proper exception.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ScheduleDelayedWithNullActionThrows() => await Assert
        .That(static () => WasmScheduler.Default.Schedule(0, ValidInterval, null!))
        .Throws<ArgumentNullException>();

    /// <summary>Verifies that scheduling periodic with negative period throws.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task SchedulePeriodicWithNegativePeriodThrows() => await Assert
        .That(static () => WasmScheduler.Default.SchedulePeriodic(0, TimeSpan.FromMilliseconds(-1), static s => s))
        .Throws<ArgumentOutOfRangeException>();

    /// <summary>Verifies that scheduling periodic with null action returns proper exception.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task SchedulePeriodicWithNullActionThrows() => await Assert
        .That(static () => WasmScheduler.Default.SchedulePeriodic(0, ValidInterval, null!))
        .Throws<ArgumentNullException>();

    /// <summary>
    /// Verifies a disposed scheduler rejects new work rather than queueing work it can never drain. The drain timer
    /// is released on disposal, so an accepted item would sit in the ready queue forever behind a latch the failed
    /// timer post left set. Every scheduling overload fails fast instead, and none of the actions run.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ScheduleAfterDisposeThrowsObjectDisposedException()
    {
        var scheduler = CreateIsolatedScheduler();
        scheduler.Dispose();
        var ran = 0;

        await Assert.That(() => scheduler.Schedule(0, (_, _) =>
        {
            ran++;
            return Disposable.Empty;
        })).ThrowsExactly<ObjectDisposedException>();

        await Assert.That(() => scheduler.Schedule(0, UnreachableDueTime, (_, _) =>
        {
            ran++;
            return Disposable.Empty;
        })).ThrowsExactly<ObjectDisposedException>();

        await Assert.That(() => scheduler.SchedulePeriodic(0, UnreachablePeriod, state =>
        {
            ran++;
            return state;
        })).ThrowsExactly<ObjectDisposedException>();

        // Each overload threw before it built an item, so there is nothing left that could run the actions.
        await Assert.That(ran).IsEqualTo(0);
    }

    /// <summary>
    /// Verifies disposing the scheduler cancels work still waiting in the ready queue while a drain is in flight:
    /// the queued item is released, not left for the resuming drain to run against a scheduler that is already gone.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task DisposeCancelsWorkTheInFlightDrainHasNotReachedYet()
    {
        var scheduler = CreateIsolatedScheduler();
        var queuedRan = 0;
        WasmScheduler.StatefulWorkItem<int> queued = new(scheduler, 0, (_, _) =>
        {
            queuedRan++;
            return Disposable.Empty;
        });

        // Queue without arming a drain, so the item is provably still waiting when the disposal runs.
        scheduler.QueueReady(queued);

        scheduler.Dispose();

        // The parked drain resuming: the item it never reached is gone from the queue the disposal released.
        scheduler.RunReadyBatch();

        await Assert.That(queuedRan).IsEqualTo(0);
        await Assert.That(queued.IsDisposed).IsTrue();
        await Assert.That(queued.Dispose).ThrowsNothing();
    }

    /// <summary>
    /// Verifies a one-shot timer handed to a work item that was already cancelled is released instead of left armed.
    /// A delayed schedule builds the item first and attaches its timer afterwards, so a dispose landing in that window
    /// must not strand a timer that would still fire against an item nobody can cancel any more.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task AttachTimerReleasesATimerGivenToAnAlreadyCancelledItem()
    {
        using var scheduler = CreateIsolatedScheduler();
        var ran = 0;
        WasmScheduler.StatefulWorkItem<int> item = new(
            scheduler,
            StatePayload,
            (_, _) =>
            {
                ran++;
                return Disposable.Empty;
            });

        // Cancel before the delayed schedule reaches its AttachTimer call.
        item.Dispose();

        // Stands in for the one-shot timer, recording the release the item owes it.
        BooleanDisposable timer = new();
        item.AttachTimer(timer);

        await Assert.That(timer.IsDisposed).IsTrue();
        await Assert.That(ran).IsEqualTo(0);
    }

    /// <summary>
    /// Verifies a periodic tick that loses the race to disposal drops the tick instead of running the action. A timer
    /// callback the runtime had already dispatched when <see cref="IDisposable.Dispose"/> won still lands, and must
    /// find the item cancelled rather than mutate state the disposal has already torn down.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task PeriodicTickThatLosesTheRaceToDisposeDoesNotRunTheAction()
    {
        var ticks = 0;
        var item = WasmScheduler.PeriodicWorkItem<int>.Start(
            StatePayload,
            UnreachablePeriod,
            state =>
            {
                ticks++;
                return state;
            });

        item.Dispose();

        // The period never elapses on its own, so this is the tick a callback already in flight would have delivered.
        item.Tick();

        await Assert.That(ticks).IsEqualTo(0);
    }

    /// <summary>
    /// Verifies an enqueue that loses the race to disposal releases the item it just queued. The scheduler's disposed
    /// check happens before the item joins the ready queue, so a disposal that drains the queue in between would
    /// otherwise strand the item behind a drain timer that can never fire again.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task EnqueueThatLosesTheRaceToDisposeReleasesTheItemItQueued()
    {
        var scheduler = CreateIsolatedScheduler();
        var ran = 0;
        WasmScheduler.StatefulWorkItem<int> item = new(
            scheduler,
            StatePayload,
            (_, _) =>
            {
                ran++;
                return Disposable.Empty;
            });

        scheduler.Dispose();

        // The enqueue that was already past Schedule's disposed check when the disposal drained the ready queue.
        scheduler.Enqueue(item);

        await Assert.That(item.IsDisposed).IsTrue();

        // A drain that arrives after the enqueue finds nothing to run, so the released item stays unrun.
        scheduler.RunReadyBatch();

        await Assert.That(ran).IsEqualTo(0);
    }

    /// <summary>
    /// Verifies a drain pass dispatches every queued item exactly once: an item the batch skips would be stranded,
    /// and one it claims twice would run its action twice.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task DrainDispatchesEveryQueuedItemExactlyOnce()
    {
        using var scheduler = CreateIsolatedScheduler();
        var dispatches = new int[BatchItemCount];

        for (var index = 0; index < BatchItemCount; index++)
        {
            scheduler.QueueReady(new WasmScheduler.StatefulWorkItem<int>(scheduler, index, (_, state) =>
            {
                dispatches[state]++;
                return Disposable.Empty;
            }));
        }

        scheduler.RunReadyBatch();

        // Every slot holding exactly one dispatch rules out both a skipped and a doubled item.
        await Assert.That(Array.FindIndex(dispatches, static count => count != 1)).IsEqualTo(NoMatch);
    }

    /// <summary>
    /// Creates a scheduler that owns its own drain timer and ready queue, so a test can dispose it without
    /// disturbing the shared singleton every other test schedules through.
    /// </summary>
    /// <returns>The isolated scheduler.</returns>
    private static WasmScheduler CreateIsolatedScheduler() => new();
}
