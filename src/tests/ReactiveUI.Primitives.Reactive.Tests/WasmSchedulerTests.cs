// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Disposables;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Reactive.Concurrency;

namespace ReactiveUI.Primitives.Reactive.Tests;

/// <summary>Tests for <see cref="WasmScheduler"/>.</summary>
public sealed class WasmSchedulerTests
{
    /// <summary>State payload threaded through the scheduled action.</summary>
    private const int StatePayload = 42;

    /// <summary>Periodic ticks a test drives before disposing; more than one, so threaded state is observable.</summary>
    private const int PeriodicTickCount = 2;

    /// <summary>Items queued ahead of a single drain pass in the exactly-once dispatch test.</summary>
    private const int BatchItemCount = 2000;

    /// <summary>Sentinel <see cref="Array.FindIndex{T}(T[], Predicate{T})"/> returns when no element matches.</summary>
    private const int NoMatch = -1;

    /// <summary>The values an immediate burst produces, in the FIFO order asserted.</summary>
    private static readonly int[] ExpectedBurst = [1, 2, 3];

    /// <summary>Due time of a work item that is expected to run after its delay elapses.</summary>
    private static readonly TimeSpan DelayedDueTime = TimeSpan.FromMilliseconds(50);

    /// <summary>A positive due time or period, so a null action is the only invalid argument under test.</summary>
    private static readonly TimeSpan ValidInterval = TimeSpan.FromMilliseconds(100);

    /// <summary>Due time for manually dispatched callbacks.</summary>
    private static readonly TimeSpan UnreachableDueTime = TimeSpan.FromHours(1);

    /// <summary>Period for manually dispatched callbacks.</summary>
    private static readonly TimeSpan UnreachablePeriod = TimeSpan.FromHours(1);

    /// <summary>Verifies the shared instance is a singleton.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task DefaultReturnsSingleton() =>
        await Assert.That(WasmScheduler.Default).IsSameReferenceAs(WasmScheduler.Default);

    /// <summary>Verifies the provider constructor rejects a missing provider.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorRejectsANullTimeProvider() =>
        await Assert.That(static () => new WasmScheduler(null!)).ThrowsExactly<ArgumentNullException>();

    /// <summary>Verifies the scheduler reports the provider's current time.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task NowFollowsTheTimeProvider()
    {
        ManualTimeProvider timeProvider = new();
        using WasmScheduler scheduler = new(timeProvider);

        await Assert.That(scheduler.Now).IsEqualTo(DateTimeOffset.UnixEpoch);

        timeProvider.UtcNow = DateTimeOffset.UnixEpoch + ValidInterval;

        await Assert.That(scheduler.Now).IsEqualTo(DateTimeOffset.UnixEpoch + ValidInterval);
    }

    /// <summary>Verifies scheduling rejects null actions.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ScheduleRejectsNullAction()
    {
        ManualTimeProvider timeProvider = new();
        using WasmScheduler scheduler = new(timeProvider);

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
        ManualTimeProvider timeProvider = new();
        using WasmScheduler scheduler = new(timeProvider);
        TaskCompletionSource<int> executed = new(TaskCreationOptions.RunContinuationsAsynchronously);

        _ = scheduler.Schedule(StatePayload, (scheduler, state) =>
        {
            _ = scheduler;
            _ = executed.TrySetResult(state);
            return Disposable.Empty;
        });
        timeProvider.FireAll();

        await Assert.That(await executed.Task).IsEqualTo(StatePayload);
    }

    /// <summary>Verifies a burst of immediate work executes in FIFO order.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ImmediateBurstExecutesInOrder()
    {
        ManualTimeProvider timeProvider = new();
        using WasmScheduler scheduler = new(timeProvider);
        TaskCompletionSource<bool> done = new(TaskCreationOptions.RunContinuationsAsynchronously);
        List<int> values = [];

        foreach (var value in ExpectedBurst)
        {
            _ = scheduler.Schedule(value, (scheduler, state) =>
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

        timeProvider.FireAll();

        _ = await done.Task;
        await Assert.That(values).IsEquivalentTo(ExpectedBurst, EqualityComparer<int>.Default, TUnit.Assertions.Enums.CollectionOrdering.Matching);
    }

    /// <summary>Verifies an item disposed while it waits in the ready queue is skipped and later work runs.</summary>
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
        ManualTimeProvider timeProvider = new();
        using WasmScheduler scheduler = new(timeProvider);
        TaskCompletionSource<bool> delayed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource<bool> immediate = new(TaskCreationOptions.RunContinuationsAsynchronously);

        _ = scheduler.Schedule(0, DelayedDueTime, (_, _) =>
        {
            _ = delayed.TrySetResult(true);
            return Disposable.Empty;
        });
        _ = scheduler.Schedule(0, TimeSpan.Zero, (_, _) =>
        {
            _ = immediate.TrySetResult(true);
            return Disposable.Empty;
        });
        timeProvider.FireAll();

        await Assert.That(await delayed.Task).IsTrue();
        await Assert.That(await immediate.Task).IsTrue();
    }

    /// <summary>Verifies disposing a delayed work item before it is due cancels it, so a late callback is dropped.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task DisposedDelayedItemDoesNotRun()
    {
        ManualTimeProvider timeProvider = new();
        using WasmScheduler scheduler = new(timeProvider);
        var ran = false;
        var subscription = (WasmScheduler.StatefulWorkItem<int>)scheduler.Schedule(
            0,
            UnreachableDueTime,
            (_, _) =>
            {
                ran = true;
                return Disposable.Empty;
            });

        subscription.Dispose();

        // The due time never elapses on its own, so this stands in for a timer callback in flight.
        subscription.Run();

        await Assert.That(subscription.IsDisposed).IsTrue();
        await Assert.That(ran).IsFalse();
    }

    /// <summary>Verifies periodic work ticks repeatedly, threads state, and stops on dispose.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task SchedulePeriodicTicksAndStopsOnDispose()
    {
        ManualTimeProvider timeProvider = new();
        using WasmScheduler scheduler = new(timeProvider);
        var count = 0;
        var subscription = (WasmScheduler.PeriodicWorkItem<int>)scheduler.SchedulePeriodic(
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

    /// <summary>Verifies a zero period is clamped and dispatches ticks.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task SchedulePeriodicClampsZeroPeriod()
    {
        ManualTimeProvider timeProvider = new();
        using WasmScheduler scheduler = new(timeProvider);
        TaskCompletionSource<bool> ticked = new(TaskCreationOptions.RunContinuationsAsynchronously);

        var subscription = scheduler.SchedulePeriodic(0, TimeSpan.Zero, state =>
        {
            _ = ticked.TrySetResult(true);
            return state;
        });
        await Assert.That(timeProvider.LastPeriod).IsEqualTo(TimeSpan.FromMilliseconds(1));
        timeProvider.FireAll();

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
        ManualTimeProvider timeProvider = new();
        using WasmScheduler scheduler = new(timeProvider);
        var subscription =
            scheduler.Schedule(0, UnreachableDueTime, static (_, _) => Disposable.Empty);

        subscription.Dispose();

        await Assert.That(subscription.Dispose).ThrowsNothing();
    }

    /// <summary>Verifies disposing a periodic work item twice is idempotent.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task DisposedPeriodicItemDisposeIsIdempotent()
    {
        ManualTimeProvider timeProvider = new();
        using WasmScheduler scheduler = new(timeProvider);
        var subscription = scheduler.SchedulePeriodic(0, UnreachablePeriod, static state => state);

        subscription.Dispose();

        await Assert.That(subscription.Dispose).ThrowsNothing();
    }

    /// <summary>Verifies an action that cancels its own item before returning has its returned disposable released.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task SelfCancellingImmediateActionDisposesReturnedDisposable()
    {
        using var scheduler = CreateIsolatedScheduler();
        BooleanDisposable returned = new();

        // Publish the cancellation handle before running the queued action.
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

    /// <summary>Verifies the shared scheduler's immediate overload rejects a null action.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ScheduleWithNullActionThrows() =>
        await Assert.That(static () => WasmScheduler.Default.Schedule(0, null!)).Throws<ArgumentNullException>();

    /// <summary>Verifies the shared scheduler's delayed overload rejects a null action.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ScheduleDelayedWithNullActionThrows() => await Assert
        .That(static () => WasmScheduler.Default.Schedule(0, ValidInterval, null!))
        .Throws<ArgumentNullException>();

    /// <summary>Verifies the shared scheduler's periodic overload rejects a negative period.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task SchedulePeriodicWithNegativePeriodThrows() => await Assert
        .That(static () => WasmScheduler.Default.SchedulePeriodic(0, TimeSpan.FromMilliseconds(-1), static s => s))
        .Throws<ArgumentOutOfRangeException>();

    /// <summary>Verifies the shared scheduler's periodic overload rejects a null action.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task SchedulePeriodicWithNullActionThrows() => await Assert
        .That(static () => WasmScheduler.Default.SchedulePeriodic(0, ValidInterval, null!))
        .Throws<ArgumentNullException>();

    /// <summary>Verifies every scheduling overload rejects work after disposal.</summary>
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

    /// <summary>Verifies disposing the scheduler releases a queued item rather than leaving it for a resuming drain to run.</summary>
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

        // Queue without arming a drain, so the item sits unclaimed in the queue when the disposal runs.
        scheduler.QueueReady(queued);

        scheduler.Dispose();

        // The parked drain resuming: the item it never reached is gone from the queue the disposal released.
        scheduler.RunReadyBatch();

        await Assert.That(queuedRan).IsEqualTo(0);
        await Assert.That(queued.IsDisposed).IsTrue();
        await Assert.That(queued.Dispose).ThrowsNothing();
    }

    /// <summary>Verifies a timer attached after cancellation is disposed immediately.</summary>
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

        item.Dispose();

        BooleanDisposable timer = new();
        item.AttachTimer(timer);

        await Assert.That(timer.IsDisposed).IsTrue();
        await Assert.That(ran).IsEqualTo(0);
    }

    /// <summary>Final cancellation cleanup does not release a result or attached timer twice.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task CancellationAfterPublication_ReleasesResultAndTimerOnce()
    {
        using var scheduler = CreateIsolatedScheduler();
        CountingDisposable result = new();
        CountingDisposable timer = new();
        WasmScheduler.StatefulWorkItem<int> item = new(scheduler, 0, (_, _) => result);
        item.Run();
        item.AttachTimer(timer);
        item.Dispose();
        item.ReleaseCanceledResult();
        item.ReleaseCanceledTimer();
        await Assert.That(result.DisposeCount).IsEqualTo(1);
        await Assert.That(timer.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Verifies a dispatched periodic callback observes disposal before invoking the action.</summary>
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
            },
            new ManualTimeProvider());

        item.Dispose();

        item.Tick();

        await Assert.That(ticks).IsEqualTo(0);
    }

    /// <summary>Verifies disposal between the initial check and enqueue cancels the queued item.</summary>
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

        // Models an enqueue that passed Schedule's disposed check before the disposal drained the ready queue.
        scheduler.Enqueue(item);

        await Assert.That(item.IsDisposed).IsTrue();

        // A drain that arrives after the enqueue finds nothing to run, so the released item stays unrun.
        scheduler.RunReadyBatch();

        await Assert.That(ran).IsEqualTo(0);
    }

    /// <summary>Verifies a drain pass dispatches every queued item exactly once.</summary>
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

    /// <summary>Verifies a drain claim rejects stale snapshots and preserves the current claimant.</summary>
    /// <returns>A task representing the asynchronous test operation.</returns>
    [Test]
    public async Task DrainClaimsRejectStaleStateSnapshots()
    {
        const int Idle = 0;
        const int Running = 1;
        const int Pending = 2;
        ManualTimeProvider timeProvider = new();
        using WasmScheduler scheduler = new(timeProvider);
        var runs = 0;
        scheduler.QueueReady(new WasmScheduler.StatefulWorkItem<int>(scheduler, 0, (_, _) =>
        {
            runs++;
            return Disposable.Empty;
        }));

        await Assert.That(scheduler.TryPostDrain(Running)).IsFalse();
        await Assert.That(scheduler.TryPostDrain(Idle)).IsTrue();
        await Assert.That(scheduler.TryPostDrain(Idle)).IsFalse();
        await Assert.That(scheduler.TryPostDrain(Running)).IsTrue();
        await Assert.That(scheduler.TryPostDrain(Running)).IsFalse();
        await Assert.That(scheduler.TryPostDrain(Pending)).IsTrue();
        timeProvider.FireAll();
        await Assert.That(runs).IsEqualTo(1);
        await Assert.That(scheduler.TryPostDrain(Idle)).IsTrue();
    }

    /// <summary>Verifies work published without a drain request is picked up by the finishing drain.</summary>
    /// <returns>A task representing the asynchronous test operation.</returns>
    [Test]
    public async Task FinishingDrainSchedulesWorkPublishedDuringItsBatch()
    {
        const int First = 1;
        const int Second = 2;
        ManualTimeProvider timeProvider = new();
        using WasmScheduler scheduler = new(timeProvider);
        List<int> values = [];
        _ = scheduler.Schedule(First, (_, value) =>
        {
            values.Add(value);
            scheduler.QueueReady(new WasmScheduler.StatefulWorkItem<int>(scheduler, Second, (_, next) =>
            {
                values.Add(next);
                return Disposable.Empty;
            }));
            return Disposable.Empty;
        });

        timeProvider.FireAll();
        await Assert.That(values).IsEquivalentTo([First], EqualityComparer<int>.Default, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        timeProvider.FireAll();
        await Assert.That(values).IsEquivalentTo([First, Second], EqualityComparer<int>.Default, TUnit.Assertions.Enums.CollectionOrdering.Matching);
    }

    /// <summary>Creates a scheduler with manually dispatched timers.</summary>
    /// <returns>The isolated scheduler.</returns>
    private static WasmScheduler CreateIsolatedScheduler() => new(new ManualTimeProvider());

    /// <summary>Counts every release, including duplicate calls.</summary>
    private sealed class CountingDisposable : IDisposable
    {
        /// <summary>Gets the number of release calls.</summary>
        internal int DisposeCount { get; private set; }

        /// <inheritdoc/>
        public void Dispose() => DisposeCount++;
    }

    /// <summary>Stores timer callbacks until the test dispatches them.</summary>
    private sealed class ManualTimeProvider : TimeProvider
    {
        /// <summary>The timers owned by this provider.</summary>
        private readonly List<ManualTimer> _timers = [];

        /// <summary>Gets the most recently requested timer period.</summary>
        public TimeSpan LastPeriod => _timers[^1].Period;

        /// <summary>Gets or sets the time reported as the current UTC time.</summary>
        public DateTimeOffset UtcNow { get; set; } = DateTimeOffset.UnixEpoch;

        /// <inheritdoc/>
        public override DateTimeOffset GetUtcNow() => UtcNow;

        /// <inheritdoc/>
        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            ManualTimer timer = new(callback, state);
            _ = timer.Change(dueTime, period);
            _timers.Add(timer);
            return timer;
        }

        /// <summary>Invokes each armed timer once.</summary>
        public void FireAll()
        {
            foreach (var timer in _timers.ToArray())
            {
                timer.Fire();
            }
        }

        /// <summary>A timer whose callback is dispatched explicitly.</summary>
        /// <param name="callback">The timer callback.</param>
        /// <param name="state">The callback state.</param>
        private sealed class ManualTimer(TimerCallback callback, object? state) : ITimer
        {
            /// <summary>The next due time.</summary>
            private TimeSpan _dueTime;

            /// <summary>Whether the timer has been disposed.</summary>
            private bool _disposed;

            /// <summary>Gets the repeating period.</summary>
            public TimeSpan Period { get; private set; }

            /// <inheritdoc/>
            public bool Change(TimeSpan dueTime, TimeSpan period)
            {
                _dueTime = dueTime;
                Period = period;
                return !_disposed;
            }

            /// <summary>Invokes the callback if the timer is armed.</summary>
            public void Fire()
            {
                if (_disposed || _dueTime == Timeout.InfiniteTimeSpan)
                {
                    return;
                }

                _dueTime = Period;
                callback(state);
            }

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Dispose() => _disposed = true;

            /// <inheritdoc/>
            public ValueTask DisposeAsync()
            {
                Dispose();
                return default;
            }
        }
    }
}
