// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Disposables;
using ReactiveUI.Primitives.Reactive.Concurrency;
using Timer = System.Threading.Timer;

namespace ReactiveUI.Primitives.Reactive.Tests;

/// <summary>Tests for <see cref="WasmScheduler"/>.</summary>
public sealed class WasmSchedulerTests
{
    /// <summary>State payload used to verify state threading.</summary>
    private const int StatePayload = 42;

    /// <summary>Minimum periodic ticks a test observes before disposing.</summary>
    private const int MinimumTicks = 2;

    /// <summary>Threads that enqueue concurrently in the single-flight drain test.</summary>
    private const int ProducerCount = 4;

    /// <summary>Items each producer enqueues in the single-flight drain test.</summary>
    private const int ItemsPerProducer = 500;

    /// <summary>Expected values produced by an immediate burst, used to verify FIFO order.</summary>
    private static readonly int[] ExpectedBurst = [1, 2, 3];

    /// <summary>Longest a test waits for scheduled work before failing.</summary>
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Due time of a work item that is expected to run after its delay elapses.</summary>
    private static readonly TimeSpan DelayedDueTime = TimeSpan.FromMilliseconds(50);

    /// <summary>Due time of a work item that is disposed before it becomes due, so it must never run.</summary>
    private static readonly TimeSpan CancellationDueTime = TimeSpan.FromMilliseconds(100);

    /// <summary>How long a test waits past <see cref="CancellationDueTime"/> to prove a cancelled item did not run.</summary>
    private static readonly TimeSpan CancellationObservationWindow = TimeSpan.FromMilliseconds(250);

    /// <summary>Period between ticks of a periodic work item.</summary>
    private static readonly TimeSpan TickPeriod = TimeSpan.FromMilliseconds(10);

    /// <summary>How long a test waits after disposing a periodic item to prove no further ticks arrive.</summary>
    private static readonly TimeSpan PostDisposeObservationWindow = TimeSpan.FromMilliseconds(100);

    /// <summary>A positive due time or period, so a null action is the only invalid argument under test.</summary>
    private static readonly TimeSpan ValidInterval = TimeSpan.FromMilliseconds(100);

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
        await Assert.That(() => scheduler.Schedule(0, TimeSpan.FromMilliseconds(1), null!))
            .ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => scheduler.SchedulePeriodic(0, TimeSpan.FromMilliseconds(1), null!))
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

        await Assert.That(await delayed.Task.WaitAsync(WaitTimeout)).IsTrue();
        await Assert.That(await immediate.Task.WaitAsync(WaitTimeout)).IsTrue();
    }

    /// <summary>Verifies disposing a delayed work item before it is due cancels it.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task DisposedDelayedItemDoesNotRun()
    {
        var ran = false;

        var subscription = WasmScheduler.Default.Schedule(0, CancellationDueTime, (_, _) =>
        {
            ran = true;
            return Disposable.Empty;
        });
        subscription.Dispose();

        await Task.Delay(CancellationObservationWindow);
        await Assert.That(ran).IsFalse();
    }

    /// <summary>Verifies periodic work ticks repeatedly, threads state, and stops on dispose.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task SchedulePeriodicTicksAndStopsOnDispose()
    {
        TaskCompletionSource<bool> reachedTwo = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var count = 0;

        var subscription = WasmScheduler.Default.SchedulePeriodic(0, TickPeriod, state =>
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

        await Task.Delay(PostDisposeObservationWindow);
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
            WasmScheduler.Default.Schedule(0, TimeSpan.FromMinutes(1), static (_, _) => Disposable.Empty);

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
        var returned = Disposable.Create(returnedDisposed, static source => source.TrySetResult());

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
            _ = Interlocked.Increment(ref ran);
            return Disposable.Empty;
        })).ThrowsExactly<ObjectDisposedException>();

        await Assert.That(() => scheduler.Schedule(0, DelayedDueTime, (_, _) =>
        {
            _ = Interlocked.Increment(ref ran);
            return Disposable.Empty;
        })).ThrowsExactly<ObjectDisposedException>();

        await Assert.That(() => scheduler.SchedulePeriodic(0, TickPeriod, state =>
        {
            _ = Interlocked.Increment(ref ran);
            return state;
        })).ThrowsExactly<ObjectDisposedException>();

        await Task.Delay(CancellationObservationWindow);
        await Assert.That(Volatile.Read(ref ran)).IsEqualTo(0);
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
        TaskCompletionSource gateEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        using ManualResetEventSlim release = new(false);

        // Park the single drain inside the first item, so the second item is provably still queued when Dispose runs.
        _ = scheduler.Schedule(0, (_, _) =>
        {
            _ = gateEntered.TrySetResult();
            _ = release.Wait(WaitTimeout);
            return Disposable.Empty;
        });
        await gateEntered.Task.WaitAsync(WaitTimeout);

        var queuedRan = 0;
        var queued = scheduler.Schedule(0, (_, _) =>
        {
            _ = Interlocked.Increment(ref queuedRan);
            return Disposable.Empty;
        });

        scheduler.Dispose();

        // Let the parked drain resume: the item it never reached must have been cancelled by the disposal.
        release.Set();
        await Task.Delay(PostDisposeObservationWindow);

        await Assert.That(Volatile.Read(ref queuedRan)).IsEqualTo(0);
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
                _ = Interlocked.Increment(ref ran);
                return Disposable.Empty;
            });

        // Cancel before the delayed schedule reaches its AttachTimer call.
        item.Dispose();

        var fired = 0;
        await using Timer timer = new(
            _ => Interlocked.Increment(ref fired),
            null,
            DelayedDueTime,
            Timeout.InfiniteTimeSpan);

        item.AttachTimer(timer);

        // A timer still armed would have fired well inside this window; the released one never can.
        await Task.Delay(CancellationObservationWindow);

        await Assert.That(Volatile.Read(ref fired)).IsEqualTo(0);
        await Assert.That(Volatile.Read(ref ran)).IsEqualTo(0);
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
                _ = Interlocked.Increment(ref ticks);
                return state;
            });

        item.Dispose();

        // The period never elapses on its own, so this is the tick a callback already in flight would have delivered.
        item.Tick();

        await Assert.That(Volatile.Read(ref ticks)).IsEqualTo(0);
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
                _ = Interlocked.Increment(ref ran);
                return Disposable.Empty;
            });

        scheduler.Dispose();

        // The enqueue that was already past Schedule's disposed check when the disposal drained the ready queue.
        scheduler.Enqueue(item);

        await Assert.That(item.IsDisposed).IsTrue();

        await Task.Delay(PostDisposeObservationWindow);
        await Assert.That(Volatile.Read(ref ran)).IsEqualTo(0);
    }

    /// <summary>
    /// Verifies the single-flight drain latch dispatches every item exactly once when many threads enqueue at the same
    /// time. Enqueues, drain posts and the running drain all interleave here, so a lost drain post would strand work
    /// and a double-claimed latch would run an item twice.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ConcurrentSchedulingDispatchesEveryItemExactlyOnce()
    {
        using var scheduler = CreateIsolatedScheduler();
        var ran = 0;
        using CountdownEvent completed = new(ProducerCount * ItemsPerProducer);
        var producers = new Task[ProducerCount];

        for (var producer = 0; producer < ProducerCount; producer++)
        {
            producers[producer] = Task.Run(() =>
            {
                for (var item = 0; item < ItemsPerProducer; item++)
                {
                    _ = scheduler.Schedule(0, (_, _) =>
                    {
                        _ = Interlocked.Increment(ref ran);
                        _ = completed.Signal();
                        return Disposable.Empty;
                    });
                }
            });
        }

        await Task.WhenAll(producers);

        await Assert.That(completed.Wait(WaitTimeout)).IsTrue();

        // Settle, then prove the latch never let a second drain re-run an item it had already dispatched.
        await Task.Delay(PostDisposeObservationWindow);
        await Assert.That(Volatile.Read(ref ran)).IsEqualTo(ProducerCount * ItemsPerProducer);
    }

    /// <summary>
    /// Creates a scheduler that owns its own drain timer and ready queue, so a test can dispose it without
    /// disturbing the shared singleton every other test schedules through.
    /// </summary>
    /// <returns>The isolated scheduler.</returns>
    private static WasmScheduler CreateIsolatedScheduler() => new();

    /// <summary>Simple holder to work around race condition in closure.</summary>
    private sealed class Holder
    {
        /// <summary>Gets or sets the subscription held by this holder.</summary>
        public IDisposable? Subscription { get; set; }
    }
}
