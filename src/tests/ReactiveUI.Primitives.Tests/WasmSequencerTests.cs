// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for <see cref="WasmSequencer"/>.</summary>
public sealed class WasmSequencerTests
{
    /// <summary>Expected values produced by an immediate burst, used to verify FIFO order.</summary>
    private static readonly int[] ExpectedBurst = [1, 2, 3];

    /// <summary>Longest a test waits for scheduled work before failing.</summary>
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(5);

    /// <summary>How far in the future delayed work is scheduled.</summary>
    private static readonly TimeSpan ScheduleDelay = TimeSpan.FromMilliseconds(50);

    /// <summary>How long a disposed sequencer is watched to prove it never ran the work it rejected or released.</summary>
    private static readonly TimeSpan PostDisposeObservationWindow = TimeSpan.FromMilliseconds(200);

    /// <summary>
    /// How long to wait for the marshal step to release a delayed item once it comes due. The release runs on the
    /// shared timer's pool thread, so on a saturated runner it can fire long after the item is due; a real failure
    /// to release the item never signals, so this window only has to outlast a slow runner and costs nothing when
    /// the item is released promptly.
    /// </summary>
    private static readonly TimeSpan ReleaseTimeout = TimeSpan.FromSeconds(30);

    /// <summary>Verifies the shared instance is a singleton.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task DefaultReturnsSingleton() =>
        await Assert.That(WasmSequencer.Default).IsSameReferenceAs(WasmSequencer.Default);

    /// <summary>Verifies immediate scheduling rejects a null work item.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ScheduleRejectsNullItem()
    {
        await Assert.That(static () => WasmSequencer.Default.Schedule(null!)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(static () => WasmSequencer.Default.Schedule(null!, 0L)).ThrowsExactly<ArgumentNullException>();
    }

    /// <summary>Verifies the clock properties are sane.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ClockAdvances()
    {
        var sequencer = WasmSequencer.Default;
        var before = sequencer.Timestamp;

        await Assert.That(sequencer.Now).IsGreaterThan(DateTimeOffset.MinValue);
        await Assert.That(sequencer.Timestamp).IsGreaterThanOrEqualTo(before);
    }

    /// <summary>Verifies immediate work executes without the caller pumping anything.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ImmediateScheduleExecutes()
    {
        TaskCompletionSource<bool> executed = new(TaskCreationOptions.RunContinuationsAsynchronously);

        WasmSequencer.Default.Schedule(new DelegateWorkItem(() => executed.TrySetResult(true)));

        await Assert.That(await executed.Task.WaitAsync(WaitTimeout)).IsTrue();
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
            var captured = value;
            WasmSequencer.Default.Schedule(new DelegateWorkItem(() =>
            {
                values.Add(captured);
                if (values.Count != ExpectedBurst.Length)
                {
                    return;
                }

                _ = done.TrySetResult(true);
            }));
        }

        _ = await done.Task.WaitAsync(WaitTimeout);
        await Assert.That(values).IsEquivalentTo(ExpectedBurst, EqualityComparer<int>.Default);
    }

    /// <summary>Verifies delayed work executes no earlier than its due timestamp.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task DelayedScheduleExecutesAfterDue()
    {
        var sequencer = WasmSequencer.Default;
        TaskCompletionSource<long> executed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var start = sequencer.Timestamp;
        var due = Sequencer.AddTimestamp(start, ScheduleDelay);

        sequencer.Schedule(new DelegateWorkItem(() => executed.TrySetResult(sequencer.Timestamp)), due);

        var executedAt = await executed.Task.WaitAsync(WaitTimeout);
        await Assert.That(executedAt).IsGreaterThanOrEqualTo(start);
    }

    /// <summary>Verifies a past-due timestamp executes promptly through the immediate path.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task PastDueScheduleExecutes()
    {
        TaskCompletionSource<bool> executed = new(TaskCreationOptions.RunContinuationsAsynchronously);

        WasmSequencer.Default.Schedule(new DelegateWorkItem(() => executed.TrySetResult(true)), long.MinValue);

        await Assert.That(await executed.Task.WaitAsync(WaitTimeout)).IsTrue();
    }

    /// <summary>Verifies a cancelled work item never executes while later work still runs.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task CancelledItemIsSkipped()
    {
        TaskCompletionSource<bool> markerRan = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var cancelledRan = false;
        CancellableWorkItem cancelled = new(() => cancelledRan = true);
        cancelled.Dispose();

        WasmSequencer.Default.Schedule(cancelled);
        WasmSequencer.Default.Schedule(new DelegateWorkItem(() => markerRan.TrySetResult(true)));

        _ = await markerRan.Task.WaitAsync(WaitTimeout);
        await Assert.That(cancelledRan).IsFalse();
    }

    /// <summary>Verifies disposing a fresh sequencer releases its drain timer and is idempotent.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task DisposeReleasesDrainTimerAndIsIdempotent()
    {
        WasmSequencer sequencer = new();

        sequencer.Dispose();

        await Assert.That(sequencer.Dispose).ThrowsNothing();
    }

    /// <summary>
    /// Verifies a disposed sequencer rejects new work rather than queueing work it can never drain. Disposal releases
    /// the drain timer, so an accepted item would sit in the ready queue forever behind a timer that can no longer be
    /// armed. Both scheduling overloads fail fast instead, and neither item runs.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ScheduleAfterDisposeThrowsObjectDisposedException()
    {
        WasmSequencer sequencer = new();
        var ran = 0;
        DelegateWorkItem immediate = new(() => Interlocked.Increment(ref ran));
        DelegateWorkItem delayed = new(() => Interlocked.Increment(ref ran));

        sequencer.Dispose();

        await Assert.That(() => sequencer.Schedule(immediate)).ThrowsExactly<ObjectDisposedException>();
        await Assert
            .That(() => sequencer.Schedule(delayed, Sequencer.AddTimestamp(sequencer.Timestamp, ScheduleDelay)))
            .ThrowsExactly<ObjectDisposedException>();

        await Task.Delay(PostDisposeObservationWindow);
        await Assert.That(Volatile.Read(ref ran)).IsEqualTo(0);
    }

    /// <summary>
    /// Verifies an enqueue that loses the race to disposal releases the item it just queued. The disposed check runs
    /// before the item joins the ready queue, so a disposal landing in between would otherwise strand the item behind
    /// a drain timer that can never fire again — the caller would hold a handle to work that neither runs nor cancels.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ScheduleReadyThatLosesTheRaceToDisposeReleasesTheItemItQueued()
    {
        WasmSequencer sequencer = new();
        var ran = 0;
        CancellableWorkItem item = new(() => Interlocked.Increment(ref ran));

        sequencer.Dispose();

        // The enqueue that was already past the disposed check when the disposal drained the ready queue.
        sequencer.ScheduleReady(item);

        await Assert.That(item.IsDisposed).IsTrue();

        await Task.Delay(PostDisposeObservationWindow);
        await Assert.That(Volatile.Read(ref ran)).IsEqualTo(0);
    }

    /// <summary>
    /// Verifies delayed work still parked on the shared timer when the sequencer is disposed is released rather than
    /// marshalled back into a sequencer that is gone. The marshal step must neither run the item nor throw
    /// <see cref="ObjectDisposedException"/> on the timer's thread, where nothing could catch it.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task DisposeReleasesDelayedWorkThatComesDueAfterwards()
    {
        WasmSequencer sequencer = new();
        var ran = 0;
        using ManualResetEventSlim released = new();
        CancellableWorkItem delayed = new(() => Interlocked.Increment(ref ran), released.Set);

        sequencer.Schedule(delayed, Sequencer.AddTimestamp(sequencer.Timestamp, ScheduleDelay));
        sequencer.Dispose();

        // The marshal step disposes the item on the shared timer's pool thread once it comes due. Wait on the
        // actual release signal rather than sleeping a fixed window: on a saturated runner the pool-driven marshal
        // step can fire well after any fixed delay, so a fixed sleep would report the item as never released even
        // though it was. An event wait is an OS-level wait no pool pressure can starve, and a genuine failure to
        // release the item never signals, so the generous window still fails.
        await Assert.That(released.Wait(ReleaseTimeout)).IsTrue();
        await Assert.That(delayed.IsDisposed).IsTrue();
        await Assert.That(Volatile.Read(ref ran)).IsEqualTo(0);
    }

    /// <summary>
    /// Verifies delayed work cancelled before it comes due is dropped by the marshal step rather than pushed onto the
    /// drain. The shared timer still fires, but the marshalled item observes the cancellation and returns without
    /// scheduling anything.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task DelayedItemCancelledBeforeItIsDueIsSkippedByTheMarshalStep()
    {
        WasmSequencer sequencer = new();
        var ran = 0;
        CancellableWorkItem delayed = new(() => Interlocked.Increment(ref ran));

        sequencer.Schedule(delayed, Sequencer.AddTimestamp(sequencer.Timestamp, ScheduleDelay));
        delayed.Dispose();

        // Outlast the due time: the marshal step must observe the cancellation and drop the item.
        await Task.Delay(ScheduleDelay + PostDisposeObservationWindow);

        await Assert.That(delayed.IsDisposed).IsTrue();
        await Assert.That(Volatile.Read(ref ran)).IsEqualTo(0);
    }

    /// <summary>
    /// Verifies delayed work that carries no cancellation handle is simply dropped when the sequencer is disposed
    /// before the item comes due. The marshal step cannot hand a non-disposable item back to a caller, so it releases
    /// nothing and never runs it.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task DisposeDropsDelayedNonDisposableWorkThatComesDueAfterwards()
    {
        WasmSequencer sequencer = new();
        var ran = 0;
        DelegateWorkItem delayed = new(() => Interlocked.Increment(ref ran));

        sequencer.Schedule(delayed, Sequencer.AddTimestamp(sequencer.Timestamp, ScheduleDelay));
        sequencer.Dispose();

        // Outlast the due time: the marshal step sees the disposed owner and, with no handle to release, drops it.
        await Task.Delay(ScheduleDelay + PostDisposeObservationWindow);

        await Assert.That(Volatile.Read(ref ran)).IsEqualTo(0);
    }

    /// <summary>Work item that invokes a delegate when executed.</summary>
    private sealed class DelegateWorkItem : IWorkItem
    {
        /// <summary>The action to run on execution.</summary>
        private readonly Action _action;

        /// <summary>Initializes a new instance of the <see cref="DelegateWorkItem"/> class.</summary>
        /// <param name="action">The action to run on execution.</param>
        public DelegateWorkItem(Action action) => _action = action;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Execute() => _action();
    }

    /// <summary>Cancellable work item that reports disposal to the sequencer.</summary>
    /// <param name="action">The action to run on execution.</param>
    /// <param name="onDisposed">An optional callback invoked the instant the item is disposed.</param>
    private sealed class CancellableWorkItem(Action action, Action? onDisposed = null) : IWorkItem, IsDisposed
    {
        /// <summary>The action to run on execution.</summary>
        private readonly Action _action = action;

        /// <summary>Invoked the instant the item is disposed, so a test can wait on the real release.</summary>
        private readonly Action? _onDisposed = onDisposed;

        /// <inheritdoc/>
        public bool IsDisposed { get; private set; }

        /// <inheritdoc/>
        public void Dispose()
        {
            IsDisposed = true;
            _onDisposed?.Invoke();
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Execute() => _action();
    }
}
