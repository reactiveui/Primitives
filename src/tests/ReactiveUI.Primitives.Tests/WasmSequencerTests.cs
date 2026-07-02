// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

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
        await Assert.That(() => WasmSequencer.Default.Schedule(null!)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => WasmSequencer.Default.Schedule(null!, 0L)).ThrowsExactly<ArgumentNullException>();
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
        var delay = TimeSpan.FromMilliseconds(50);
        var start = sequencer.Timestamp;
        var due = Sequencer.AddTimestamp(start, delay);

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
        var sequencer = (WasmSequencer)Activator.CreateInstance(typeof(WasmSequencer), nonPublic: true)!;

        sequencer.Dispose();

        await Assert.That(sequencer.Dispose).ThrowsNothing();
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
        public void Execute() => _action();
    }

    /// <summary>Cancellable work item that reports disposal to the sequencer.</summary>
    private sealed class CancellableWorkItem : IWorkItem, IsDisposed
    {
        /// <summary>The action to run on execution.</summary>
        private readonly Action _action;

        /// <summary>Initializes a new instance of the <see cref="CancellableWorkItem"/> class.</summary>
        /// <param name="action">The action to run on execution.</param>
        public CancellableWorkItem(Action action) => _action = action;

        /// <inheritdoc/>
        public bool IsDisposed { get; private set; }

        /// <inheritdoc/>
        public void Dispose() => IsDisposed = true;

        /// <inheritdoc/>
        public void Execute() => _action();
    }
}
