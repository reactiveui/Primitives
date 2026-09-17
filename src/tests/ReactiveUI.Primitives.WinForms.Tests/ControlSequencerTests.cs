// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;
using TUnit.Assertions.Enums;

namespace ReactiveUI.Primitives.WinForms.Tests;

/// <summary>Tests control dispatch with manually delivered callbacks and handle notifications.</summary>
public sealed class ControlSequencerTests
{
    /// <summary>The second value in a scheduled batch.</summary>
    private const int SecondValue = 2;

    /// <summary>Constructor validation rejects a missing control.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task ConstructorRejectsNullControl() =>
        await Assert.That(static () => new ControlSequencer(null!)).ThrowsExactly<ArgumentNullException>();

    /// <summary>The public constructor retains the control and exposes its clock without creating a handle.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Constructor_RetainsControlWithoutCreatingHandle()
    {
        using var control = CreateControl();
        ControlSequencer sequencer = new(control);
        var before = System.Diagnostics.Stopwatch.GetTimestamp();
        var timestamp = sequencer.Timestamp;
        var after = System.Diagnostics.Stopwatch.GetTimestamp();

        await Assert.That(sequencer.Control).IsSameReferenceAs(control);
        await Assert.That(control.IsHandleCreated).IsFalse();
        await Assert.That(sequencer.Now.Offset).IsEqualTo(TimeSpan.Zero);
        await Assert.That(timestamp).IsGreaterThanOrEqualTo(before);
        await Assert.That(timestamp).IsLessThanOrEqualTo(after);
        await Assert.That(GetDebuggerDisplay(sequencer)).IsEqualTo(sequencer.ToString());
    }

    /// <summary>Work rejected before handle creation is retried in order when the handle becomes ready.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task HandleCreatedRetriesQueuedWorkAndSkipsCancellation()
    {
        using var control = CreateControl();
        Queue<Action> drains = new();
        var ready = false;
        ControlSequencer sequencer = new(
            control,
            drain =>
            {
                if (!ready)
                {
                    return false;
                }

                drains.Enqueue(drain);
                return true;
            },
            null);
        List<int> values = [];
        RecordingWorkItem cancelled = new(() => values.Add(0));
        sequencer.Schedule(new RecordingWorkItem(() => values.Add(1)));
        sequencer.Schedule(cancelled);
        sequencer.Schedule(new RecordingWorkItem(() => values.Add(SecondValue)), 0);
        cancelled.Dispose();
        await Assert.That(sequencer.Control).IsSameReferenceAs(control);
        await Assert.That(drains).IsEmpty();
        await Assert.That(values).IsEmpty();
        ready = true;
        sequencer.OnHandleCreated(control, EventArgs.Empty);
        await Assert.That(drains).Count().IsEqualTo(1);
        drains.Dequeue()();
        await Assert.That(values).IsEquivalentTo([1, SecondValue], EqualityComparer<int>.Default, CollectionOrdering.Matching);
        sequencer.OnHandleCreated(control, EventArgs.Empty);
        await Assert.That(drains).IsEmpty();
    }

    /// <summary>A missing handle rejects a post before invoking the native dispatcher.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task TryPostWithoutAHandleDoesNotInvoke()
    {
        using var control = CreateControl();
        var accepted = ControlSequencer.TryPost(
            control,
            false,
            static () => { },
            static (_, _) => throw new InvalidOperationException("Unexpected dispatch."));
        await Assert.That(accepted).IsFalse();
    }

    /// <summary>A live handle forwards exactly the supplied callback.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task TryPostWithAHandleForwardsTheCallback()
    {
        using var control = CreateControl();
        var calls = 0;
        Action callback = () => calls++;
        Action? posted = null;
        var accepted = ControlSequencer.TryPost(control, true, callback, (_, drain) => posted = drain);
        await Assert.That(accepted).IsTrue();
        await Assert.That<Action?>(posted).IsSameReferenceAs(callback);
        await Assert.That(calls).IsEqualTo(0);
        posted!();
        await Assert.That(calls).IsEqualTo(1);
    }

    /// <summary>A disappeared handle rejects the post while disposal propagates the failure.</summary>
    /// <param name="dispose">Whether disposal occurs during dispatch.</param>
    /// <returns>The test operation.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task TryPostHandlesDispatcherFailureAccordingToDisposal(bool dispose)
    {
        using var control = CreateControl();
        InvalidOperationException failure = new("Handle lost.");
        bool Post() => ControlSequencer.TryPost(
            control,
            true,
            static () => { },
            (target, _) =>
            {
                if (dispose)
                {
                    target.Dispose();
                }

                throw failure;
            });
        if (dispose)
        {
            var observed = await Assert.That(Post).ThrowsExactly<InvalidOperationException>();
            await Assert.That(observed).IsSameReferenceAs(failure);
        }
        else
        {
            await Assert.That(Post()).IsFalse();
        }
    }

    /// <summary>Disposed controls reject work before dispatch.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task TryPostAfterDisposalThrows()
    {
        var control = CreateControl();
        control.Dispose();
        await Assert.That(() => ControlSequencer.TryPost(control, true, static () => { }, static (_, _) => { }))
            .ThrowsExactly<ObjectDisposedException>();
    }

    /// <summary>Delayed work waits for its callback and skips cancellation.</summary>
    /// <param name="cancel">Whether to cancel before delivery.</param>
    /// <returns>The test operation.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task DelayedScheduleWaitsForItsCallback(bool cancel)
    {
        using var control = CreateControl();
        Queue<(IWorkItem Item, long Due)> delayed = new();
        ControlSequencer sequencer = new(control, static _ => false, (item, due) => delayed.Enqueue((item, due)));
        var calls = 0;
        RecordingWorkItem item = new(() => calls++);
        sequencer.Schedule(item, long.MaxValue);
        await Assert.That(calls).IsEqualTo(0);
        var pending = delayed.Dequeue();
        await Assert.That(pending.Due).IsEqualTo(long.MaxValue);
        if (cancel)
        {
            item.Dispose();
        }

        DispatchSequencerState.RunIfActive(pending.Item);
        await Assert.That(calls).IsEqualTo(cancel ? 0 : 1);
    }

    /// <summary>Invokes the getter used by the debugger without reflection.</summary>
    /// <param name="sequencer">The sequencer to display.</param>
    /// <returns>The debugger display text.</returns>
    [UnsafeAccessor(UnsafeAccessorKind.Method, Name = "get_DebuggerDisplay")]
    private static extern string GetDebuggerDisplay(ControlSequencer sequencer);

    /// <summary>Creates a control without changing the caller's synchronization context.</summary>
    /// <returns>The control.</returns>
    private static Control CreateControl()
    {
        var previous = SynchronizationContext.Current;
        try
        {
            return new();
        }
        finally
        {
            SynchronizationContext.SetSynchronizationContext(previous);
        }
    }

    /// <summary>Records execution and supports cancellation.</summary>
    /// <param name="action">The callback to run.</param>
    private sealed class RecordingWorkItem(Action action) : IWorkItem, IsDisposed
    {
        /// <inheritdoc/>
        public bool IsDisposed { get; private set; }

        /// <inheritdoc/>
        public void Dispose() => IsDisposed = true;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Execute() => action();
    }
}
