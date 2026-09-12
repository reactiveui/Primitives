// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Reactive.Concurrency;
using TUnit.Assertions.Enums;

namespace ReactiveUI.Primitives.WinForms.Reactive.Tests;

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

    /// <summary>Work rejected before handle creation is retried in order when the handle becomes ready.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task HandleCreatedRetriesQueuedWorkAndSkipsCancellation()
    {
        using var control = CreateControl();
        Queue<Action> drains = new();
        var ready = false;
        ControlSequencer scheduler = new(
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
        using var first = scheduler.Schedule(() => values.Add(1));
        var cancelled = scheduler.Schedule(() => values.Add(0));
        using var second = scheduler.Schedule(TimeSpan.Zero, () => values.Add(SecondValue));
        cancelled.Dispose();
        await Assert.That(scheduler.Control).IsSameReferenceAs(control);
        await Assert.That(drains).IsEmpty();
        await Assert.That(values).IsEmpty();
        ready = true;
        scheduler.OnHandleCreated(control, EventArgs.Empty);
        await Assert.That(drains).Count().IsEqualTo(1);
        drains.Dequeue()();
        await Assert.That(values).IsEquivalentTo([1, SecondValue], EqualityComparer<int>.Default, CollectionOrdering.Matching);
        scheduler.OnHandleCreated(control, EventArgs.Empty);
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

    /// <summary>Disposal cancels a delayed timer and suppresses even a late callback.</summary>
    /// <param name="cancel">Whether to cancel before delivery.</param>
    /// <returns>The test operation.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task DelayedScheduleHonorsCancellationBeforeDelivery(bool cancel)
    {
        using var control = CreateControl();
        Queue<Action> drains = new();
        ManualDelayScheduler delayed = new();
        ControlSequencer scheduler = new(
            control,
            drain =>
            {
                drains.Enqueue(drain);
                return true;
            },
            delayed);
        var calls = 0;
        var delay = TimeSpan.FromSeconds(1);
        var handle = scheduler.Schedule(delay, () => calls++);
        var pending = delayed.Callbacks.Dequeue();
        await Assert.That(pending.Delay).IsEqualTo(delay);
        await Assert.That(calls).IsEqualTo(0);
        if (cancel)
        {
            handle.Dispose();
        }

        pending.Callback();
        drains.Dequeue()();
        await Assert.That(pending.Cancellation.IsDisposed).IsEqualTo(cancel);
        await Assert.That(calls).IsEqualTo(cancel ? 0 : 1);
        if (cancel)
        {
            return;
        }

        handle.Dispose();
    }

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

    /// <summary>Retains delayed actions and permits explicit late delivery after cancellation.</summary>
    private sealed class ManualDelayScheduler : IScheduler
    {
        /// <summary>Gets pending callbacks and their cancellation handles.</summary>
        public Queue<(Action Callback, TimeSpan Delay, SingleAssignmentDisposable Cancellation)> Callbacks { get; } = new();

        /// <inheritdoc/>
        public DateTimeOffset Now => DateTimeOffset.UnixEpoch;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IDisposable Schedule<TState>(TState state, Func<IScheduler, TState, IDisposable> action) =>
            Schedule(state, TimeSpan.Zero, action);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IDisposable Schedule<TState>(TState state, DateTimeOffset dueTime, Func<IScheduler, TState, IDisposable> action) =>
            Schedule(state, dueTime - Now, action);

        /// <inheritdoc/>
        public IDisposable Schedule<TState>(TState state, TimeSpan dueTime, Func<IScheduler, TState, IDisposable> action)
        {
            SingleAssignmentDisposable cancellation = new();
            Callbacks.Enqueue((() => cancellation.Disposable = action(this, state), dueTime, cancellation));
            return cancellation;
        }
    }
}
