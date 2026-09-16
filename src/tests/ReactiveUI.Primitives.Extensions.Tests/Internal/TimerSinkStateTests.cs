// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Extensions.Tests.Internal;

/// <summary>Tests for <see cref="TimerSinkState{T}"/>, the shared timer sink state.</summary>
public class TimerSinkStateTests
{
    /// <summary>The value queued by the value tests.</summary>
    private const int QueuedValue = 7;

    /// <summary>Verifies a queued value is delivered only once the state is flushed.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenQueueLocked_ThenDeliversOnFlush()
    {
        RecordingWitness<int> observer = new();
        TimerSinkState<int> state = new(observer);

        var queued = state.QueueLocked(QueuedValue);
        var deliveredBeforeFlush = observer.Values.Count;
        state.Flush();

        await Assert.That(queued).IsTrue();
        await Assert.That(deliveredBeforeFlush).IsEqualTo(0);
        await Assert.That(observer.Values).IsCollectionEqualTo([QueuedValue]);
    }

    /// <summary>Verifies <c>QueueErrorLocked</c> marks the state done and delivers the error on flush.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenQueueErrorLocked_ThenMarksDoneAndDeliversOnFlush()
    {
        RecordingWitness<int> observer = new();
        TimerSinkState<int> state = new(observer);
        InvalidOperationException expected = new("timer-error");

        var queued = state.QueueErrorLocked(expected);
        state.Flush();

        await Assert.That(queued).IsTrue();
        await Assert.That(state.Done).IsTrue();
        await Assert.That(state.Timer.IsDisposed).IsTrue();
        await Assert.That(observer.Errors).Count().IsEqualTo(1);
        await Assert.That(observer.Errors[0]).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies <c>QueueCompletedLocked</c> marks the state done and delivers completion on flush.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenQueueCompletedLocked_ThenMarksDoneAndDeliversOnFlush()
    {
        RecordingWitness<int> observer = new();
        TimerSinkState<int> state = new(observer);

        var queued = state.QueueCompletedLocked();
        state.Flush();

        await Assert.That(queued).IsTrue();
        await Assert.That(state.Done).IsTrue();
        await Assert.That(observer.Completions).IsEqualTo(1);
    }

    /// <summary>Verifies nothing more is queued once an error has been queued.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenQueuedAfterError_ThenNothingMoreIsQueued()
    {
        RecordingWitness<int> observer = new();
        TimerSinkState<int> state = new(observer);

        _ = state.QueueErrorLocked(new InvalidOperationException("first"));
        var completedQueued = state.QueueCompletedLocked();
        var valueQueued = state.QueueLocked(QueuedValue);
        state.Flush();

        await Assert.That(completedQueued).IsFalse();
        await Assert.That(valueQueued).IsFalse();
        await Assert.That(observer.Values).IsEmpty();
        await Assert.That(observer.Completions).IsEqualTo(0);
        await Assert.That(observer.Errors).Count().IsEqualTo(1);
    }

    /// <summary>Verifies an error is not queued once completion has been queued.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenQueueErrorLockedAfterCompleted_ThenNotQueued()
    {
        RecordingWitness<int> observer = new();
        TimerSinkState<int> state = new(observer);

        _ = state.QueueCompletedLocked();
        var errorQueued = state.QueueErrorLocked(new InvalidOperationException("second"));
        state.Flush();

        await Assert.That(errorQueued).IsFalse();
        await Assert.That(observer.Errors).IsEmpty();
        await Assert.That(observer.Completions).IsEqualTo(1);
    }

    /// <summary>Verifies a null error is rejected.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenQueueErrorLockedWithNull_ThenThrows()
    {
        TimerSinkState<int> state = new(new RecordingWitness<int>());

        await Assert.That(() => state.QueueErrorLocked(null!)).Throws<ArgumentNullException>();
    }

    /// <summary>Verifies <c>HandleDisposeLocked</c> marks the state done without forwarding anything and refuses later values.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenHandleDisposeLocked_ThenMarksDoneWithoutForwarding()
    {
        RecordingWitness<int> observer = new();
        TimerSinkState<int> state = new(observer);

        state.HandleDisposeLocked();
        var valueQueued = state.QueueLocked(QueuedValue);
        state.Flush();

        await Assert.That(state.Done).IsTrue();
        await Assert.That(valueQueued).IsFalse();
        await Assert.That(observer.Values).IsEmpty();
        await Assert.That(observer.Errors).IsEmpty();
        await Assert.That(observer.Completions).IsEqualTo(0);
    }

    /// <summary>Verifies a terminal notification queued before disposal is still delivered.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDisposedAfterCompletionIsQueued_ThenCompletionIsStillDelivered()
    {
        RecordingWitness<int> observer = new();
        TimerSinkState<int> state = new(observer);

        _ = state.QueueCompletedLocked();
        state.HandleDisposeLocked();
        state.Flush();

        await Assert.That(observer.Completions).IsEqualTo(1);
    }

    /// <summary>Recording observer used by the direct <c>TimerSinkState</c> tests.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    private sealed class RecordingWitness<T> : IObserver<T>
    {
        /// <summary>Gets the captured values.</summary>
        public List<T> Values { get; } = [];

        /// <summary>Gets the captured errors.</summary>
        public List<Exception> Errors { get; } = [];

        /// <summary>Gets the number of <c>OnCompleted</c> calls observed.</summary>
        public int Completions { get; private set; }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(T value) => Values.Add(value);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => Errors.Add(error);

        /// <inheritdoc/>
        public void OnCompleted() => Completions++;
    }
}
