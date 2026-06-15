// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Extensions.Tests.Internal;

/// <summary>Direct RxVoid tests for <see cref="TimerSinkState{T}"/> — covers the terminal
/// idempotency guards across <c>HandleErrorLocked</c>, <c>HandleCompletedLocked</c>, and <c>HandleDisposeLocked</c>.</summary>
public class TimerSinkStateTests
{
    /// <summary>Verifies <c>HandleErrorLocked</c> forwards the error then marks the state done.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenHandleErrorLocked_ThenForwardsAndMarksDone()
    {
        RecordingWitness<int> observer = new();
        TimerSinkState<int> state = new(observer);
        InvalidOperationException expected = new("timer-error");

        state.HandleErrorLocked(expected);

        await Assert.That(state.Done).IsTrue();
        await Assert.That(observer.Errors).Count().IsEqualTo(1);
        await Assert.That(observer.Errors[0]).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies <c>HandleCompletedLocked</c> forwards completion then marks the state done.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenHandleCompletedLocked_ThenForwardsAndMarksDone()
    {
        RecordingWitness<int> observer = new();
        TimerSinkState<int> state = new(observer);

        state.HandleCompletedLocked();

        await Assert.That(state.Done).IsTrue();
        await Assert.That(observer.Completions).IsEqualTo(1);
    }

    /// <summary>Exercises the <c>HandleCompletedLocked</c> idempotency guard — once the state is
    /// already terminal, a second call returns without re-forwarding to the downstream.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenHandleCompletedLockedAfterError_ThenNoOp()
    {
        RecordingWitness<int> observer = new();
        TimerSinkState<int> state = new(observer);

        state.HandleErrorLocked(new InvalidOperationException("first"));
        state.HandleCompletedLocked();

        await Assert.That(observer.Completions).IsEqualTo(0);
        await Assert.That(observer.Errors).Count().IsEqualTo(1);
    }

    /// <summary>Exercises the <c>HandleErrorLocked</c> idempotency guard — once the state is
    /// already terminal, a second call returns without re-forwarding.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenHandleErrorLockedAfterCompleted_ThenNoOp()
    {
        RecordingWitness<int> observer = new();
        TimerSinkState<int> state = new(observer);

        state.HandleCompletedLocked();
        state.HandleErrorLocked(new InvalidOperationException("second"));

        await Assert.That(observer.Errors).IsEmpty();
        await Assert.That(observer.Completions).IsEqualTo(1);
    }

    /// <summary>Verifies <c>HandleDisposeLocked</c> marks the state done without forwarding any notification to the downstream.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenHandleDisposeLocked_ThenMarksDoneWithoutForwarding()
    {
        RecordingWitness<int> observer = new();
        TimerSinkState<int> state = new(observer);

        state.HandleDisposeLocked();

        await Assert.That(state.Done).IsTrue();
        await Assert.That(observer.Errors).IsEmpty();
        await Assert.That(observer.Completions).IsEqualTo(0);
    }

    /// <summary>Recording observer used by the direct <c>TimerSinkState</c> tests.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    private sealed class RecordingWitness<T> : IObserver<T>
    {
        /// <summary>Gets the captured errors.</summary>
        public List<Exception> Errors { get; } = [];

        /// <summary>Gets the number of <c>OnCompleted</c> calls observed.</summary>
        public int Completions { get; private set; }

        /// <inheritdoc/>
        public void OnNext(T value)
        {
        }

        /// <inheritdoc/>
        public void OnError(Exception error) => Errors.Add(error);

        /// <inheritdoc/>
        public void OnCompleted() => Completions++;
    }
}
