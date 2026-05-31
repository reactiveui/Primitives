// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions.Tests.Internal;

/// <summary>Direct RxVoid tests for <see cref="TimerSinkState{T}"/> — covers the terminal
/// idempotency guards across <c>HandleError</c>, <c>HandleCompleted</c>, and <c>HandleDispose</c>.</summary>
public class TimerSinkStateTests
{
    /// <summary>Verifies <c>HandleError</c> forwards the error then marks the state done.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenHandleError_ThenForwardsAndMarksDone()
    {
        var observer = new RecordingObserver<int>();
        var state = new TimerSinkState<int>(observer);
        var expected = new InvalidOperationException("timer-error");

        state.HandleError(expected);

        await Assert.That(state.Done).IsTrue();
        await Assert.That(observer.Errors).Count().IsEqualTo(1);
        await Assert.That(observer.Errors[0]).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies <c>HandleCompleted</c> forwards completion then marks the state done.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenHandleCompleted_ThenForwardsAndMarksDone()
    {
        var observer = new RecordingObserver<int>();
        var state = new TimerSinkState<int>(observer);

        state.HandleCompleted();

        await Assert.That(state.Done).IsTrue();
        await Assert.That(observer.Completions).IsEqualTo(1);
    }

    /// <summary>Exercises the <c>HandleCompleted</c> idempotency guard — once the state is
    /// already terminal, a second call returns without re-forwarding to the downstream.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenHandleCompletedAfterError_ThenNoOp()
    {
        var observer = new RecordingObserver<int>();
        var state = new TimerSinkState<int>(observer);

        state.HandleError(new InvalidOperationException("first"));
        state.HandleCompleted();

        await Assert.That(observer.Completions).IsEqualTo(0);
        await Assert.That(observer.Errors).Count().IsEqualTo(1);
    }

    /// <summary>Exercises the <c>HandleError</c> idempotency guard — once the state is
    /// already terminal, a second call returns without re-forwarding.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenHandleErrorAfterCompleted_ThenNoOp()
    {
        var observer = new RecordingObserver<int>();
        var state = new TimerSinkState<int>(observer);

        state.HandleCompleted();
        state.HandleError(new InvalidOperationException("second"));

        await Assert.That(observer.Errors).IsEmpty();
        await Assert.That(observer.Completions).IsEqualTo(1);
    }

    /// <summary>Verifies <c>HandleDispose</c> marks the state done without forwarding any
    /// notification to the downstream.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenHandleDispose_ThenMarksDoneWithoutForwarding()
    {
        var observer = new RecordingObserver<int>();
        var state = new TimerSinkState<int>(observer);

        state.HandleDispose();

        await Assert.That(state.Done).IsTrue();
        await Assert.That(observer.Errors).IsEmpty();
        await Assert.That(observer.Completions).IsEqualTo(0);
    }

    /// <summary>Recording observer used by the direct <c>TimerSinkState</c> tests.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    private sealed class RecordingObserver<T> : IObserver<T>
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
