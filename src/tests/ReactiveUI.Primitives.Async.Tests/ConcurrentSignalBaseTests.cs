// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Immutable;
using System.Threading.Tasks.Sources;
using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Coverage for the static fan-out helpers in
/// <see cref="Concurrent"/> — exercises empty / single / multi-observer paths and the
/// slow-path that uses <see cref="Task.WhenAll(Task[])"/> when at
/// least one observer's <see cref="ValueTask"/> hasn't completed synchronously.</summary>
public class ConcurrentSignalBaseTests
{
    /// <summary>Value forwarded by the <c>OnNext</c> fan-out tests.</summary>
    private const int ForwardedValue = 42;

    /// <summary>Delay in milliseconds used to force the slow-path branch.</summary>
    private const int SlowPathDelayMilliseconds = 5;

    /// <summary>Verifies that <c>ForwardOnNextConcurrently</c> with an empty observer list returns immediately.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRelayNextAsyncEmpty_ThenCompletesImmediately()
    {
        var empty = ImmutableArray<IObserverAsync<int>>.Empty;

        await Concurrent.ForwardOnNextConcurrently(empty, ForwardedValue, default);

        // No observers → nothing to assert beyond reaching this line without throwing.
        await Assert.That(empty.Length).IsEqualTo(0);
    }

    /// <summary>Verifies that <c>ForwardOnNextConcurrently</c> with a single observer forwards once.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRelayNextAsyncSingleObserver_ThenForwardsOnce()
    {
        IntCapture capture = new();
        ImmutableArray<IObserverAsync<int>> observers = [MakeSync(capture)];

        await Concurrent.ForwardOnNextConcurrently(observers, ForwardedValue, default);

        await Assert.That(capture.Value).IsEqualTo(ForwardedValue);
    }

    /// <summary>Verifies that the synchronous-fast-path branch fires every observer when each returns a synchronously-completed <see cref="ValueTask"/>.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRelayNextAsyncMultipleSync_ThenAllReceive()
    {
        IntCapture a = new();
        IntCapture b = new();
        IntCapture c = new();

        ImmutableArray<IObserverAsync<int>> observers =
        [
            MakeSync(a),
            MakeSync(b),
            MakeSync(c)
        ];

        await Concurrent.ForwardOnNextConcurrently(observers, ForwardedValue, default);

        await Assert.That(a.Value).IsEqualTo(ForwardedValue);
        await Assert.That(b.Value).IsEqualTo(ForwardedValue);
        await Assert.That(c.Value).IsEqualTo(ForwardedValue);
    }

    /// <summary>Verifies that the slow-path branch (Task.WhenAll) runs every observer when any of them returns a non-completed <see cref="ValueTask"/>.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRelayNextAsyncSlowPath_ThenWhenAllForwarded()
    {
        IntCapture a = new();
        IntCapture b = new();
        IntCapture c = new();

        ImmutableArray<IObserverAsync<int>> observers =
        [
            MakeSlow(a),
            MakeSync(b),
            MakeSlow(c)
        ];

        await Concurrent.ForwardOnNextConcurrently(observers, ForwardedValue, default);

        await Assert.That(a.Value).IsEqualTo(ForwardedValue);
        await Assert.That(b.Value).IsEqualTo(ForwardedValue);
        await Assert.That(c.Value).IsEqualTo(ForwardedValue);
    }

    /// <summary>Verifies the empty / single / slow-path branches of <c>ForwardOnErrorResumeConcurrently</c>.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRelayErrorAsync_ThenAllBranchesForward()
    {
        var emptyObservers = ImmutableArray<IObserverAsync<int>>.Empty;
        await Concurrent.ForwardOnErrorResumeConcurrently(
            emptyObservers,
            new InvalidOperationException("empty"),
            default);

        ErrorCapture singleCaught = new();
        ImmutableArray<IObserverAsync<int>> single =
        [
            new CallbackWitnessAsync<int>(static (_, _) => default, MakeErrorSync(singleCaught))
        ];
        InvalidOperationException singleError = new("single");
        await Concurrent.ForwardOnErrorResumeConcurrently(single, singleError, default);
        await Assert.That(singleCaught.Error).IsSameReferenceAs(singleError);

        ErrorCapture a = new();
        ErrorCapture b = new();
        ImmutableArray<IObserverAsync<int>> multi =
        [
            new CallbackWitnessAsync<int>(static (_, _) => default, MakeErrorSlow(a)),
            new CallbackWitnessAsync<int>(static (_, _) => default, MakeErrorSync(b))
        ];
        InvalidOperationException multiError = new("multi");
        await Concurrent.ForwardOnErrorResumeConcurrently(multi, multiError, default);
        await Assert.That(a.Error).IsSameReferenceAs(multiError);
        await Assert.That(b.Error).IsSameReferenceAs(multiError);
    }

    /// <summary>Verifies the empty / single / slow-path branches of <c>ForwardOnCompletedConcurrently</c>.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenForwardOnCompleted_ThenAllBranchesForward()
    {
        var emptyObservers = ImmutableArray<IObserverAsync<int>>.Empty;
        await Concurrent.ForwardOnCompletedConcurrently(emptyObservers, Result.Success);

        ResultCapture singleResult = new();
        ImmutableArray<IObserverAsync<int>> single =
        [
            new CallbackWitnessAsync<int>(static (_, _) => default, null, MakeCompletedSync(singleResult))
        ];
        await Concurrent.ForwardOnCompletedConcurrently(single, Result.Success);
        await Assert.That(singleResult.Result).IsEqualTo(Result.Success);

        ResultCapture a = new();
        ResultCapture b = new();
        ImmutableArray<IObserverAsync<int>> multi =
        [
            new CallbackWitnessAsync<int>(static (_, _) => default, null, MakeCompletedSlow(a)),
            new CallbackWitnessAsync<int>(static (_, _) => default, null, MakeCompletedSync(b))
        ];
        await Concurrent.ForwardOnCompletedConcurrently(multi, Result.Success);
        await Assert.That(a.Result).IsEqualTo(Result.Success);
        await Assert.That(b.Result).IsEqualTo(Result.Success);
    }

    /// <summary>Verifies that serial error broadcasting resumes remaining observers after an asynchronous first observer.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSerialErrorBroadcastSlowPath_ThenRemainingObserversReceiveError()
    {
        ErrorCapture a = new();
        ErrorCapture b = new();
        ImmutableArray<IObserverAsync<int>> observers =
        [
            new CallbackWitnessAsync<int>(static (_, _) => default, MakeErrorSlow(a)),
            new CallbackWitnessAsync<int>(static (_, _) => default, MakeErrorSync(b))
        ];
        InvalidOperationException error = new("serial-error");

        await SerialBroadcastHelpers.BroadcastOnErrorResumeAsync(observers, error, default);

        await Assert.That(a.Error).IsSameReferenceAs(error);
        await Assert.That(b.Error).IsSameReferenceAs(error);
    }

    /// <summary>Verifies that serial completion broadcasting resumes remaining observers after an asynchronous first observer.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSerialCompletionBroadcastSlowPath_ThenRemainingObserversReceiveResult()
    {
        ResultCapture a = new();
        ResultCapture b = new();
        ImmutableArray<IObserverAsync<int>> observers =
        [
            new CallbackWitnessAsync<int>(static (_, _) => default, null, MakeCompletedSlow(a)),
            new CallbackWitnessAsync<int>(static (_, _) => default, null, MakeCompletedSync(b))
        ];

        await SerialBroadcastHelpers.BroadcastOnCompletedAsync(observers, Result.Success);

        await Assert.That(a.Result).IsEqualTo(Result.Success);
        await Assert.That(b.Result).IsEqualTo(Result.Success);
    }

    /// <summary>Verifies that serial synchronous fast-path broadcasts consume every completed <see cref="ValueTask"/>.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSerialBroadcastSyncValueTaskSources_ThenConsumesEachCompletedValueTask()
    {
        CompletedValueTaskSource nextFirst = new();
        CompletedValueTaskSource nextSecond = new();
        ImmutableArray<IObserverAsync<int>> nextObservers =
        [
            new ValueTaskSourceObserver(nextFirst.CreateTask),
            new ValueTaskSourceObserver(nextSecond.CreateTask)
        ];

        await SerialBroadcastHelpers.BroadcastOnNextAsyncMulti(nextObservers, ForwardedValue, default);

        await Assert.That(nextFirst.GetResultCount).IsEqualTo(1);
        await Assert.That(nextSecond.GetResultCount).IsEqualTo(1);

        CompletedValueTaskSource errorFirst = new();
        CompletedValueTaskSource errorSecond = new();
        ImmutableArray<IObserverAsync<int>> errorObservers =
        [
            new ValueTaskSourceObserver(onError: errorFirst.CreateTask),
            new ValueTaskSourceObserver(onError: errorSecond.CreateTask)
        ];

        await SerialBroadcastHelpers.BroadcastOnErrorResumeAsync(
            errorObservers,
            new InvalidOperationException("sync-source"),
            default);

        await Assert.That(errorFirst.GetResultCount).IsEqualTo(1);
        await Assert.That(errorSecond.GetResultCount).IsEqualTo(1);

        CompletedValueTaskSource completedFirst = new();
        CompletedValueTaskSource completedSecond = new();
        ImmutableArray<IObserverAsync<int>> completedObservers =
        [
            new ValueTaskSourceObserver(onCompleted: completedFirst.CreateTask),
            new ValueTaskSourceObserver(onCompleted: completedSecond.CreateTask)
        ];

        await SerialBroadcastHelpers.BroadcastOnCompletedAsync(completedObservers, Result.Success);

        await Assert.That(completedFirst.GetResultCount).IsEqualTo(1);
        await Assert.That(completedSecond.GetResultCount).IsEqualTo(1);
    }

    /// <summary>Creates a synchronously-completing OnNext observer that captures the value.</summary>
    /// <param name="capture">The capture sink.</param>
    /// <returns>An observer whose <c>OnNextAsync</c> completes synchronously.</returns>
    private static CallbackWitnessAsync<int> MakeSync(IntCapture capture) =>
        new((x, _) =>
        {
            capture.Value = x;
            return default;
        });

    /// <summary>Creates an OnNext observer that delays before capturing — forces the slow path.</summary>
    /// <param name="capture">The capture sink.</param>
    /// <returns>An observer whose <c>OnNextAsync</c> completes asynchronously.</returns>
    private static CallbackWitnessAsync<int> MakeSlow(IntCapture capture) =>
        new(async (x, ct) =>
        {
            await Task.Delay(SlowPathDelayMilliseconds, ct).ConfigureAwait(false);
            capture.Value = x;
        });

    /// <summary>Synchronously-completing OnErrorResume handler that records the exception.</summary>
    /// <param name="capture">The capture sink.</param>
    /// <returns>An OnErrorResume delegate.</returns>
    private static Func<Exception, CancellationToken, ValueTask> MakeErrorSync(ErrorCapture capture) =>
        (ex, _) =>
        {
            capture.Error = ex;
            return default;
        };

    /// <summary>OnErrorResume handler that delays before recording — forces the slow path.</summary>
    /// <param name="capture">The capture sink.</param>
    /// <returns>An OnErrorResume delegate.</returns>
    private static Func<Exception, CancellationToken, ValueTask> MakeErrorSlow(ErrorCapture capture) =>
        async (ex, ct) =>
        {
            await Task.Delay(SlowPathDelayMilliseconds, ct).ConfigureAwait(false);
            capture.Error = ex;
        };

    /// <summary>Synchronously-completing OnCompleted handler that records the result.</summary>
    /// <param name="capture">The capture sink.</param>
    /// <returns>An OnCompleted delegate.</returns>
    private static Func<Result, ValueTask> MakeCompletedSync(ResultCapture capture) =>
        r =>
        {
            capture.Result = r;
            return default;
        };

    /// <summary>OnCompleted handler that delays before recording — forces the slow path.</summary>
    /// <param name="capture">The capture sink.</param>
    /// <returns>An OnCompleted delegate.</returns>
    private static Func<Result, ValueTask> MakeCompletedSlow(ResultCapture capture) =>
        async r =>
        {
            await Task.Delay(SlowPathDelayMilliseconds, CancellationToken.None).ConfigureAwait(false);
            capture.Result = r;
        };

    /// <summary>Mutable holder for an <see cref="int"/> captured by an observer delegate.</summary>
    private sealed class IntCapture
    {
        /// <summary>Gets or sets the captured value.</summary>
        public int Value { get; set; }
    }

    /// <summary>Mutable holder for an <see cref="Exception"/> captured by an observer delegate.</summary>
    private sealed class ErrorCapture
    {
        /// <summary>Gets or sets the captured exception.</summary>
        public Exception? Error { get; set; }
    }

    /// <summary>Mutable holder for a <see cref="Result"/> captured by an observer delegate.</summary>
    private sealed class ResultCapture
    {
        /// <summary>Gets or sets the captured result.</summary>
        public Result? Result { get; set; }
    }

    /// <summary>Observer that returns caller-supplied <see cref="ValueTask"/> instances for selected notifications.</summary>
    /// <param name="onNext">The <see cref="IObserverAsync{T}.OnNextAsync"/> value-task factory.</param>
    /// <param name="onError">The resumable-error source factory.</param>
    /// <param name="onCompleted">The terminal-completion source factory.</param>
    private sealed class ValueTaskSourceObserver(
        Func<ValueTask>? onNext = null,
        Func<ValueTask>? onError = null,
        Func<ValueTask>? onCompleted = null) : IObserverAsync<int>
    {
        /// <inheritdoc/>
        public ValueTask DisposeAsync() => default;

        /// <inheritdoc/>
        public ValueTask OnCompletedAsync(Result result) =>
            onCompleted?.Invoke() ?? default;

        /// <inheritdoc/>
        public ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken) =>
            onError?.Invoke() ?? default;

        /// <inheritdoc/>
        public ValueTask OnNextAsync(int value, CancellationToken cancellationToken) =>
            onNext?.Invoke() ?? default;
    }

    /// <summary>Synchronously-completed <see cref="IValueTaskSource"/> that tracks exact <c>GetResult</c> consumption.</summary>
    private sealed class CompletedValueTaskSource : IValueTaskSource
    {
        /// <summary>The number of times <see cref="GetResult"/> has been called.</summary>
        private int _getResultCount;

        /// <summary>Gets the number of times <see cref="GetResult"/> has been called.</summary>
        public int GetResultCount => Volatile.Read(ref _getResultCount);

        /// <summary>Creates a <see cref="ValueTask"/> backed by this source.</summary>
        /// <returns>The synchronously-completed task.</returns>
        public ValueTask CreateTask() => new(this, 0);

        /// <inheritdoc/>
        public void GetResult(short token)
        {
            if (Interlocked.Increment(ref _getResultCount) == 1)
            {
                return;
            }

            throw new InvalidOperationException("The completed ValueTask source was consumed more than once.");
        }

        /// <inheritdoc/>
        public ValueTaskSourceStatus GetStatus(short token) =>
            ValueTaskSourceStatus.Succeeded;

        /// <inheritdoc/>
        public void OnCompleted(
            Action<object?> continuation,
            object? state,
            short token,
            ValueTaskSourceOnCompletedFlags flags) =>
            throw new InvalidOperationException("Synchronous completions must not register continuations.");
    }
}
