// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests the run and dispose state held by task-based subscriptions.</summary>
public sealed class TaskSignalStateTests
{
    /// <summary>The upper bound for a job to observe cancellation on a loaded machine.</summary>
    private static readonly TimeSpan CompletionTimeout = TimeSpan.FromSeconds(30);

    /// <summary>Disposing synchronously cancels the running job without waiting, and later disposals do nothing.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task DisposeCancelsTheJobWithoutWaiting()
    {
        TaskCompletionSource started = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource cancelled = new(TaskCreationOptions.RunContinuationsAsynchronously);
        ProbeSubscription subscription = new(
            async (_, cancellationToken) =>
            {
                started.SetResult();
                try
                {
                    await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    cancelled.SetResult();
                }
            },
            new RecordingObserver());

        subscription.Start();
        await started.Task.WaitAsync(CompletionTimeout);
        DisposeTwice(subscription);
        await subscription.DisposeAsync();
        await cancelled.Task.WaitAsync(CompletionTimeout);

        await Assert.That(cancelled.Task.IsCompletedSuccessfully).IsTrue();
    }

    /// <summary>Disposing asynchronously cancels the job and returns only once the job has finished.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task DisposeAsyncJoinsTheCancelledJob()
    {
        TaskCompletionSource started = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var finished = false;
        ProbeSubscription subscription = new(
            async (_, cancellationToken) =>
            {
                started.SetResult();
                try
                {
                    await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(false);
                }
                finally
                {
                    finished = true;
                }
            },
            new RecordingObserver());

        subscription.Start();
        await started.Task.WaitAsync(CompletionTimeout);
        await subscription.DisposeAsync();

        await Assert.That(finished).IsTrue();
    }

    /// <summary>Disposing a state whose job never started completes at once, synchronously or asynchronously.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task DisposingAnUnstartedStateCompletesAtOnce()
    {
        var ran = false;
        ProbeSubscription synchronous = new(
            (_, _) =>
            {
                ran = true;
                return default;
            },
            new RecordingObserver());
        ProbeSubscription asynchronous = new(
            (_, _) =>
            {
                ran = true;
                return default;
            },
            new RecordingObserver());

        DisposeTwice(synchronous);
        var disposal = asynchronous.DisposeAsync();

        await Assert.That(disposal.IsCompletedSuccessfully).IsTrue();
        await Assert.That(ran).IsFalse();
    }

    /// <summary>A job that throws completes the observer with a failure carrying the exception.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task JobThatThrowsCompletesTheObserverWithFailure()
    {
        InvalidOperationException expected = new("job");
        RecordingObserver observer = new();
        ProbeSubscription subscription = new((_, _) => throw expected, observer);

        await TaskSignalState.ExecuteAsync(subscription, observer, CancellationToken.None);

        await Assert.That(observer.Completion).IsNotNull();
        await Assert.That(observer.Completion!.Value.Exception).IsSameReferenceAs(expected);
    }

    /// <summary>Disposes a subscription synchronously twice.</summary>
    /// <param name="subscription">The subscription to dispose.</param>
    private static void DisposeTwice(ProbeSubscription subscription)
    {
        subscription.Dispose();
        subscription.Dispose();
    }

    /// <summary>A subscription holding the state around a job supplied by the test.</summary>
    /// <param name="job">The job to run.</param>
    /// <param name="observer">The observer receiving the job's notifications.</param>
    private sealed class ProbeSubscription(Func<IObserverAsync<int>, CancellationToken, ValueTask> job, IObserverAsync<int> observer) : ITaskSignalJob<int>, IAsyncDisposable, IDisposable
    {
        /// <summary>The run and dispose state.</summary>
        private readonly TaskSignalState _task = new();

        /// <summary>Starts the job.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Start() => _task.Start(this, observer);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose() => _task.Dispose();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => _task.DisposeAsync();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask ExecuteAsync(IObserverAsync<int> observer, CancellationToken cancellationToken) => job(observer, cancellationToken);
    }

    /// <summary>An observer that records its completion.</summary>
    private sealed class RecordingObserver : IObserverAsync<int>
    {
        /// <summary>Gets the recorded completion, if any.</summary>
        public Result? Completion { get; private set; }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask OnNextAsync(int value, CancellationToken cancellationToken) => default;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken) => default;

        /// <inheritdoc/>
        public ValueTask OnCompletedAsync(Result result)
        {
            Completion = result;
            return default;
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => default;
    }
}
