// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Internals;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests for <see cref="TaskSignalSubscription{T}"/> lifecycle and disposal behavior.</summary>
public sealed class TaskSignalSubscriptionTests
{
    /// <summary>Verifies a reentrant dispose issued from within the job's own async flow does not deadlock,
    /// even after the notification continuation has hopped to a different thread.</summary>
    /// <returns>A task that completes when the subscription disposes; faults on timeout if a deadlock occurs.</returns>
    [Test]
    public async Task WhenDisposedReentrantlyAfterThreadHop_ThenDoesNotDeadlock()
    {
        TaskSignalSubscription<int>? subscription = null;
        var subscriptionReady = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var disposed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var observer = new ReentrantDisposingObserver(async () =>
        {
            await subscriptionReady.Task.ConfigureAwait(false);
            await Task.Yield();
            await subscription!.DisposeAsync().ConfigureAwait(false);
            disposed.SetResult();
        });

        subscription = TaskSignalSubscription.StartNew<int>(
            static async (obs, ct) => await obs.OnNextAsync(1, ct).ConfigureAwait(false),
            observer);
        subscriptionReady.SetResult();

        await disposed.Task.WaitAsync(TimeSpan.FromSeconds(5));
    }

    /// <summary>Observer that runs a supplied callback from its OnNext notification.</summary>
    /// <param name="onNext">The callback invoked from OnNext.</param>
    private sealed class ReentrantDisposingObserver(Func<ValueTask> onNext) : IObserverAsync<int>
    {
        /// <inheritdoc/>
        public ValueTask OnNextAsync(int value, CancellationToken cancellationToken) => onNext();

        /// <inheritdoc/>
        public ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken) => default;

        /// <inheritdoc/>
        public ValueTask OnCompletedAsync(Result result) => default;

        /// <inheritdoc/>
        public ValueTask DisposeAsync() => default;
    }
}
