// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Disposables;
using ReactiveUI.Primitives.Async.Internals;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests for <see cref="WitnessAsync{T}"/> disposal behavior.</summary>
public sealed class ObserverAsyncDisposeTests
{
    /// <summary>The value emitted by the hopping source.</summary>
    private const int EmittedValue = 7;

    /// <summary>Verifies the reentrant dispose path lets an observer dispose itself from within its own in-flight
    /// notification without deadlocking, even after the notification continuation has hopped to a different thread.</summary>
    /// <returns>A task that completes when disposal finishes; faults on timeout if a self-join deadlock occurs.</returns>
    [Test]
    public async Task WhenDisposedReentrantlyFromOwnNotificationAfterThreadHop_ThenDoesNotDeadlock()
    {
        var observer = new SelfDisposingObserver();

        await observer.OnNextAsync(1, CancellationToken.None).AsTask().WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.That(observer.HasDisposed).IsTrue();
    }

    /// <summary>Verifies the terminal-sink reentrant dispose path completes when the result resolves during a
    /// notification whose continuation has hopped threads.</summary>
    /// <returns>A task to monitor completion.</returns>
    [Test]
    public async Task WhenFirstAsyncResolvesDuringHoppedNotification_ThenCompletes()
    {
        var source = SignalAsync.Create<int>(async (observer, _) =>
        {
            await Task.Yield();
            await observer.OnNextAsync(EmittedValue, CancellationToken.None).ConfigureAwait(false);
            return DisposableAsync.Empty;
        });

        var value = await source.FirstAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.That(value).IsEqualTo(EmittedValue);
    }

    /// <summary>Observer whose OnNext hops threads then disposes itself via the reentrant path.</summary>
    private sealed class SelfDisposingObserver : WitnessAsync<int>
    {
        /// <inheritdoc/>
        protected override async ValueTask OnNextAsyncCore(int value, CancellationToken cancellationToken)
        {
            await Task.Yield();
            await ((IReentrantAsyncDisposable)this).DisposeFromNotificationAsync().ConfigureAwait(false);
        }

        /// <inheritdoc/>
        protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) => default;

        /// <inheritdoc/>
        protected override ValueTask OnCompletedAsyncCore(Result result) => default;
    }
}
