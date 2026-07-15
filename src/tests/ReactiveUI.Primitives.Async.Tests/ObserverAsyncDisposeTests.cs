// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Disposables;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests for <see cref="WitnessAsync{T}"/> disposal behavior.</summary>
public sealed class ObserverAsyncDisposeTests
{
    /// <summary>The value emitted by the hopping source.</summary>
    private const int EmittedValue = 7;

    /// <summary>The value pushed after the external link has been cancelled; it must never be delivered.</summary>
    private const int PostCancellationValue = 8;

    /// <summary>
    /// How many times the dispose-versus-exiting-notification race is replayed. The interesting interleaving —
    /// the in-flight call count reaching zero between the disposer reading it and re-reading it after publishing
    /// its wait handle — is a nanosecond-wide window, so it is provoked repeatedly rather than once.
    /// </summary>
    private const int DisposeRaceAttempts = 256;

    /// <summary>Maximum time a reentrant dispose may take before it is treated as a deadlock.</summary>
    private static readonly TimeSpan DeadlockTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Verifies the reentrant dispose path lets an observer dispose itself from within its own in-flight
    /// notification without deadlocking, even after the notification continuation has hopped to a different thread.</summary>
    /// <returns>A task that completes when disposal finishes; faults on timeout if a self-join deadlock occurs.</returns>
    [Test]
    public async Task WhenDisposedReentrantlyFromOwnNotificationAfterThreadHop_ThenDoesNotDeadlock()
    {
        SelfDisposingObserver observer = new();

        await observer.OnNextAsync(1, CancellationToken.None).AsTask().WaitAsync(DeadlockTimeout);

        await Assert.That(observer.HasDisposed).IsTrue();
    }

    /// <summary>Verifies the terminal-sink reentrant dispose path completes when the result resolves during a
    /// notification whose continuation has hopped threads.</summary>
    /// <returns>A task to monitor completion.</returns>
    [Test]
    public async Task WhenFirstAsyncResolvesDuringHoppedNotification_ThenCompletes()
    {
        var source = SignalAsync.Create<int>(static async (observer, _) =>
        {
            await Task.Yield();
            await observer.OnNextAsync(EmittedValue, CancellationToken.None).ConfigureAwait(false);
            return DisposableAsync.Empty;
        });

        var value = await source.FirstAsync().AsTask().WaitAsync(DeadlockTimeout);

        await Assert.That(value).IsEqualTo(EmittedValue);
    }

    /// <summary>Verifies that linking an observer to its own dispose token is ignored: the guard must return
    /// before the existing external registration is torn down, so the token the observer was constructed with
    /// still disposes it.</summary>
    /// <returns>A task to monitor completion.</returns>
    [Test]
    public async Task WhenLinkedToItsOwnDisposeToken_ThenExternalLinkStillDisposesTheObserver()
    {
        using CancellationTokenSource external = new();
        RecordingObserver observer = new(external.Token);

        observer.LinkUpstreamCancellation(observer.InternalDisposedToken);

        await observer.OnNextAsync(EmittedValue, CancellationToken.None);
        await Assert.That(observer.Received).IsCollectionEqualTo([EmittedValue]);

        await external.CancelAsync();
        await Assert.That(observer.HasDisposed).IsTrue();

        await observer.OnNextAsync(PostCancellationValue, CancellationToken.None);
        await Assert.That(observer.Received).IsCollectionEqualTo([EmittedValue]);
    }

    /// <summary>Verifies that disposing an observer from one thread while a notification is still in flight on
    /// another never hangs, including when that notification's call count drops to zero inside the disposer's
    /// publish-then-recheck window — the case the disposer must self-signal to avoid waiting forever.</summary>
    /// <returns>A task that completes when every attempt has disposed; faults on timeout if a wait deadlocks.</returns>
    [Test]
    public async Task WhenDisposedFromAnotherThreadAsNotificationExits_ThenDoesNotDeadlock()
    {
        for (var attempt = 0; attempt < DisposeRaceAttempts; attempt++)
        {
            SpinningObserver observer = new();
            var notification = Task.Run(async () =>
                await observer.OnNextAsync(EmittedValue, CancellationToken.None));

            await observer.Entered.WaitAsync(DeadlockTimeout);
            await observer.DisposeAsync().AsTask().WaitAsync(DeadlockTimeout);
            await notification.WaitAsync(DeadlockTimeout);

            await Assert.That(observer.HasDisposed).IsTrue();
        }
    }

    /// <summary>Observer that records every value it is handed, constructed with an external dispose link.</summary>
    /// <param name="externalLink">The token whose cancellation disposes this observer.</param>
    private sealed class RecordingObserver(CancellationToken externalLink) : WitnessAsync<int>(externalLink)
    {
        /// <summary>Gets the values this observer was handed.</summary>
        internal List<int> Received { get; } = [];

        /// <inheritdoc/>
        protected override ValueTask OnNextAsyncCore(int value, CancellationToken cancellationToken)
        {
            Received.Add(value);
            return default;
        }

        /// <inheritdoc/>
        protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
            default;

        /// <inheritdoc/>
        protected override ValueTask OnCompletedAsyncCore(Result result) => default;
    }

    /// <summary>Observer whose notification stays in flight, spinning, until disposal releases it — so the call
    /// exits within nanoseconds of the disposer starting, rather than parking and exiting long afterwards.</summary>
    private sealed class SpinningObserver : WitnessAsync<int>
    {
        /// <summary>Upper bound on the spin the in-flight notification performs while waiting to be released.</summary>
        private const int MaxReleaseSpins = 10_000_000;

        /// <summary>Completes once the notification has been entered and the in-flight call count is non-zero.</summary>
        private readonly TaskCompletionSource _entered = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Non-zero once disposal has released the spinning notification.</summary>
        private int _released;

        /// <summary>Gets a task that completes once the notification is in flight.</summary>
        internal Task Entered => _entered.Task;

        /// <inheritdoc/>
        protected override ValueTask OnNextAsyncCore(int value, CancellationToken cancellationToken)
        {
            IgnoredResult.Of(_entered.TrySetResult());

            for (var spin = 0; spin < MaxReleaseSpins && Volatile.Read(ref _released) == 0; spin++)
            {
                Thread.SpinWait(1);
            }

            return default;
        }

        /// <inheritdoc/>
        protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
            default;

        /// <inheritdoc/>
        protected override ValueTask OnCompletedAsyncCore(Result result) => default;

        /// <inheritdoc/>
        protected override ValueTask DisposeAsyncCore()
        {
            Volatile.Write(ref _released, 1);
            return base.DisposeAsyncCore();
        }
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
        protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
            default;

        /// <inheritdoc/>
        protected override ValueTask OnCompletedAsyncCore(Result result) => default;
    }
}
