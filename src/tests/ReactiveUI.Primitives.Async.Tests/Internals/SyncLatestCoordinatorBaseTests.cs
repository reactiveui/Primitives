// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Internals;

namespace ReactiveUI.Primitives.Async.Tests.Internals;

/// <summary>Tests for <see cref="SyncLatestCoordinatorBase{TResult}"/>, the shared scaffolding derived by every <c>CombineLatestN</c> arity-specific subscription.</summary>
public class SyncLatestCoordinatorBaseTests
{
    /// <summary>Verifies that the base wires its <see cref="SyncLatestLifecycle{TResult}"/> with the supplied observer and source count.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConstructed_ThenLifecycleSlotsSized()
    {
        const int SourceCount = 3;
        var captured = new CaptureObserverAsync<int>();
        var subscription = new TestSubscription(captured, SourceCount);

        await Assert.That(subscription.Lifecycle.Subscriptions).Count().IsEqualTo(SourceCount);
        await Assert.That(subscription.Lifecycle.HasDisposed).IsFalse();
    }

    /// <summary>Verifies that <see cref="SyncLatestCoordinatorBase{TResult}.SubscribeSourcesAsync"/> drives the per-arity <c>SubscribeAtAsync</c> for every source index in order.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribeSourcesAsync_ThenSubscribeAtCalledForEveryIndex()
    {
        const int SourceCount = 3;
        const int FirstIndex = 0;
        const int SecondIndex = 1;
        const int ThirdIndex = 2;
        var captured = new CaptureObserverAsync<int>();
        var subscription = new TestSubscription(captured, SourceCount);

        await subscription.SubscribeSourcesAsync(CancellationToken.None);

        await Assert.That(subscription.SubscribedIndices).IsCollectionEqualTo([FirstIndex, SecondIndex, ThirdIndex]);
        await Assert.That(subscription.Lifecycle.Subscriptions[FirstIndex]).IsNotNull();
        await Assert.That(subscription.Lifecycle.Subscriptions[SecondIndex]).IsNotNull();
        await Assert.That(subscription.Lifecycle.Subscriptions[ThirdIndex]).IsNotNull();

        await subscription.DisposeAsync();
    }

    /// <summary>Verifies that <see cref="SyncLatestCoordinatorBase{TResult}.RelaySourceErrorAsync"/> forwards the error through the lifecycle to the downstream observer.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnErrorResume_ThenForwardsViaLifecycle()
    {
        var captured = new CaptureObserverAsync<int>();
        var subscription = new TestSubscription(captured, sourceCount: 1);
        var expected = new InvalidOperationException("forward");

        await subscription.RelaySourceErrorAsync(expected, CancellationToken.None);

        await Assert.That(captured.Errors).Count().IsEqualTo(1);
        await Assert.That(captured.Errors[0]).IsEqualTo(expected);

        await subscription.DisposeAsync();
    }

    /// <summary>Verifies that <see cref="SyncLatestCoordinatorBase{TResult}.DisposeAsync"/> disposes the underlying lifecycle so subsequent disposal is idempotent.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDisposeAsync_ThenLifecycleDisposed()
    {
        var captured = new CaptureObserverAsync<int>();
        var subscription = new TestSubscription(captured, sourceCount: 2);

        await subscription.DisposeAsync();
        await subscription.DisposeAsync(); // idempotent

        await Assert.That(subscription.Lifecycle.HasDisposed).IsTrue();
    }

    /// <summary>Verifies that <c>Lifecycle.LinkExternalCancellation</c> short-circuits when the
    /// supplied token cannot be cancelled or already equals the dispose token.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenLinkExternalCancellationNonCancellable_ThenNoOp()
    {
        var captured = new CaptureObserverAsync<int>();
        var subscription = new TestSubscription(captured, sourceCount: 1);

        // CancellationToken.None — can't be cancelled, helper should bail out early.
        subscription.Lifecycle.LinkExternalCancellation(CancellationToken.None);

        await Assert.That(subscription.Lifecycle.HasDisposed).IsFalse();
        await subscription.DisposeAsync();
    }

    /// <summary>Verifies that <c>Lifecycle.LinkExternalCancellation</c> cancels the dispose CTS
    /// immediately when the supplied token is already cancelled.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenLinkExternalCancellationAlreadyCancelled_ThenDisposeTokenFires()
    {
        var captured = new CaptureObserverAsync<int>();
        var subscription = new TestSubscription(captured, sourceCount: 1);
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        subscription.Lifecycle.LinkExternalCancellation(cts.Token);

        await Assert.That(subscription.Lifecycle.DisposeToken.IsCancellationRequested).IsTrue();
        await subscription.DisposeAsync();
    }

    /// <summary>Verifies that <c>Lifecycle.LinkExternalCancellation</c> registers a callback so a
    /// later cancellation on the external token cancels the dispose CTS.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenLinkExternalCancellationCancellable_ThenLaterCancelPropagates()
    {
        var captured = new CaptureObserverAsync<int>();
        var subscription = new TestSubscription(captured, sourceCount: 1);
        using var cts = new CancellationTokenSource();

        subscription.Lifecycle.LinkExternalCancellation(cts.Token);
        await Assert.That(subscription.Lifecycle.DisposeToken.IsCancellationRequested).IsFalse();

        await cts.CancelAsync();
        await Assert.That(subscription.Lifecycle.DisposeToken.IsCancellationRequested).IsTrue();

        await subscription.DisposeAsync();
    }

    /// <summary>Verifies <see cref="SyncLatestCoordinatorBase{TResult}.ValuesLock"/> is usable as a <c>lock</c> target on both paths.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenLockOnValuesLock_ThenNoThrow()
    {
        var captured = new CaptureObserverAsync<int>();
        var subscription = new TestSubscription(captured, sourceCount: 1);
        var entered = false;

        subscription.LockAndDo(() => entered = true);

        await Assert.That(entered).IsTrue();
        await subscription.DisposeAsync();
    }

    /// <summary>Minimal concrete subclass exposing the base's protected surface for testing.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="sourceCount">The number of upstream sources.</param>
    private sealed class TestSubscription(IObserverAsync<int> observer, int sourceCount)
        : SyncLatestCoordinatorBase<int>(observer, sourceCount)
    {
        /// <summary>Gets the indices passed to <see cref="SubscribeAtAsync"/> in order.</summary>
        public List<int> SubscribedIndices { get; } = [];

        /// <summary>Invokes <paramref name="action"/> while holding the protected <c>ValuesLock</c>.</summary>
        /// <param name="action">Action invoked while the values-lock is held.</param>
        public void LockAndDo(Action action)
        {
            lock (ValuesLock)
            {
                action();
            }
        }

        /// <inheritdoc/>
        internal override ValueTask EmitLatestAsync() => default;

        /// <inheritdoc/>
        protected override ValueTask<IAsyncDisposable> SubscribeAtAsync(int index, CancellationToken cancellationToken)
        {
            _ = cancellationToken;
            SubscribedIndices.Add(index);
            return new(NoopDisposable.Instance);
        }
    }

    /// <summary>No-op async disposable handed back from the test subscription.</summary>
    private sealed class NoopDisposable : IAsyncDisposable
    {
        /// <summary>Gets the shared no-op singleton.</summary>
        public static IAsyncDisposable Instance { get; } = new NoopDisposable();

        /// <inheritdoc/>
        public ValueTask DisposeAsync() => default;
    }

    /// <summary>Capture observer for the helper tests.</summary>
    /// <typeparam name="T">Element type captured.</typeparam>
    private sealed class CaptureObserverAsync<T> : IObserverAsync<T>
    {
        /// <summary>Gets the captured OnNext values in order.</summary>
        public List<T> Values { get; } = [];

        /// <summary>Gets the captured error-resume exceptions in order.</summary>
        public List<Exception> Errors { get; } = [];

        /// <summary>Gets the captured OnCompleted results in order.</summary>
        public List<Result> Completions { get; } = [];

        /// <inheritdoc/>
        public ValueTask OnNextAsync(T value, CancellationToken cancellationToken)
        {
            _ = cancellationToken;
            Values.Add(value);
            return default;
        }

        /// <inheritdoc/>
        public ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken)
        {
            _ = cancellationToken;
            Errors.Add(error);
            return default;
        }

        /// <inheritdoc/>
        public ValueTask OnCompletedAsync(Result result)
        {
            Completions.Add(result);
            return default;
        }

        /// <inheritdoc/>
        public ValueTask DisposeAsync() => default;
    }
}
