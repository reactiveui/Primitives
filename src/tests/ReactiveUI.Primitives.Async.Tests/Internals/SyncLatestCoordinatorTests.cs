// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Tests.Internals;

/// <summary>Tests for <see cref="SyncLatestCoordinator"/>, the shared subscribe loop, and the <see cref="SyncLatestLifecycle{TResult}"/> every <c>SyncLatest</c> coordinator holds.</summary>
public class SyncLatestCoordinatorTests
{
    /// <summary>Verifies that the lifecycle is sized for the supplied source count.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConstructed_ThenLifecycleSlotsSized()
    {
        const int SourceCount = 3;
        CaptureObserverAsync<int> captured = new();
        TestCoordinator coordinator = new(captured, SourceCount);

        await Assert.That(coordinator.Lifecycle.Subscriptions).Count().IsEqualTo(SourceCount);
        await Assert.That(coordinator.Lifecycle.HasDisposed).IsFalse();
    }

    /// <summary>Verifies that <see cref="SyncLatestCoordinator.SubscribeSourcesAsync{TResult}"/> subscribes every source index in order.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribeSourcesAsync_ThenSubscribeAtCalledForEveryIndex()
    {
        const int SourceCount = 3;
        const int FirstIndex = 0;
        const int SecondIndex = 1;
        const int ThirdIndex = 2;
        CaptureObserverAsync<int> captured = new();
        TestCoordinator coordinator = new(captured, SourceCount);

        await SyncLatestCoordinator.SubscribeSourcesAsync(coordinator, CancellationToken.None);

        await Assert.That(coordinator.SubscribedIndices).IsCollectionEqualTo([FirstIndex, SecondIndex, ThirdIndex]);
        await Assert.That(coordinator.Lifecycle.Subscriptions[FirstIndex]).IsNotNull();
        await Assert.That(coordinator.Lifecycle.Subscriptions[SecondIndex]).IsNotNull();
        await Assert.That(coordinator.Lifecycle.Subscriptions[ThirdIndex]).IsNotNull();

        await coordinator.DisposeAsync();
    }

    /// <summary>Verifies that disposing the lifecycle is idempotent.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDisposeAsync_ThenLifecycleDisposed()
    {
        const int SourceCount = 2;

        CaptureObserverAsync<int> captured = new();
        TestCoordinator coordinator = new(captured, SourceCount);

        await coordinator.DisposeAsync();
        await coordinator.DisposeAsync();

        await Assert.That(coordinator.Lifecycle.HasDisposed).IsTrue();
    }

    /// <summary>Verifies that <c>Lifecycle.LinkExternalCancellation</c> short-circuits when the supplied token cannot be cancelled.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenLinkExternalCancellationNonCancellable_ThenNoOp()
    {
        CaptureObserverAsync<int> captured = new();
        TestCoordinator coordinator = new(captured, 1);

        coordinator.Lifecycle.LinkExternalCancellation(CancellationToken.None);

        await Assert.That(coordinator.Lifecycle.HasDisposed).IsFalse();
        await coordinator.DisposeAsync();
    }

    /// <summary>Verifies that <c>Lifecycle.LinkExternalCancellation</c> cancels the dispose token immediately when the supplied token is already cancelled.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenLinkExternalCancellationAlreadyCancelled_ThenDisposeTokenFires()
    {
        CaptureObserverAsync<int> captured = new();
        TestCoordinator coordinator = new(captured, 1);
        using CancellationTokenSource cts = new();
        await cts.CancelAsync();

        coordinator.Lifecycle.LinkExternalCancellation(cts.Token);

        await Assert.That(coordinator.Lifecycle.DisposeToken.IsCancellationRequested).IsTrue();
        await coordinator.DisposeAsync();
    }

    /// <summary>Verifies that <c>Lifecycle.LinkExternalCancellation</c> propagates a later cancellation of the external token.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenLinkExternalCancellationCancellable_ThenLaterCancelPropagates()
    {
        CaptureObserverAsync<int> captured = new();
        TestCoordinator coordinator = new(captured, 1);
        using CancellationTokenSource cts = new();

        coordinator.Lifecycle.LinkExternalCancellation(cts.Token);
        await Assert.That(coordinator.Lifecycle.DisposeToken.IsCancellationRequested).IsFalse();

        await cts.CancelAsync();
        await Assert.That(coordinator.Lifecycle.DisposeToken.IsCancellationRequested).IsTrue();

        await coordinator.DisposeAsync();
    }

    /// <summary>Minimal coordinator recording the indices it was asked to subscribe.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="sourceCount">The number of upstream sources.</param>
    private sealed class TestCoordinator(IObserverAsync<int> observer, int sourceCount) : ISyncLatestCoordinator<int>
    {
        /// <inheritdoc/>
        public SyncLatestLifecycle<int> Lifecycle { get; } = new(observer, sourceCount);

        /// <summary>Gets the indices passed to <see cref="SubscribeAtAsync"/> in order.</summary>
        public List<int> SubscribedIndices { get; } = [];

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => Lifecycle.DisposeAsync();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask EmitLatestAsync() => default;

        /// <inheritdoc/>
        public ValueTask<IAsyncDisposable> SubscribeAtAsync(int index, CancellationToken cancellationToken)
        {
            _ = cancellationToken;
            SubscribedIndices.Add(index);
            return new(NoopDisposable.Instance);
        }
    }

    /// <summary>No-op async disposable handed back from the test coordinator.</summary>
    private sealed class NoopDisposable : IAsyncDisposable
    {
        /// <summary>Gets the shared no-op singleton.</summary>
        public static IAsyncDisposable Instance { get; } = new NoopDisposable();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => default;
    }

    /// <summary>Capture observer for the helper tests.</summary>
    /// <typeparam name="T">Element type captured.</typeparam>
    private sealed class CaptureObserverAsync<T> : IObserverAsync<T>
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask OnNextAsync(T value, CancellationToken cancellationToken) => default;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken) => default;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask OnCompletedAsync(Result result) => default;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => default;
    }
}
