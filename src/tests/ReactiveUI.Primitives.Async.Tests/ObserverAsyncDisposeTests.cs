// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Async.Disposables;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests for <see cref="IWitnessAsync{T}"/> disposal behavior.</summary>
public sealed class ObserverAsyncDisposeTests
{
    /// <summary>The value emitted by the asynchronous source.</summary>
    private const int EmittedValue = 7;

    /// <summary>The value pushed after the external link has been cancelled; it must never be delivered.</summary>
    private const int PostCancellationValue = 8;

    /// <summary>Verifies that a resumed notification can dispose its own observer.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WhenDisposedReentrantlyAfterNotificationResumes_ThenCompletes()
    {
        TaskCompletionSource release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        SelfDisposingObserver observer = new(release.Task);

        var notification = observer.OnNextAsync(1, CancellationToken.None);
        await Assert.That(notification.IsCompleted).IsFalse();
        release.SetResult();
        await notification;

        await Assert.That(observer.HasDisposed).IsTrue();
    }

    /// <summary>Verifies that FirstAsync resolves and disposes its source after a suspended subscription resumes.</summary>
    /// <returns>A task to monitor completion.</returns>
    [Test]
    public async Task WhenFirstAsyncResolvesAfterSubscriptionResumes_ThenCompletes()
    {
        TaskCompletionSource release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var source = SignalAsync.Create<int>(async (observer, _) =>
        {
            await release.Task;
            await observer.OnNextAsync(EmittedValue, CancellationToken.None).ConfigureAwait(false);
            return DisposableAsync.Empty;
        });

        var first = source.FirstAsync();
        await Assert.That(first.IsCompleted).IsFalse();
        release.SetResult();
        var value = await first;

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

    /// <summary>Verifies that the last active notification signals an already published disposal waiter.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WhenCompletionWaiterPublishedBeforeNotificationExits_ThenExitSignalsWaiter()
    {
        RecordingObserver observer = new(CancellationToken.None);
        var entered = observer.TryEnterOnSomethingCall(CancellationToken.None, out var scope);
        await Assert.That(entered).IsTrue();
        var waiter = observer.PublishCallCompletionWaiter();
        await Assert.That(waiter.IsCompleted).IsFalse();
        scope.Dispose();
        await Assert.That(observer.ExitOnSomethingCall()).IsFalse();
        await waiter;
        await observer.DisposeAsync();
        await Assert.That(observer.HasDisposed).IsTrue();
    }

    /// <summary>A notification that exits before waiter publication requires no further signal.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WhenNotificationExitsBeforeCompletionWaiterPublished_ThenWaiterIsCompleted()
    {
        RecordingObserver observer = new(CancellationToken.None);
        var entered = observer.TryEnterOnSomethingCall(CancellationToken.None, out var scope);
        await Assert.That(entered).IsTrue();
        scope.Dispose();
        await Assert.That(observer.ExitOnSomethingCall()).IsTrue();
        var waiter = observer.PublishCallCompletionWaiter();
        await Assert.That(waiter.IsCompletedSuccessfully).IsTrue();
        await waiter;
        await observer.DisposeAsync();
    }

    /// <summary>Disposal waits for a notification owned by another thread before releasing its source subscription.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WhenDisposedWithForeignNotificationOwner_ThenSourceWaitsForNotificationExit()
    {
        const int ForeignOwnerThread = -1;
        RecordingObserver observer = new(CancellationToken.None);
        StrongBox<bool> sourceDisposed = new();
        await observer.AssignSourceSubscriptionAsync(DisposableAsync.Create(sourceDisposed, static state =>
        {
            state.Value = true;
            return default;
        }));
        var entered = observer.TryEnterOnSomethingCall(ForeignOwnerThread, CancellationToken.None, out var scope);
        await Assert.That(entered).IsTrue();

        var disposal = observer.DisposeAsync();
        await Assert.That(disposal.IsCompleted).IsFalse();
        await Assert.That(sourceDisposed.Value).IsFalse();
        scope.Dispose();
        await Assert.That(observer.ExitOnSomethingCall()).IsFalse();
        await disposal;

        await Assert.That(sourceDisposed.Value).IsTrue();
        await Assert.That(observer.HasDisposed).IsTrue();
    }

    /// <summary>Verifies entry and exit retry against the current notification count after stale observations.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WhenCallStateChangesAfterObservation_ThenEntryAndExitRetry()
    {
        const int CallerThread = 1;
        await using RecordingObserver observer = new(CancellationToken.None);
        var empty = observer.ReadCallState();
        var enteredFirst = observer.TryEnterOnSomethingCall(CallerThread, CancellationToken.None, out var firstScope);
        var beforeSecondEntry = observer.ReadCallState();
        var enteredSecond = observer.TryEnterObservedCallState(CallerThread, empty, CancellationToken.None, out var secondScope);
        var waiter = observer.PublishCallCompletionWaiter();
        firstScope.Dispose();
        secondScope.Dispose();
        await Assert.That(enteredFirst).IsTrue();
        await Assert.That(enteredSecond).IsTrue();
        await Assert.That(observer.ExitObservedCallState(beforeSecondEntry)).IsTrue();
        await Assert.That(waiter.IsCompleted).IsFalse();
        await Assert.That(observer.ExitOnSomethingCall()).IsFalse();
        await waiter;
    }

    /// <summary>Verifies that competing source publications retain one source and preserve prior disposal.</summary>
    /// <param name="disposeFirst">Whether disposal precedes source publication.</param>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task WhenDisposeSourcePublishedTwice_ThenFirstSourceIsRetained(bool disposeFirst)
    {
        RecordingObserver observer = new(CancellationToken.None);
        if (disposeFirst)
        {
            await observer.DisposeAsync();
        }

        var first = observer.MaterializeDisposeCts();
        var second = observer.MaterializeDisposeCts();
        await Assert.That(second).IsSameReferenceAs(first);
        await Assert.That(first.IsCancellationRequested).IsEqualTo(disposeFirst);
        await observer.DisposeAsync();
    }

    /// <summary>Verifies that the last call either signals the disposal waiter or performs disposal itself.</summary>
    /// <param name="publishWaiter">Whether another caller is waiting for disposal.</param>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task WhenLastCallCompletes_ThenDisposalFollowsWaiterOwnership(bool publishWaiter)
    {
        RecordingObserver observer = new(CancellationToken.None);
        var entered = observer.TryEnterOnSomethingCall(CancellationToken.None, out var scope);
        await Assert.That(entered).IsTrue();
        var waiter = publishWaiter ? observer.PublishCallCompletionWaiter() : Task.CompletedTask;
        scope.Dispose();
        await observer.CompleteOrChainDispose();
        await waiter;
        await Assert.That(observer.HasDisposed).IsEqualTo(!publishWaiter);
        await observer.DisposeAsync();
    }

    /// <summary>Observer that records every value it is handed, constructed with an external dispose link.</summary>
    /// <param name="externalLink">The token whose cancellation disposes this observer.</param>
    [DebuggerDisplay("RecordingObserver: {_witness}")]
    private sealed class RecordingObserver(CancellationToken externalLink) : IWitnessAsync<int>
    {
        /// <summary>The notification gate, cancellation link and disposal state.</summary>
        private WitnessAsyncState _witness = new(externalLink);

        /// <summary>Gets the values this observer was handed.</summary>
        internal List<int> Received { get; } = [];

        /// <inheritdoc/>
        ref WitnessAsyncState IWitnessState.Witness => ref _witness;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask OnNextAsync(int value, CancellationToken cancellationToken) =>
            WitnessAsync.OnNextAsync(this, value, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken) =>
            WitnessAsync.OnErrorResumeAsync(this, error, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask OnCompletedAsync(Result result) => WitnessAsync.OnCompletedAsync(this, result);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => WitnessAsync.DisposeStateAsync(this);

        /// <inheritdoc/>
        ValueTask IWitnessAsync<int>.OnNextAsyncCore(int value, CancellationToken cancellationToken)
        {
            Received.Add(value);
            return default;
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ValueTask IWitnessAsync<int>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
            default;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ValueTask IWitnessAsync<int>.OnCompletedAsyncCore(Result result) => default;
    }

    /// <summary>Observer that disposes itself when a suspended notification resumes.</summary>
    /// <param name="release">The notification gate.</param>
    [DebuggerDisplay("SelfDisposingObserver: {_witness}")]
    private sealed class SelfDisposingObserver(Task release) : IWitnessAsync<int>
    {
        /// <summary>The notification gate, cancellation link and disposal state.</summary>
        private WitnessAsyncState _witness;

        /// <inheritdoc/>
        ref WitnessAsyncState IWitnessState.Witness => ref _witness;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask OnNextAsync(int value, CancellationToken cancellationToken) =>
            WitnessAsync.OnNextAsync(this, value, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken) =>
            WitnessAsync.OnErrorResumeAsync(this, error, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask OnCompletedAsync(Result result) => WitnessAsync.OnCompletedAsync(this, result);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => WitnessAsync.DisposeStateAsync(this);

        /// <inheritdoc/>
        async ValueTask IWitnessAsync<int>.OnNextAsyncCore(int value, CancellationToken cancellationToken)
        {
            await release;
            await WitnessAsync.DisposeFromNotificationAsync(this).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ValueTask IWitnessAsync<int>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
            default;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ValueTask IWitnessAsync<int>.OnCompletedAsyncCore(Result result) => default;
    }
}
