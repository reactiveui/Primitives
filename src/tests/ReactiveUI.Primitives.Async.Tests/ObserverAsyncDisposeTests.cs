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

    /// <summary>Verifies that self-disposing a downstream witness after hopping threads propagates notification disposal to an upstream witness without deadlocking.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WhenChainedWitnessSelfDisposesOnForeignThread_ThenUpstreamDisposesWithoutDeadlock()
    {
        StrongBox<bool> rootDisposed = new();
        ThreadHoppingSelfDisposingObserver downstream = new();
        ForwardingTestWitness upstream = new(downstream);

        downstream.LinkUpstreamCancellation(upstream.InternalDisposedToken);
        await upstream.AssignSourceSubscriptionAsync(DisposableAsync.Create(rootDisposed, static state =>
        {
            state.Value = true;
            return default;
        }));
        await downstream.AssignSourceSubscriptionAsync(upstream);

        await upstream.OnNextAsync(EmittedValue, CancellationToken.None);

        await Assert.That(downstream.HasDisposed).IsTrue();
        await Assert.That(upstream.HasDisposed).IsTrue();
        await Assert.That(rootDisposed.Value).IsTrue();
    }

    /// <summary>Verifies that completing a downstream witness across a thread hop tears down the upstream witness without deadlocking.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WhenChainedWitnessCompletesOnForeignThread_ThenUpstreamDisposesWithoutDeadlock()
    {
        StrongBox<bool> rootDisposed = new();
        ThreadHoppingCompletionObserver downstream = new();
        CompletingOnNextTestWitness upstream = new(downstream);

        downstream.LinkUpstreamCancellation(upstream.InternalDisposedToken);
        await upstream.AssignSourceSubscriptionAsync(DisposableAsync.Create(rootDisposed, static state =>
        {
            state.Value = true;
            return default;
        }));
        await downstream.AssignSourceSubscriptionAsync(upstream);

        await upstream.OnNextAsync(EmittedValue, CancellationToken.None);

        await Assert.That(downstream.HasDisposed).IsTrue();
        await Assert.That(upstream.HasDisposed).IsTrue();
        await Assert.That(rootDisposed.Value).IsTrue();
    }

    /// <summary>Verifies stop coordinators propagate notification disposal to upstream witness subscriptions after thread switches.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WhenStopCoordinatorsTriggerAfterThreadSwitch_ThenChainedUpstreamWitnessesDisposeCleanly()
    {
        var syncSource = Signals.Signal.Create<int>();
        var syncResultTask = syncSource.Values
            .Select(static x => x)
            .TakeUntil(static x => x == EmittedValue)
            .Select(static (x, _) => new ValueTask<int>(RunOnForeignThreadAsync(x)))
            .FirstAsync()
            .AsTask();
        await syncSource.OnNextAsync(EmittedValue, CancellationToken.None);
        var syncValue = await syncResultTask;
        await Assert.That(syncValue).IsEqualTo(EmittedValue);

        var asyncSource = Signals.Signal.Create<int>();
        var asyncResultTask = asyncSource.Values
            .Select(static x => x)
            .TakeUntil(MatchesOnForeignThreadAsync)
            .FirstAsync()
            .AsTask();
        await asyncSource.OnNextAsync(EmittedValue, CancellationToken.None);
        var asyncValue = await asyncResultTask;
        await Assert.That(asyncValue).IsEqualTo(EmittedValue);

        var mainSource = Signals.Signal.Create<int>();
        var stopSource = Signals.Signal.Create<int>();
        List<int> collected = [];
        await using var sub = await mainSource.Values
            .TakeUntil(stopSource.Values.Select(static (x, _) => new ValueTask<int>(RunOnForeignThreadAsync(x))))
            .SubscribeAsync((x, _) =>
            {
                collected.Add(x);
                return default;
            });
        await mainSource.OnNextAsync(EmittedValue, CancellationToken.None);
        await stopSource.OnNextAsync(1, CancellationToken.None);
        await Assert.That(collected).IsCollectionEqualTo([EmittedValue]);

        var failMainSource = Signals.Signal.Create<int>();
        var failStopSource = Signals.Signal.Create<int>();
        TaskCompletionSource completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var failSub = await failMainSource.Values
            .TakeUntil(failStopSource.Values.Select(static x => x))
            .SubscribeAsync(
                static (_, _) => default,
                static (_, _) => default,
                _ => new ValueTask(RunOnForeignThreadAndSignalAsync(completed)));
        await failStopSource.OnCompletedAsync(Result.Failure(new InvalidOperationException("stop-fault")));
        await completed.Task;

        const int DebounceSeconds = 10;
        var debounceSignal = Signals.Signal.Create<int>();
        var debounceFirst = debounceSignal.Values
            .DebounceUntil(TimeSpan.FromSeconds(DebounceSeconds), static v => v == EmittedValue)
            .Take(1)
            .FirstAsync();
        await debounceSignal.OnNextAsync(EmittedValue, CancellationToken.None);
        var debounceValue = await debounceFirst;
        await Assert.That(debounceValue).IsEqualTo(EmittedValue);
    }

    /// <summary>Evaluates stop match on a thread-pool worker.</summary>
    /// <param name="value">The candidate value.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>Whether <paramref name="value"/> matches <see cref="EmittedValue"/>.</returns>
    private static async ValueTask<bool> MatchesOnForeignThreadAsync(int value, CancellationToken cancellationToken)
    {
        _ = cancellationToken;
        var hopped = await RunOnForeignThreadAsync(value).ConfigureAwait(false);
        return hopped == EmittedValue;
    }

    /// <summary>Executes a return continuation on a thread-pool worker distinct from synchronous inline execution.</summary>
    /// <param name="value">The value to return.</param>
    /// <returns>A task yielding <paramref name="value"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Task<int> RunOnForeignThreadAsync(int value) =>
        Task.Run(() => value);

    /// <summary>Completes <paramref name="completion"/> on a thread-pool worker.</summary>
    /// <param name="completion">The completion source to signal.</param>
    /// <returns>A task representing the worker operation.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Task RunOnForeignThreadAndSignalAsync(TaskCompletionSource completion) =>
        Task.Run(() => { _ = completion.TrySetResult(); });

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

    /// <summary>Upstream witness that forwards values directly to a downstream observer.</summary>
    /// <param name="downstream">The downstream observer.</param>
    [DebuggerDisplay("ForwardingTestWitness: {_witness}")]
    private sealed class ForwardingTestWitness(IObserverAsync<int> downstream) : IWitnessAsync<int>
    {
        /// <summary>Embedded witness state.</summary>
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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ValueTask IWitnessAsync<int>.OnNextAsyncCore(int value, CancellationToken cancellationToken) =>
            downstream.OnNextAsync(value, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ValueTask IWitnessAsync<int>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
            default;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ValueTask IWitnessAsync<int>.OnCompletedAsyncCore(Result result) => default;
    }

    /// <summary>Upstream witness that signals downstream completion from inside its value notification.</summary>
    /// <param name="downstream">The downstream observer.</param>
    [DebuggerDisplay("CompletingOnNextTestWitness: {_witness}")]
    private sealed class CompletingOnNextTestWitness(IObserverAsync<int> downstream) : IWitnessAsync<int>
    {
        /// <summary>Embedded witness state.</summary>
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
            await downstream.OnNextAsync(value, cancellationToken).ConfigureAwait(false);
            await downstream.OnCompletedAsync(Result.Success).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ValueTask IWitnessAsync<int>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
            default;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ValueTask IWitnessAsync<int>.OnCompletedAsyncCore(Result result) => default;
    }

    /// <summary>Downstream witness that hops onto a thread-pool worker before disposing itself from notification context.</summary>
    [DebuggerDisplay("ThreadHoppingSelfDisposingObserver: {_witness}")]
    private sealed class ThreadHoppingSelfDisposingObserver : IWitnessAsync<int>
    {
        /// <summary>Embedded witness state.</summary>
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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ValueTask IWitnessAsync<int>.OnNextAsyncCore(int value, CancellationToken cancellationToken) =>
            new(Task.Run(async () => await WitnessAsync.DisposeFromNotificationAsync(this).ConfigureAwait(false), CancellationToken.None));

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ValueTask IWitnessAsync<int>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
            default;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ValueTask IWitnessAsync<int>.OnCompletedAsyncCore(Result result) => default;
    }

    /// <summary>Downstream witness whose completion hook hops onto a thread-pool worker before automatic teardown runs.</summary>
    [DebuggerDisplay("ThreadHoppingCompletionObserver: {_witness}")]
    private sealed class ThreadHoppingCompletionObserver : IWitnessAsync<int>
    {
        /// <summary>Embedded witness state.</summary>
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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ValueTask IWitnessAsync<int>.OnNextAsyncCore(int value, CancellationToken cancellationToken) => default;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ValueTask IWitnessAsync<int>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
            default;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ValueTask IWitnessAsync<int>.OnCompletedAsyncCore(Result result) =>
            new(Task.Run(static () => { }));
    }
}
