// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Async.Disposables;

namespace ReactiveUI.Primitives.Async;

/// <summary>
/// The notification gate, cancellation link and disposal state of an asynchronous witness, embedded as a mutable field by
/// the witness.
/// </summary>
/// <remarks>
/// The witness exposes the field through <see cref="IWitnessState.Witness"/>, and <see cref="WitnessAsync"/> runs the
/// shared behaviour against it. Reentrant calls on the active thread are allowed; overlapping calls from another thread are
/// dropped and reported to the unhandled exception handler as <see cref="ConcurrentWitnessCallsException"/>. Keep the field
/// non-readonly, since a copy is a separate gate.
/// </remarks>
[DebuggerDisplay("WitnessAsyncState: Disposed = {_disposed}, CallState = {_callState}")]
public record struct WitnessAsyncState
{
    /// <summary>The token supplied at construction, linked on first use because the witness is not reachable while it is constructed.</summary>
    private readonly CancellationToken _pendingLink;

    /// <summary>Disposal source, allocated only when a disposal token is requested.</summary>
    private CancellationTokenSource? _disposeCts;

    /// <summary>Completion source that is set when all in-flight calls finish after disposal has been requested.</summary>
    private TaskCompletionSource<object?>? _allCallsCompletedTcs;

    /// <summary>The disposable representing the upstream source subscription, disposed when the witness is disposed.</summary>
    private IAsyncDisposable? _sourceSubscription;

    /// <summary>Registration linking an external token to this witness's disposal, released when the witness disposes.</summary>
    private CancellationTokenRegistration _externalLinkRegistration;

    /// <summary>The token last linked, cached because its cancellation reaches the disposal source anyway, so a call carrying it needs no linked source.</summary>
    private CancellationToken _externalLinkedToken;

    /// <summary>Atomic call state: owner thread ID in the high 32 bits, active call depth in the low 32 bits.</summary>
    private long _callState;

    /// <summary>Disposal latch, set independently of <see cref="_disposeCts"/> so the lazy path can report post-dispose state with no source in existence.</summary>
    private int _disposed;

    /// <summary>Prevents disposal inside a notification from waiting for that same notification.</summary>
    private int _disposeFromNotification;

    /// <summary>1 while <see cref="_pendingLink"/> has not been linked yet.</summary>
    private int _linkPending;

    /// <summary>Initializes a new instance of the <see cref="WitnessAsyncState"/> struct linked to external cancellation.</summary>
    /// <param name="externalLink">The external token whose cancellation disposes the witness.</param>
    public WitnessAsyncState(CancellationToken externalLink)
    {
        _pendingLink = externalLink;
        _linkPending = externalLink.CanBeCanceled ? 1 : 0;
    }

    /// <summary>Gets a value indicating whether the witness has been disposed, including by a construction-time token that has since been cancelled.</summary>
    internal bool HasDisposed =>
        Volatile.Read(ref _disposed) != 0 || (Volatile.Read(ref _linkPending) != 0 && _pendingLink.IsCancellationRequested);

    /// <summary>Gets a value indicating whether disposal was requested from inside the witness's own notification.</summary>
    internal bool IsDisposingFromNotification => Volatile.Read(ref _disposeFromNotification) != 0;

    /// <summary>Gets the disposal source, or <see langword="null"/> when none has been created.</summary>
    internal CancellationTokenSource? DisposeSource => Volatile.Read(ref _disposeCts);

    /// <summary>Links the construction-time token once, now that the witness is reachable.</summary>
    /// <param name="owner">The witness that holds this state.</param>
    internal void EnsureLinked(IWitnessState owner)
    {
        if (Volatile.Read(ref _linkPending) == 0 || Interlocked.Exchange(ref _linkPending, 0) == 0)
        {
            return;
        }

        LinkExternalCancellation(owner, _pendingLink);
    }

    /// <summary>Captures the owner thread and active notification count atomically.</summary>
    /// <returns>The packed notification state.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal long ReadCallState() => Volatile.Read(ref _callState);

    /// <summary>Attempts notification entry against a captured call state, retrying when it has changed.</summary>
    /// <param name="currentThreadId">The calling thread identifier.</param>
    /// <param name="oldState">The captured notification state.</param>
    /// <param name="cancellationToken">Cancellation for the notification.</param>
    /// <param name="scope">The notification's effective cancellation scope.</param>
    /// <returns>Whether entry succeeded.</returns>
    internal bool TryEnterObservedCallState(int currentThreadId, long oldState, CancellationToken cancellationToken, out LinkedTokenScope scope)
    {
        while (true)
        {
            if (Volatile.Read(ref _disposed) != 0 || cancellationToken.IsCancellationRequested)
            {
                scope = default;
                return false;
            }

            var oldCount = (int)oldState;
            var oldThreadId = (int)(oldState >> 32);

            // Same-thread reentrancy is allowed; overlapping calls from different threads violate the contract.
            if (oldCount > 0 && oldThreadId != currentThreadId)
            {
                UnhandledExceptionHandler.ReportUnhandledException(new ConcurrentWitnessCallsException());
                scope = default;
                return false;
            }

            var newThreadId = oldCount == 0 ? currentThreadId : oldThreadId;
            var newState = ((long)newThreadId << 32) | (uint)(oldCount + 1);

            var observedState = Interlocked.CompareExchange(ref _callState, newState, oldState);
            if (observedState != oldState)
            {
                oldState = observedState;
                continue;
            }

            scope = BuildLinkedTokenScope(cancellationToken);
            return true;
        }
    }

    /// <summary>Leaves a notification call, releasing a disposal that is waiting on the last one.</summary>
    /// <returns><see langword="true"/> when the caller should proceed with disposal itself; <see langword="false"/> when a waiting disposal was signalled instead.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal bool ExitOnSomethingCall() => ExitObservedCallState(ReadCallState());

    /// <summary>Retries notification exit when the captured call state has changed.</summary>
    /// <param name="oldState">The captured notification state.</param>
    /// <returns>Whether the exiting caller owns disposal instead of an existing waiter.</returns>
    internal bool ExitObservedCallState(long oldState)
    {
        while (true)
        {
            var oldCount = (int)oldState;
            var oldThreadId = (int)(oldState >> 32);

            Debug.Assert(oldCount > 0, "Calls count should be positive when exiting.");

            var newCount = oldCount - 1;
            var newState = ((long)(newCount == 0 ? 0 : oldThreadId) << 32) | (uint)newCount;

            var observedState = Interlocked.CompareExchange(ref _callState, newState, oldState);
            if (observedState != oldState)
            {
                oldState = observedState;
                continue;
            }

            if (newCount == 0)
            {
                var tcs = Volatile.Read(ref _allCallsCompletedTcs);
                if (tcs is not null)
                {
                    _ = tcs.TrySetResult(null);
                    return false;
                }
            }

            return true;
        }
    }

    /// <summary>Publishes the completion waiter and accounts for a notification that exited before publication.</summary>
    /// <returns>The task completed when the active call count reaches zero.</returns>
    internal Task PublishCallCompletionWaiter()
    {
        TaskCompletionSource<object?> tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        _ = Interlocked.Exchange(ref _allCallsCompletedTcs, tcs);
        if ((int)Volatile.Read(ref _callState) == 0)
        {
            _ = tcs.TrySetResult(null);
        }

        return tcs.Task;
    }

    /// <summary>Publishes a disposal source, releasing a losing allocation and preserving prior disposal.</summary>
    /// <returns>The published dispose source.</returns>
    internal CancellationTokenSource MaterializeDisposeCts()
    {
        var fresh = WitnessAsync.CreateDisposeSource();
        var prior = Interlocked.CompareExchange(ref _disposeCts, fresh, null);
        if (prior is not null)
        {
            fresh.Dispose();
            return prior;
        }

        if (Volatile.Read(ref _disposed) != 0)
        {
            fresh.Cancel();
        }

        return fresh;
    }

    /// <summary>Returns the dispose source, creating it on first request.</summary>
    /// <returns>The dispose source, cancelled when the witness is disposed.</returns>
    internal CancellationTokenSource GetOrCreateDisposeCts() => Volatile.Read(ref _disposeCts) ?? MaterializeDisposeCts();

    /// <summary>Replaces the external cancellation link that disposes the witness and cancels its notifications.</summary>
    /// <param name="owner">The witness that holds this state, passed to the registration callback.</param>
    /// <param name="external">The external token whose cancellation should trigger disposal.</param>
    internal void LinkExternalCancellation(IWitnessState owner, CancellationToken external)
    {
        if (!external.CanBeCanceled)
        {
            return;
        }

        if (external.IsCancellationRequested)
        {
            Volatile.Write(ref _disposed, 1);
            GetOrCreateDisposeCts().Cancel();
            return;
        }

        var cts = GetOrCreateDisposeCts();
        if (external == cts.Token)
        {
            return;
        }

        _externalLinkRegistration.Dispose();
        _externalLinkRegistration = external.UnsafeRegister(
            static state => ((IWitnessState)state!).Witness.CancelFromExternalLink(),
            owner);
        _externalLinkedToken = external;
    }

    /// <summary>Marks the witness disposed and cancels its disposal source when a linked token is cancelled.</summary>
    /// <remarks>The link is registered only after the disposal source exists, and released before the source is disposed.</remarks>
    internal void CancelFromExternalLink()
    {
        Volatile.Write(ref _disposed, 1);
        Volatile.Read(ref _disposeCts)!.Cancel();
    }

    /// <summary>Assigns the upstream source subscription.</summary>
    /// <param name="value">The source subscription to track, or <see langword="null"/> to clear it.</param>
    /// <returns>A task that completes once any disposal this assignment triggered has finished.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal ValueTask AssignSourceSubscriptionAsync(IAsyncDisposable? value) =>
        SingleAssignmentDisposableAsync.AssignDisposableAsync(ref _sourceSubscription, value);

    /// <summary>Records that disposal is running from inside the witness's own notification.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void MarkDisposingFromNotification() => Volatile.Write(ref _disposeFromNotification, 1);

    /// <summary>Claims disposal once.</summary>
    /// <returns><see langword="true"/> for the first caller.</returns>
    internal bool TryClaimDisposal() => Interlocked.Exchange(ref _disposed, 1) == 0;

    /// <summary>Publishes a waiter when a notification owned by another thread is in flight and disposal must wait for it.</summary>
    /// <returns>The waiter, or <see langword="null"/> when disposal need not wait.</returns>
    internal Task? PublishWaiterForForeignCall()
    {
        var initialState = Volatile.Read(ref _callState);
        return (int)initialState > 0
            && (int)(initialState >> 32) != Environment.CurrentManagedThreadId
            && Volatile.Read(ref _disposeFromNotification) == 0
            ? PublishCallCompletionWaiter()
            : null;
    }

    /// <summary>Releases the external cancellation link.</summary>
    /// <returns>A task that completes once the registration is released.</returns>
    internal ValueTask DisposeExternalLinkAsync()
    {
#if NETCOREAPP3_0_OR_GREATER || NETSTANDARD2_1_OR_GREATER
        return _externalLinkRegistration.DisposeAsync();
#else
        _externalLinkRegistration.Dispose();
        return default;
#endif
    }

    /// <summary>Releases the disposal source, when one was created.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void DisposeCancellationSource() => Volatile.Read(ref _disposeCts)?.Dispose();

    /// <summary>Disposes the upstream source subscription once.</summary>
    /// <returns>A task that completes once the subscription is disposed.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal ValueTask DisposeSourceSubscriptionAsync() =>
        SingleAssignmentDisposableAsync.DisposeAsync(ref _sourceSubscription);

    /// <summary>Links a distinct caller token with the witness's disposal token.</summary>
    /// <param name="cancellationToken">The caller-supplied cancellation token.</param>
    /// <returns>A scope whose <c>Token</c> drives the in-flight call's cancellation.</returns>
    private LinkedTokenScope BuildLinkedTokenScope(CancellationToken cancellationToken)
    {
        var existingDisposeToken = Volatile.Read(ref _disposeCts)?.Token ?? default;
        if (cancellationToken == CancellationToken.None
            || (existingDisposeToken.CanBeCanceled && cancellationToken == existingDisposeToken)
            || cancellationToken == _externalLinkedToken)
        {
            return new(null, existingDisposeToken);
        }

        var linkedCts =
            CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, GetOrCreateDisposeCts().Token);
        return new(linkedCts, linkedCts.Token);
    }
}
