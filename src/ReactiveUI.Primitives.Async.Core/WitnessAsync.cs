// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Async.Disposables;
using ReactiveUI.Primitives.Internal;

namespace ReactiveUI.Primitives.Async;

/// <summary>Base class for an asynchronous observer: handles the notification gate, cancellation linking and disposal so a derived sink only has to implement the three <c>Core</c> methods.</summary>
/// <typeparam name="T">The type of the elements received by the observer.</typeparam>
/// <remarks>One notification runs at a time. A reentrant call from the thread that holds the gate is allowed, but an
/// overlapping call from another thread is a contract violation: the notification is dropped and a
/// <see cref="ConcurrentWitnessCallsException"/> goes to <see cref="UnhandledExceptionHandler"/>.</remarks>
[System.Diagnostics.DebuggerDisplay("WitnessAsync: Disposed = {_disposed}, CallState = {_callState}")]
public abstract class WitnessAsync<T> : IObserverAsync<T>, IReentrantAsyncDisposable
{
    /// <summary>Disposal source, allocated only when a disposal token is requested.</summary>
    private CancellationTokenSource? _disposeCts;

    /// <summary>Disposal latch, set independently of <see cref="_disposeCts"/> so the lazy path can report post-dispose state with no source in existence.</summary>
    private int _disposed;

    /// <summary>Prevents disposal inside a notification from waiting for that same notification.</summary>
    private int _disposeFromNotification;

    /// <summary>Atomic call state: owner thread ID in the high 32 bits, active call depth in the low 32 bits.</summary>
    private long _callState;

    /// <summary>Completion source that is set when all in-flight calls finish after disposal has been requested.</summary>
    private TaskCompletionSource<object?>? _allCallsCompletedTcs;

    /// <summary>The disposable representing the upstream source subscription, disposed when this observer is disposed.</summary>
    private IAsyncDisposable? _sourceSubscription;

    /// <summary>Registration created by <see cref="LinkExternalCancellation(CancellationToken)"/> so the link can be released when the observer disposes.</summary>
    private CancellationTokenRegistration _externalLinkRegistration;

    /// <summary>The token last passed to <see cref="LinkExternalCancellation(CancellationToken)"/>, cached because its
    /// cancellation reaches <see cref="_disposeCts"/> anyway, so a call carrying it needs no linked source.</summary>
    private CancellationToken _externalLinkedToken;

    /// <summary>Initializes a new instance of the <see cref="WitnessAsync{T}"/> class.</summary>
    protected WitnessAsync()
    {
    }

    /// <summary>Initializes a new instance of the WitnessAsync class linked to external cancellation.</summary>
    /// <param name="externalLink">The external token whose cancellation should trigger this observer's disposal.</param>
    protected WitnessAsync(CancellationToken externalLink) => LinkExternalCancellation(externalLink);

    /// <summary>
    /// Gets the cancellation token that fires when this observer disposes, for an operator to link into a downstream
    /// observer's dispose chain. Reading it creates the backing <see cref="CancellationTokenSource"/>.
    /// </summary>
    public CancellationToken InternalDisposedToken => GetOrCreateDisposeCts().Token;

    /// <summary>Gets a value indicating whether this observer has been disposed.</summary>
    internal bool HasDisposed => Volatile.Read(ref _disposed) != 0;

    /// <summary>Delivers a value to <see cref="OnNextAsyncCore"/>, dropping it silently when this observer is disposed,
    /// the token is cancelled, or another thread holds the notification gate.</summary>
    /// <param name="value">The value to be processed.</param>
    /// <param name="cancellationToken">A token that cancels the delivery; it is linked with this observer's disposal token.</param>
    /// <returns>A task that completes once the core handler and its bookkeeping have run.</returns>
    public ValueTask OnNextAsync(T value, CancellationToken cancellationToken)
    {
        if (!TryEnterOnSomethingCall(cancellationToken, out var scope))
        {
            return default;
        }

        ValueTask core;
        try
        {
            core = OnNextAsyncCore(value, scope.Token);
        }
        catch (OperationCanceledException)
        {
            scope.Dispose();
            _ = ExitOnSomethingCall();
            return default;
        }
        catch (Exception e)
        {
            return OnNextAsyncSlowAfterSyncThrow(e, scope);
        }

        if (core.IsCompletedSuccessfully)
        {
            scope.Dispose();
            _ = ExitOnSomethingCall();
            return default;
        }

        return OnNextAsyncSlow(core, scope);
    }

    /// <summary>Routes a non-terminal error to <see cref="OnErrorResumeAsyncCore"/>, never propagating a failure back to the producer.</summary>
    /// <param name="error">The exception that triggered the error handling logic. Cannot be null.</param>
    /// <param name="cancellationToken">A token that cancels the handling; it is linked with this observer's disposal token.</param>
    /// <returns>A task that completes once the error has been handled.</returns>
    public ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken)
    {
        if (!TryEnterOnSomethingCall(cancellationToken, out var scope))
        {
            return default;
        }

        // Callback failures are captured in the returned ValueTask.
        var core = RouteObserverErrorAsync(error, scope.Token);

        if (core.IsCompletedSuccessfully)
        {
            scope.Dispose();
            _ = ExitOnSomethingCall();
            return default;
        }

        return OnErrorResumeAsyncSlow(core, scope);
    }

    /// <summary>Delivers the terminal result to <see cref="OnCompletedAsyncCore"/> and then disposes this observer.</summary>
    /// <param name="result">The result of the completed operation, containing information about its outcome.</param>
    /// <returns>A task that completes once the core handler has run and this observer is torn down.</returns>
    /// <remarks>A failure from the core handler goes to <see cref="UnhandledExceptionHandler"/>; disposal runs either
    /// way.</remarks>
    [DebuggerStepThrough]
    public ValueTask OnCompletedAsync(Result result)
    {
        if (!TryEnterOnSomethingCall(CancellationToken.None, out var scope))
        {
            return default;
        }

        ValueTask core;
        try
        {
            core = OnCompletedAsyncCore(result);
        }
        catch (Exception e)
        {
            UnhandledExceptionHandler.ReportUnhandledException(e);
            scope.Dispose();
            return CompleteOrChainDispose();
        }

        if (core.IsCompletedSuccessfully)
        {
            scope.Dispose();
            return CompleteOrChainDispose();
        }

        return OnCompletedAsyncSlow(core, scope);
    }

    /// <summary>Asynchronously releases the resources used by the object.</summary>
    /// <returns>A task that represents the asynchronous dispose operation.</returns>
    /// <remarks>Idempotent. Disposal waits out a notification in flight on another thread, and a failure while
    /// releasing the source subscription is reported to <see cref="UnhandledExceptionHandler"/> rather than
    /// thrown.</remarks>
    [DebuggerStepThrough]
    public async ValueTask DisposeAsync()
    {
        await DisposeAsyncCore().ConfigureAwait(false);

        GC.SuppressFinalize(this);
    }

    /// <summary>Disposes this observer from within its own in-flight notification, skipping the in-flight-call
    /// wait that would otherwise self-deadlock once the notification continuation has hopped threads.</summary>
    /// <returns>A <see cref="ValueTask"/> representing the asynchronous dispose operation.</returns>
    [DebuggerStepThrough]
    ValueTask IReentrantAsyncDisposable.DisposeFromNotificationAsync()
    {
        Volatile.Write(ref _disposeFromNotification, 1);
        return DisposeAsync();
    }

    /// <summary>Sets the source subscription disposable for this observer.</summary>
    /// <param name="value">The source subscription to track, or <see langword="null"/> to clear it.</param>
    /// <returns>A <see cref="ValueTask"/> representing the asynchronous operation.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask AssignSourceSubscriptionAsync(IAsyncDisposable? value) =>
        SingleAssignmentDisposableAsync.AssignDisposableAsync(ref _sourceSubscription, value);

    /// <summary>Makes an upstream observer's disposal dispose this observer too, so an operator chain tears down from the top and its emissions need no linked cancellation source.</summary>
    /// <param name="upstream">The upstream observer's dispose token.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void LinkUpstreamCancellation(CancellationToken upstream) =>
        LinkExternalCancellation(upstream);

    /// <summary>Captures the owner thread and active notification count atomically.</summary>
    /// <returns>The packed notification state.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal long ReadCallState() => Volatile.Read(ref _callState);

    /// <summary>Attempts to enter a notification call, checking for disposal, cancellation, and concurrent access.</summary>
    /// <param name="cancellationToken">The caller-supplied cancellation token.</param>
    /// <param name="scope">When successful, a <see cref="LinkedTokenScope"/> providing the effective cancellation token.</param>
    /// <returns><see langword="true"/> if the call was entered successfully; otherwise, <see langword="false"/>.</returns>
    [DebuggerStepThrough]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal bool TryEnterOnSomethingCall(CancellationToken cancellationToken, out LinkedTokenScope scope) =>
        TryEnterOnSomethingCall(Environment.CurrentManagedThreadId, cancellationToken, out scope);

    /// <summary>Attempts to enter a notification for the supplied caller thread.</summary>
    /// <param name="currentThreadId">The calling thread identifier.</param>
    /// <param name="cancellationToken">Cancellation for the notification.</param>
    /// <param name="scope">The notification's effective cancellation scope.</param>
    /// <returns>Whether the caller acquired the notification gate.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal bool TryEnterOnSomethingCall(int currentThreadId, CancellationToken cancellationToken, out LinkedTokenScope scope) =>
        TryEnterObservedCallState(currentThreadId, ReadCallState(), cancellationToken, out scope);

    /// <summary>Retries notification entry when the captured call state has changed.</summary>
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

    /// <summary>Leaves a notification call, releasing a <see cref="DisposeAsync"/> that is waiting on the last one.</summary>
    /// <returns><see langword="true"/> when the caller should proceed with disposal itself; <see langword="false"/>
    /// when a waiting <see cref="DisposeAsync"/> call was signalled instead.</returns>
    [DebuggerStepThrough]
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
        CancellationTokenSource fresh = new();
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

    /// <summary>Disposes when the exiting call owns disposal, or signals an existing waiter.</summary>
    /// <returns>The disposal operation, or a completed task when a waiter owns disposal.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal ValueTask CompleteOrChainDispose() =>
        ExitOnSomethingCall() ? DisposeAsync() : default;

    /// <summary>Runs <see cref="OnErrorResumeAsyncCore"/>, sending anything it throws — and the original error when the call is cancelled — to <see cref="UnhandledExceptionHandler"/>.</summary>
    /// <param name="error">The exception that triggered error handling.</param>
    /// <param name="cancellationToken">A cancellation token for the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    internal async ValueTask RouteObserverErrorAsync(Exception error, CancellationToken cancellationToken)
    {
        try
        {
            if (cancellationToken.IsCancellationRequested)
            {
                UnhandledExceptionHandler.ReportUnhandledException(error);
                return;
            }

            await OnErrorResumeAsyncCore(error, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            UnhandledExceptionHandler.ReportUnhandledException(error);
        }
        catch (Exception e)
        {
            UnhandledExceptionHandler.ReportUnhandledException(e);
        }
    }

    /// <summary>Handles the sequence's terminal result. Called at most once, before this observer is disposed.</summary>
    /// <param name="result">The result of the operation to be processed during completion.</param>
    /// <returns>A task that completes when the result has been handled.</returns>
    protected abstract ValueTask OnCompletedAsyncCore(Result result);

    /// <summary>Replaces the external cancellation link that disposes this observer and cancels its notifications.</summary>
    /// <param name="external">The external token whose cancellation should trigger this observer's disposal.</param>
    [DebuggerStepThrough]
    protected void LinkExternalCancellation(CancellationToken external)
    {
        // A token that cannot fire adds nothing to the dispose chain, so skip creating the source entirely.
        if (!external.CanBeCanceled)
        {
            return;
        }

        // A cancelled token needs the source created only to hold the cancelled state for later token requests.
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
            static state =>
            {
                var self = (WitnessAsync<T>)state!;
                Volatile.Write(ref self._disposed, 1);
                Volatile.Read(ref self._disposeCts)?.Cancel();
            },
            this);
        _externalLinkedToken = external;
    }

    /// <summary>Claims disposal, waits for active notifications, and releases subscriptions; overrides must call the base implementation.</summary>
    /// <returns>A task that represents the asynchronous dispose operation.</returns>
    [DebuggerStepThrough]
    protected virtual async ValueTask DisposeAsyncCore()
    {
        // First disposer wins; the latch moves whether or not a dispose source exists.
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        Task? allOnSomethingCallsCompleted = null;
        var initialState = Volatile.Read(ref _callState);

        if ((int)initialState > 0
            && (int)(initialState >> 32) != Environment.CurrentManagedThreadId
            && Volatile.Read(ref _disposeFromNotification) == 0)
        {
            allOnSomethingCallsCompleted = PublishCallCompletionWaiter();
        }

        // Only an existing source needs the cancellation broadcast; a later token request builds one pre-cancelled.
        var cts = Volatile.Read(ref _disposeCts);
        if (cts is not null)
        {
            await ConcurrencyRaceHelpers.TryCancelAsync(cts).ConfigureAwait(false);
        }

        await CompleteDisposeAfterCancelAsync(allOnSomethingCallsCompleted).ConfigureAwait(false);
    }

    /// <summary>Handles non-terminal errors; callback failures reach the unhandled-exception handler.</summary>
    /// <param name="error">The exception that triggered the error handling logic. Cannot be null.</param>
    /// <param name="cancellationToken">The effective token for this notification.</param>
    /// <returns>A task that completes when the error has been handled.</returns>
    protected abstract ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken);

    /// <summary>Delivers the value, forwarding callback failures to error-resume handling and swallowing cancellation.</summary>
    /// <param name="value">The value to be processed.</param>
    /// <param name="cancellationToken">The effective token for this notification.</param>
    /// <returns>A task that completes when the value has been handled.</returns>
    protected abstract ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken);

    /// <summary>Awaits any in-flight notifications, then releases the cancellation link, the dispose source and the source subscription.</summary>
    /// <param name="allOnSomethingCallsCompleted">Optional gate awaited for in-flight On* calls.</param>
    /// <returns>A task representing the asynchronous teardown.</returns>
    private async ValueTask CompleteDisposeAfterCancelAsync(Task? allOnSomethingCallsCompleted)
    {
        if (allOnSomethingCallsCompleted is not null)
        {
            await allOnSomethingCallsCompleted.ConfigureAwait(false);
        }

#if NETCOREAPP3_0_OR_GREATER || NETSTANDARD2_1_OR_GREATER
        await _externalLinkRegistration.DisposeAsync().ConfigureAwait(false);
#else
        _externalLinkRegistration.Dispose();
#endif
        Volatile.Read(ref _disposeCts)?.Dispose();

        try
        {
            await SingleAssignmentDisposableAsync.DisposeAsync(ref _sourceSubscription).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            UnhandledExceptionHandler.ReportUnhandledException(e);
        }
    }

    /// <summary>Async continuation for <see cref="OnNextAsync"/> when the core <see cref="ValueTask"/> is incomplete.</summary>
    /// <param name="core">The pending core <see cref="ValueTask"/>.</param>
    /// <param name="scope">The linked-token scope to release on completion.</param>
    /// <returns>A <see cref="ValueTask"/> that completes once the core completes and bookkeeping has run.</returns>
    private async ValueTask OnNextAsyncSlow(ValueTask core, LinkedTokenScope scope)
    {
        try
        {
            await core.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Cooperative cancellation; swallow.
        }
        catch (Exception e)
        {
            await RouteObserverErrorAsync(e, scope.Token).ConfigureAwait(false);
        }
        finally
        {
            scope.Dispose();
            _ = ExitOnSomethingCall();
        }
    }

    /// <summary>
    /// Routes a synchronous throw from <see cref="OnNextAsyncCore"/> through <see cref="RouteObserverErrorAsync"/>,
    /// out of line so <see cref="OnNextAsync"/> itself needs no state machine.
    /// </summary>
    /// <param name="error">The exception thrown by the core.</param>
    /// <param name="scope">The linked-token scope to release on completion.</param>
    /// <returns>A <see cref="ValueTask"/> that completes once error handling and bookkeeping have run.</returns>
    private async ValueTask OnNextAsyncSlowAfterSyncThrow(Exception error, LinkedTokenScope scope)
    {
        try
        {
            await RouteObserverErrorAsync(error, scope.Token).ConfigureAwait(false);
        }
        finally
        {
            scope.Dispose();
            _ = ExitOnSomethingCall();
        }
    }

    /// <summary>Async continuation for <see cref="OnErrorResumeAsync"/> when the core returned an incomplete <see cref="ValueTask"/>.</summary>
    /// <param name="core">The pending core <see cref="ValueTask"/>.</param>
    /// <param name="scope">The linked-token scope to release on completion.</param>
    /// <returns>A <see cref="ValueTask"/> that completes once the core completes and bookkeeping has run.</returns>
    private async ValueTask OnErrorResumeAsyncSlow(ValueTask core, LinkedTokenScope scope)
    {
        try
        {
            await core.ConfigureAwait(false);
        }
        finally
        {
            scope.Dispose();
            _ = ExitOnSomethingCall();
        }
    }

    /// <summary>Async continuation for <see cref="OnCompletedAsync"/> when the core <see cref="ValueTask"/> is incomplete.</summary>
    /// <param name="core">The pending core <see cref="ValueTask"/>.</param>
    /// <param name="scope">The linked-token scope to release on completion.</param>
    /// <returns>A <see cref="ValueTask"/> that completes once the core, bookkeeping, and any required dispose have run.</returns>
    private async ValueTask OnCompletedAsyncSlow(ValueTask core, LinkedTokenScope scope)
    {
        try
        {
            await core.ConfigureAwait(false);
        }
        catch (Exception e)
        {
            UnhandledExceptionHandler.ReportUnhandledException(e);
        }
        finally
        {
            scope.Dispose();
        }

        if (ExitOnSomethingCall())
        {
            await DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>Links a distinct caller token with the observer's disposal token.</summary>
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

    /// <summary>Returns the dispose source, creating it on first request.</summary>
    /// <returns>The dispose source, cancelled when this observer is disposed.</returns>
    private CancellationTokenSource GetOrCreateDisposeCts() =>
        Volatile.Read(ref _disposeCts) ?? MaterializeDisposeCts();

    /// <summary>Pairs one notification call's effective cancellation token with the linked source backing it, if one was needed, so disposing the scope releases that source.</summary>
    /// <param name="Cts">The linked CTS to dispose, or <see langword="null"/> if no allocation was needed.</param>
    /// <param name="Token">The effective cancellation token for the notification call.</param>
    internal readonly record struct LinkedTokenScope(CancellationTokenSource? Cts, CancellationToken Token) : IDisposable
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose() => Cts?.Dispose();
    }
}
