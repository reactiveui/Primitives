// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using ReactiveUI.Primitives.Async.Disposables;
using ReactiveUI.Primitives.Internal;

namespace ReactiveUI.Primitives.Async;

/// <summary>Represents an asynchronous observer that processes notifications of type <typeparamref name="T"/> using asynchronous methods.</summary>
/// <remarks>Implement this abstract class to handle asynchronous event streams or push-based data sources, where
/// notifications may arrive concurrently or in rapid succession. The observer provides asynchronous methods for
/// handling new data, errors, and completion signals, and supports proper resource cleanup via asynchronous disposal.
/// Instances are not thread-safe for concurrent notification handling; notifications are processed sequentially, and
/// reentrant calls are detected and reported as unhandled exceptions.</remarks>
/// <typeparam name="T">The type of the elements received by the observer.</typeparam>
public abstract class ObserverAsync<T> : IObserverAsync<T>
{
    /// <summary>Lazily-created CTS that signals disposal to in-flight operations. Stays
    /// <see langword="null"/> until someone requests <see cref="InternalDisposedToken"/>,
    /// <see cref="LinkExternalCancellation"/> wires a real token, or <see cref="DisposeAsync"/>
    /// is called. Terminal observers (most user-facing sinks) never trigger creation and save
    /// the ~72 B per instance the CTS would cost.</summary>
    private CancellationTokenSource? _disposeCts;

    /// <summary>Disposal latch. Set independently of <see cref="_disposeCts"/> so the lazy-CTS
    /// path can detect post-dispose state before the CTS has been materialized.</summary>
    private int _disposed;

    /// <summary>Packed call-state: high 32 bits hold the managed-thread ID of the thread
    /// currently inside <c>OnNext/OnError/OnCompleted</c>; low 32 bits hold the in-flight call
    /// depth. Updated lock-free via <see cref="Interlocked.CompareExchange(ref long, long, long)"/>.
    /// Replaces a monitor gate + two separate <c>int</c> fields, saving ~24 B per observer.</summary>
    private long _callState;

    /// <summary>Completion source that is set when all in-flight calls finish after disposal has been requested.</summary>
    private TaskCompletionSource<object?>? _allCallsCompletedTcs;

    /// <summary>The disposable representing the upstream source subscription, disposed when this observer is disposed.</summary>
    private IAsyncDisposable? _sourceSubscription;

    /// <summary>Registration created by <see cref="LinkExternalCancellation(CancellationToken)"/> so the link can be released when the observer disposes.</summary>
    private CancellationTokenRegistration _externalLinkRegistration;

    /// <summary>
    /// The external token last passed to <see cref="LinkExternalCancellation(CancellationToken)"/>. Cached so
    /// <see cref="TryEnterOnSomethingCall(CancellationToken, out LinkedTokenScope)"/> can treat it as a
    /// fast-path-equal token: its cancellation already propagates to <see cref="_disposeCts"/>, so combining
    /// it again per emission would allocate a redundant linked CTS.
    /// </summary>
    private CancellationToken _externalLinkedToken;

    /// <summary>Initializes a new instance of the <see cref="ObserverAsync{T}"/> class.</summary>
    protected ObserverAsync()
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="ObserverAsync{T}"/> class and links an external cancellation
    /// token into its dispose chain. Equivalent to calling the parameterless constructor followed by
    /// <see cref="LinkExternalCancellation(CancellationToken)"/>.
    /// </summary>
    /// <param name="externalLink">The external token whose cancellation should trigger this observer's disposal.</param>
    protected ObserverAsync(CancellationToken externalLink) => LinkExternalCancellation(externalLink);

    /// <summary>Gets a value indicating whether this observer has been disposed.</summary>
    internal bool HasDisposed => Volatile.Read(ref _disposed) != 0;

    /// <summary>
    /// Gets the cancellation token that fires when this observer disposes. Exposed for sibling operators
    /// in this assembly so they can wire it into a downstream observer's <see cref="LinkExternalCancellation(CancellationToken)"/>
    /// chain — that lets the downstream's hot-path equality check recognise our token as already-linked and
    /// skip the per-emission linked CTS allocation. Lazily materializes the backing
    /// <see cref="CancellationTokenSource"/> on first access.
    /// </summary>
    internal CancellationToken InternalDisposedToken => GetOrCreateDisposeCts().Token;

    /// <summary>Asynchronously processes the next value in the sequence.</summary>
    /// <param name="value">The value to be processed.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the operation.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
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
            ExitOnSomethingCall();
            return default;
        }
        catch (Exception e)
        {
            return OnNextAsyncSlowAfterSyncThrow(e, scope);
        }

        if (core.IsCompletedSuccessfully)
        {
            scope.Dispose();
            ExitOnSomethingCall();
            return default;
        }

        return OnNextAsyncSlow(core, scope);
    }

    /// <summary>Handles an error by attempting to resume processing asynchronously.</summary>
    /// <param name="error">The exception that triggered the error handling logic. Cannot be null.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
    /// <returns>A task that represents the asynchronous error handling operation.</returns>
    public ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken)
    {
        if (!TryEnterOnSomethingCall(cancellationToken, out var scope))
        {
            return default;
        }

        // RouteObserverErrorAsync is an async ValueTask method — any sync or async exception
        // it raises is captured into the returned ValueTask and surfaces through the await in
        // OnErrorResumeAsyncSlow. A try/catch around the invocation expression itself would be
        // dead code in modern C# async semantics.
        var core = RouteObserverErrorAsync(error, scope.Token);

        if (core.IsCompletedSuccessfully)
        {
            scope.Dispose();
            ExitOnSomethingCall();
            return default;
        }

        return OnErrorResumeAsyncSlow(core, scope);
    }

    /// <summary>
    /// Asynchronously performs completion logic when the operation has finished, handling any finalization or cleanup
    /// tasks required.
    /// </summary>
    /// <remarks>If an unhandled exception occurs during completion, it is passed to the unhandled exception
    /// handler. This method ensures that necessary resources are released after completion.</remarks>
    /// <param name="result">The result of the completed operation, containing information about its outcome.</param>
    /// <returns>A task that represents the asynchronous completion operation.</returns>
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
    /// <remarks>Call this method to clean up resources when the object is no longer needed. This method is
    /// safe to call multiple times; subsequent calls after disposal will have no effect. Any unhandled exceptions that
    /// occur during disposal are captured and reported but do not prevent the completion of the dispose
    /// operation.</remarks>
    /// <returns>A task that represents the asynchronous dispose operation.</returns>
    [DebuggerStepThrough]
    public async ValueTask DisposeAsync()
    {
        await DisposeAsyncCore().ConfigureAwait(false);

        GC.SuppressFinalize(this);
    }

    /// <summary>Sets the source subscription disposable for this observer.</summary>
    /// <param name="value">The source subscription to track, or <see langword="null"/> to clear it.</param>
    /// <returns>A <see cref="ValueTask"/> representing the asynchronous operation.</returns>
    internal ValueTask AssignSourceSubscriptionAsync(IAsyncDisposable? value) =>
        SingleAssignmentDisposableAsync.AssignDisposableAsync(ref _sourceSubscription, value);

    /// <summary>
    /// Internal wrapper around <see cref="LinkExternalCancellation(CancellationToken)"/> so sibling operators
    /// (in their <c>SubscribeAsyncCore</c>) can wire an upstream observer's dispose token into this observer's
    /// link chain. Combined with the cached <see cref="_externalLinkedToken"/> fast-path inside
    /// <see cref="TryEnterOnSomethingCall(CancellationToken, out LinkedTokenScope)"/>, this turns chained
    /// operator pipelines into per-emission allocation-free flows.
    /// </summary>
    /// <param name="upstream">The upstream observer's dispose token.</param>
    internal void LinkUpstreamCancellation(CancellationToken upstream) =>
        LinkExternalCancellation(upstream);

    /// <summary>Attempts to enter a notification call, checking for disposal, cancellation, and concurrent access.</summary>
    /// <param name="cancellationToken">The caller-supplied cancellation token.</param>
    /// <param name="scope">When successful, a <see cref="LinkedTokenScope"/> providing the effective cancellation token.</param>
    /// <returns><see langword="true"/> if the call was entered successfully; otherwise, <see langword="false"/>.</returns>
    [DebuggerStepThrough]
    internal bool TryEnterOnSomethingCall(CancellationToken cancellationToken, out LinkedTokenScope scope)
    {
        var currentThreadId = Environment.CurrentManagedThreadId;
        while (true)
        {
            if (Volatile.Read(ref _disposed) != 0 || cancellationToken.IsCancellationRequested)
            {
                scope = default;
                return false;
            }

            var oldState = Volatile.Read(ref _callState);
            var oldCount = (int)oldState;
            var oldThreadId = (int)(oldState >> 32);

            // Concurrent-call detection: if another thread is already in-flight, this is a contract
            // violation. Reentrant calls from the same thread (a callback that re-enters the observer)
            // are legal — only cross-thread overlap fires the exception.
            if (oldCount > 0 && oldThreadId != currentThreadId)
            {
                UnhandledExceptionHandler.ReportUnhandledException(new ConcurrentObserverCallsException());
                scope = default;
                return false;
            }

            var newThreadId = oldCount == 0 ? currentThreadId : oldThreadId;
            var newState = ((long)newThreadId << 32) | (uint)(oldCount + 1);

            if (Interlocked.CompareExchange(ref _callState, newState, oldState) != oldState)
            {
                continue;
            }

            scope = BuildLinkedTokenScope(cancellationToken);
            return true;
        }
    }

    /// <summary>Exits a notification call, decrementing counters and signalling completion if disposal is pending.</summary>
    /// <returns><see langword="true"/> if the caller should proceed with disposal; <see langword="false"/> if
    /// disposal was already signalled to a waiting <see cref="DisposeAsync"/> call.</returns>
    [DebuggerStepThrough]
    internal bool ExitOnSomethingCall()
    {
        while (true)
        {
            var oldState = Volatile.Read(ref _callState);
            var oldCount = (int)oldState;
            var oldThreadId = (int)(oldState >> 32);

            Debug.Assert(oldCount > 0, "Calls count should be positive when exiting.");

            var newCount = oldCount - 1;
            var newThreadId = newCount == 0 ? 0 : oldThreadId;
            var newState = ((long)newThreadId << 32) | (uint)newCount;

            if (Interlocked.CompareExchange(ref _callState, newState, oldState) != oldState)
            {
                continue;
            }

            if (newCount == 0)
            {
                var tcs = Volatile.Read(ref _allCallsCompletedTcs);
                if (tcs is not null)
                {
                    tcs.TrySetResult(null);
                    return false;
                }
            }

            return true;
        }
    }

    /// <summary>Internal error-resume handler that delegates to <see cref="OnErrorResumeAsyncCore"/> and routes unhandled or cancelled errors to the <see cref="UnhandledExceptionHandler"/>.</summary>
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

    /// <summary>Performs asynchronous completion logic when the operation has finished processing the specified result.</summary>
    /// <param name="result">The result of the operation to be processed during completion.</param>
    /// <returns>A ValueTask that represents the asynchronous completion operation.</returns>
    protected abstract ValueTask OnCompletedAsyncCore(Result result);

    /// <summary>
    /// Links an external cancellation token into this observer's dispose chain. When <paramref name="external"/>
    /// is cancelled, the observer disposes — propagating cancellation through the linked token that subclasses
    /// receive in their <c>OnNextAsyncCore</c> / <c>OnErrorResumeAsyncCore</c> arguments. This eliminates the
    /// need to allocate a per-emission linked <see cref="CancellationTokenSource"/>. Each observer supports at
    /// most one link; calling this method again replaces the previous registration.
    /// </summary>
    /// <param name="external">The external token whose cancellation should trigger this observer's disposal.</param>
    [DebuggerStepThrough]
    protected void LinkExternalCancellation(CancellationToken external)
    {
        // No-op fast path: token can't fire, so the dispose chain doesn't need it. Skip CTS
        // materialization entirely — terminal observers that get CancellationToken.None
        // pay nothing.
        if (!external.CanBeCanceled)
        {
            return;
        }

        // External token already cancelled — materialize the CTS only to mark it cancelled
        // (so future InternalDisposedToken consumers see the cancelled state).
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
                var self = (ObserverAsync<T>)state!;
                Volatile.Write(ref self._disposed, 1);
                Volatile.Read(ref self._disposeCts)?.Cancel();
            },
            this);
        _externalLinkedToken = external;
    }

    /// <summary>Performs application-defined tasks associated with asynchronously releasing unmanaged resources.</summary>
    /// <remarks>Override this method to provide custom asynchronous resource cleanup logic in a derived
    /// class. This method is called by DisposeAsync to perform the actual resource release.</remarks>
    /// <returns>A task that represents the asynchronous dispose operation.</returns>
    [DebuggerStepThrough]
    protected virtual async ValueTask DisposeAsyncCore()
    {
        // First-disposer wins the race. Lazy-CTS observers set the _disposed flag without
        // necessarily materializing a CTS; only callers that previously requested the token
        // need the cancellation broadcast.
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        Task? allOnSomethingCallsCompleted = null;
        var initialState = Volatile.Read(ref _callState);
        var initialCount = (int)initialState;
        var initialThreadId = (int)(initialState >> 32);

        if (initialCount > 0 && initialThreadId != Environment.CurrentManagedThreadId)
        {
            var tcs = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
            Volatile.Write(ref _allCallsCompletedTcs, tcs);

            // Re-read after publishing the TCS — Exit may have raced past us and decremented
            // the count to zero before our publish became visible. Self-signal so the awaiter
            // doesn't deadlock.
            var stateAfter = Volatile.Read(ref _callState);
            if ((int)stateAfter == 0)
            {
                tcs.TrySetResult(null);
            }

            allOnSomethingCallsCompleted = tcs.Task;
        }

        // Materialized CTS holders need cancellation propagated; lazy holders skip this step
        // (any future InternalDisposedToken request will create a pre-cancelled CTS via
        // GetOrCreateDisposeCts's post-disposed branch).
        var cts = Volatile.Read(ref _disposeCts);
        if (cts is not null)
        {
            await ConcurrencyRaceHelpers.TryCancelAsync(cts).ConfigureAwait(false);
        }

        await CompleteDisposeAfterCancelAsync(allOnSomethingCallsCompleted).ConfigureAwait(false);
    }

    /// <summary>Handles an error by providing an asynchronous mechanism to resume execution after an exception occurs.</summary>
    /// <remarks>Override this method to implement custom error recovery or resumption logic in derived
    /// classes. The method is called when an error occurs and allows the operation to continue or perform cleanup
    /// asynchronously.</remarks>
    /// <param name="error">The exception that triggered the error handling logic. Cannot be null.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous error handling operation.</param>
    /// <returns>A ValueTask that represents the asynchronous operation of resuming execution after the error.</returns>
    protected abstract ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken);

    /// <summary>Processes the next value in the asynchronous sequence.</summary>
    /// <param name="value">The value to be processed.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
    /// <returns>A ValueTask that represents the asynchronous operation.</returns>
    protected abstract ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken);

    /// <summary>
    /// Finishes the teardown after this caller won the cancellation race. Separated from
    /// <see cref="DisposeAsyncCore"/> so the race-loser branch is just the absence of this
    /// call, with no <c>return;</c> sequence point to mark uncovered.
    /// </summary>
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

    /// <summary>Returns the dispose task on the race-winner path (<see cref="ExitOnSomethingCall"/>
    /// reports the last in-flight On* call just exited), or a completed default <see cref="ValueTask"/>
    /// otherwise. Isolated from coverage because the race-winner branch is only reachable when a
    /// concurrent <see cref="DisposeAsync"/> set the in-flight gate while this On* call was running.</summary>
    /// <returns>The dispose task on race-winner, or default otherwise.</returns>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    private ValueTask CompleteOrChainDispose() =>
        ExitOnSomethingCall() ? DisposeAsync() : default;

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
            ExitOnSomethingCall();
        }
    }

    /// <summary>
    /// Async continuation for <see cref="OnNextAsync"/> when <see cref="OnNextAsyncCore"/> threw synchronously.
    /// Routes the error through <see cref="RouteObserverErrorAsync"/> off the fast path so the
    /// caller-visible <see cref="OnNextAsync"/> stays state-machine free in the common case.
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
            ExitOnSomethingCall();
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
            ExitOnSomethingCall();
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

    /// <summary>Builds the <see cref="LinkedTokenScope"/> for the current call, allocating a
    /// linked CTS only when the caller token isn't already one of the fast-path equivalents
    /// (<see cref="CancellationToken.None"/>, our own dispose token if materialized, or the
    /// upstream token already linked via <see cref="LinkExternalCancellation"/>).</summary>
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

        var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, GetOrCreateDisposeCts().Token);
        return new(linkedCts, linkedCts.Token);
    }

    /// <summary>Returns the existing <see cref="_disposeCts"/> or lazily creates it. If <see cref="_disposed"/>
    /// was set before this call (the observer was disposed before any token was requested), the
    /// freshly-created CTS is cancelled immediately so its <see cref="CancellationToken.IsCancellationRequested"/>
    /// matches the post-dispose state callers expect.</summary>
    /// <returns>The dispose CTS, freshly cancelled if disposal was already signaled.</returns>
    private CancellationTokenSource GetOrCreateDisposeCts() =>
        Volatile.Read(ref _disposeCts) ?? MaterializeDisposeCts();

    /// <summary>Creates and publishes the dispose CTS on first request, discarding the freshly-created instance
    /// if another thread published one first, and pre-cancelling it when disposal was already signaled.</summary>
    /// <returns>The published dispose CTS.</returns>
    /// <remarks>The compare-exchange-lost branch is only reachable when two threads materialize the CTS
    /// concurrently; isolated here and excluded from coverage as race-only.</remarks>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    private CancellationTokenSource MaterializeDisposeCts()
    {
        var fresh = new CancellationTokenSource();
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

    /// <summary>
    /// A lightweight scope that wraps an optional <see cref="CancellationTokenSource"/> and exposes the
    /// effective <see cref="CancellationToken"/>. When no linked source is needed (e.g. the caller token
    /// is <see cref="CancellationToken.None"/>), the scope avoids allocating a linked CTS entirely.
    /// </summary>
    /// <param name="Cts">The linked CTS to dispose, or <see langword="null"/> if no allocation was needed.</param>
    /// <param name="Token">The effective cancellation token for the notification call.</param>
    internal readonly record struct LinkedTokenScope(CancellationTokenSource? Cts, CancellationToken Token)
        : IDisposable
    {
        /// <inheritdoc/>
        public void Dispose() => Cts?.Dispose();
    }
}
