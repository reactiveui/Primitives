// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Async.Disposables;
using ReactiveUI.Primitives.Internal;

namespace ReactiveUI.Primitives.Async;

/// <summary>Base class for an asynchronous observer: handles the notification gate, cancellation linking and disposal
/// so a derived sink only has to implement the three <c>Core</c> methods.</summary>
/// <typeparam name="T">The type of the elements received by the observer.</typeparam>
/// <remarks>One notification runs at a time. A reentrant call from the thread that holds the gate is allowed, but an
/// overlapping call from another thread is a contract violation: the notification is dropped and a
/// <see cref="ConcurrentWitnessCallsException"/> goes to <see cref="UnhandledExceptionHandler"/>.</remarks>
[System.Diagnostics.DebuggerDisplay("WitnessAsync: Disposed = {_disposed}, CallState = {_callState}")]
public abstract class WitnessAsync<T> : IObserverAsync<T>, IReentrantAsyncDisposable
{
    /// <summary>Signals disposal to in-flight operations; created on the first request for a dispose token, so a
    /// terminal sink that never needs one does not pay for it.</summary>
    private CancellationTokenSource? _disposeCts;

    /// <summary>Disposal latch, set independently of <see cref="_disposeCts"/> so the lazy path can report
    /// post-dispose state with no source in existence.</summary>
    private int _disposed;

    /// <summary>Set when disposal is requested from inside this observer's own notification, telling
    /// <see cref="DisposeAsyncCore"/> to skip the in-flight-call wait that would self-join on the caller once its
    /// continuation hops threads.</summary>
    private int _disposeFromNotification;

    /// <summary>Packed call state: the high 32 bits hold the managed thread ID inside
    /// <c>OnNext/OnError/OnCompleted</c>, the low 32 bits the in-flight call depth, updated lock-free through
    /// <see cref="Interlocked.CompareExchange(ref long, long, long)"/>.</summary>
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

    /// <summary>
    /// Initializes a new instance of the <see cref="WitnessAsync{T}"/> class and links an external cancellation
    /// token into its dispose chain. Equivalent to calling the parameterless constructor followed by
    /// <see cref="LinkExternalCancellation(CancellationToken)"/>.
    /// </summary>
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
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the operation.</param>
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

    /// <summary>Routes a non-terminal error to <see cref="OnErrorResumeAsyncCore"/>, never propagating a failure back
    /// to the producer.</summary>
    /// <param name="error">The exception that triggered the error handling logic. Cannot be null.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
    /// <returns>A task that completes once the error has been handled.</returns>
    public ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken)
    {
        if (!TryEnterOnSomethingCall(cancellationToken, out var scope))
        {
            return default;
        }

        // RouteObserverErrorAsync is an async method, so a synchronous throw is captured into the returned
        // ValueTask too and surfaces at the await in OnErrorResumeAsyncSlow; a try/catch here is dead code.
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

    /// <summary>
    /// Makes an upstream observer's disposal dispose this observer too, so an operator chain tears down from the top
    /// and its emissions need no linked cancellation source.
    /// </summary>
    /// <param name="upstream">The upstream observer's dispose token.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void LinkUpstreamCancellation(CancellationToken upstream) =>
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
                UnhandledExceptionHandler.ReportUnhandledException(new ConcurrentWitnessCallsException());
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

    /// <summary>Leaves a notification call, releasing a <see cref="DisposeAsync"/> that is waiting on the last one.</summary>
    /// <returns><see langword="true"/> when the caller should proceed with disposal itself; <see langword="false"/>
    /// when a waiting <see cref="DisposeAsync"/> call was signalled instead.</returns>
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
            var newState = ((long)(newCount == 0 ? 0 : oldThreadId) << 32) | (uint)newCount;

            if (Interlocked.CompareExchange(ref _callState, newState, oldState) != oldState)
            {
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

    /// <summary>Runs <see cref="OnErrorResumeAsyncCore"/>, sending anything it throws — and the original error when the
    /// call is cancelled — to <see cref="UnhandledExceptionHandler"/>.</summary>
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

    /// <summary>
    /// Links an external cancellation token into this observer's dispose chain: cancelling <paramref name="external"/>
    /// disposes the observer and cancels the token handed to <c>OnNextAsyncCore</c> and
    /// <c>OnErrorResumeAsyncCore</c>. An observer holds at most one link, and a second call replaces it.
    /// </summary>
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

    /// <summary>Performs the observer's asynchronous teardown.</summary>
    /// <returns>A task that represents the asynchronous dispose operation.</returns>
    /// <remarks>An override must call the base implementation: it claims disposal for the first caller, waits out a
    /// notification in flight on another thread, and releases the cancellation link and source subscription.</remarks>
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
            TaskCompletionSource<object?> tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

            // Interlocked rather than a release store, so the re-read below cannot be reordered ahead of this
            // publish. ExitOnSomethingCall decrements under a full-fence CAS and only then reads this field, so
            // with the publish fenced, an Exit that reads a null source has made its decrement visible here and
            // one of the two sides always signals. Unfenced, both can miss and the await below never completes.
            _ = Interlocked.Exchange(ref _allCallsCompletedTcs, tcs);

            // Exit may have dropped the count to zero before the publish became visible, so self-signal.
            var stateAfter = Volatile.Read(ref _callState);
            if ((int)stateAfter == 0)
            {
                _ = tcs.TrySetResult(null);
            }

            allOnSomethingCallsCompleted = tcs.Task;
        }

        // Only an existing source needs the cancellation broadcast; a later token request builds one pre-cancelled.
        var cts = Volatile.Read(ref _disposeCts);
        if (cts is not null)
        {
            await ConcurrencyRaceHelpers.TryCancelAsync(cts).ConfigureAwait(false);
        }

        await CompleteDisposeAfterCancelAsync(allOnSomethingCallsCompleted).ConfigureAwait(false);
    }

    /// <summary>Handles a non-terminal error; the sequence may deliver more values afterwards.</summary>
    /// <param name="error">The exception that triggered the error handling logic. Cannot be null.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous error handling operation.</param>
    /// <returns>A task that completes when the error has been handled.</returns>
    /// <remarks>An exception thrown from an override is reported to <see cref="UnhandledExceptionHandler"/> instead of
    /// reaching the producer.</remarks>
    protected abstract ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken);

    /// <summary>Processes the next value in the asynchronous sequence.</summary>
    /// <param name="value">The value to be processed.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
    /// <returns>A task that completes when the value has been handled.</returns>
    /// <remarks>An exception thrown from an override is fed to <see cref="OnErrorResumeAsyncCore"/>, so the sequence
    /// survives a failed value; a cancellation is swallowed.</remarks>
    protected abstract ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken);

    /// <summary>Awaits any in-flight notifications, then releases the cancellation link, the dispose source and the
    /// source subscription.</summary>
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

    /// <summary>Chains into <see cref="DisposeAsync"/> when this exiting call owns disposal, which happens only when a
    /// concurrent <see cref="DisposeAsync"/> raised the in-flight gate while the notification was running.</summary>
    /// <returns>The dispose task when this caller owns disposal; otherwise a completed task.</returns>
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

    /// <summary>Builds the <see cref="LinkedTokenScope"/> for one call, allocating a linked source only when the
    /// caller's token is none of <see cref="CancellationToken.None"/>, this observer's own dispose token, or the token
    /// linked through <see cref="LinkExternalCancellation"/> — each of which cancels the call anyway.</summary>
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

    /// <summary>Creates and publishes the dispose source, discarding this instance when another thread wins the
    /// publish, and cancelling it when the observer is disposed before any token was requested.</summary>
    /// <returns>The published dispose source.</returns>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    private CancellationTokenSource MaterializeDisposeCts()
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

    /// <summary>
    /// Pairs one notification call's effective cancellation token with the linked source backing it, if one was
    /// needed, so disposing the scope releases that source.
    /// </summary>
    /// <param name="Cts">The linked CTS to dispose, or <see langword="null"/> if no allocation was needed.</param>
    /// <param name="Token">The effective cancellation token for the notification call.</param>
    internal readonly record struct LinkedTokenScope(CancellationTokenSource? Cts, CancellationToken Token) : IDisposable
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose() => Cts?.Dispose();
    }
}
