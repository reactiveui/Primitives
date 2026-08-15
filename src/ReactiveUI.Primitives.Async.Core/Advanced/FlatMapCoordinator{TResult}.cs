// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Async.Disposables;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Coordinates outer and inner subscriptions for flat-map operations.</summary>
/// <typeparam name="TResult">The result element type.</typeparam>
[System.Diagnostics.DebuggerDisplay("ActiveInnerCount = {ActiveInnerCount}, OuterCompleted = {OuterCompleted}, Disposed = {Disposed}")]
public sealed class FlatMapCoordinator<TResult> : IAsyncDisposable
{
    /// <summary>Protects mutable lifecycle state.</summary>
    private readonly Lock _gate = new();

    /// <summary>Initializes a new instance of the <see cref="FlatMapCoordinator{TResult}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    public FlatMapCoordinator(IObserverAsync<TResult> observer)
    {
        Observer = observer;
        DisposeToken = Cancellation.Token;
    }

    /// <summary>Gets the downstream observer.</summary>
    private IObserverAsync<TResult> Observer { get; }

    /// <summary>Gets the source observer slot.</summary>
    private SingleAssignmentDisposableAsync OuterObserver { get; } = new();

    /// <summary>Gets the active inner observer collection.</summary>
    private MultipleDisposableAsync InnerObservers { get; } = new();

    /// <summary>Gets the lifecycle cancellation source.</summary>
    private CancellationTokenSource Cancellation { get; } = new();

    /// <summary>Gets the cancellation token used for inner and downstream work.</summary>
    private CancellationToken DisposeToken { get; }

    /// <summary>Gets the observer notification gate.</summary>
    private AsyncSerialGate ObserverGate { get; } = new();

    /// <summary>Gets or sets the external cancellation registration.</summary>
    private CancellationTokenRegistration ExternalLinkRegistration { get; set; }

    /// <summary>Gets or sets a value indicating whether the outer source has completed.</summary>
    private bool OuterCompleted { get; set; }

    /// <summary>Gets or sets the number of active inner subscriptions.</summary>
    private int ActiveInnerCount { get; set; }

    /// <summary>Gets or sets a value indicating whether this coordinator has been disposed.</summary>
    private bool Disposed { get; set; }

    /// <summary>Sets the outer observer.</summary>
    /// <param name="observer">The outer observer.</param>
    /// <returns>A task representing the asynchronous assignment.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask SetOuterObserverAsync(IAsyncDisposable observer) =>
        OuterObserver.SetDisposableAsync(observer);

    /// <summary>Links subscribe-time cancellation into the coordinator lifecycle.</summary>
    /// <param name="external">The subscribe-time cancellation token.</param>
    public void LinkExternalCancellation(CancellationToken external)
    {
        if (!external.CanBeCanceled || external == DisposeToken)
        {
            return;
        }

        if (external.IsCancellationRequested)
        {
            Cancellation.Cancel();
            return;
        }

        ExternalLinkRegistration = external.UnsafeRegister(
            static state => ((CancellationTokenSource)state!).Cancel(),
            Cancellation);
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask DisposeAsync() => FinishAsync(null);

    /// <summary>Subscribes and tracks an inner sequence.</summary>
    /// <param name="inner">The inner sequence.</param>
    /// <returns>A task representing the asynchronous subscription.</returns>
    public async ValueTask SubscribeInnerAsync(IObservableAsync<TResult> inner)
    {
        lock (_gate)
        {
            if (Disposed)
            {
                return;
            }

            ActiveInnerCount++;
        }

        FlatMapWitness<TResult> innerObserver = new(this);
        await InnerObservers.AddAsync(innerObserver).ConfigureAwait(false);
        try
        {
            var subscription = await inner.SubscribeAsync(innerObserver, DisposeToken).ConfigureAwait(false);
            await innerObserver.AssignSourceSubscriptionAsync(subscription).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            await innerObserver.DisposeAsync().ConfigureAwait(false);
            await FinishAsync(Result.Failure(e)).ConfigureAwait(false);
        }
    }

    /// <summary>Forwards an inner value to the downstream observer.</summary>
    /// <param name="value">The value.</param>
    /// <returns>A task representing the asynchronous notification.</returns>
    public async ValueTask RelayNextAsync(TResult value)
    {
        using (await ObserverGate.EnterAsync(DisposeToken).ConfigureAwait(false))
        {
            await Observer.OnNextAsync(value, DisposeToken).ConfigureAwait(false);
        }
    }

    /// <summary>Forwards a non-terminal error to the downstream observer.</summary>
    /// <param name="error">The error.</param>
    /// <returns>A task representing the asynchronous notification.</returns>
    public async ValueTask RelayErrorAsync(Exception error)
    {
        using (await ObserverGate.EnterAsync(DisposeToken).ConfigureAwait(false))
        {
            await Observer.OnErrorResumeAsync(error, DisposeToken).ConfigureAwait(false);
        }
    }

    /// <summary>Handles outer source completion.</summary>
    /// <param name="result">The completion result.</param>
    /// <returns>A task representing the asynchronous completion handling.</returns>
    public ValueTask CompleteOuterAsync(Result result)
    {
        bool shouldComplete;
        lock (_gate)
        {
            OuterCompleted = true;
            shouldComplete = result.IsFailure || ActiveInnerCount == 0;
        }

        return shouldComplete ? FinishAsync(result) : default;
    }

    /// <summary>Handles inner source completion.</summary>
    /// <param name="result">The completion result.</param>
    /// <returns>A task representing the asynchronous completion handling.</returns>
    public ValueTask CompleteInnerAsync(Result result)
    {
        bool shouldComplete;
        lock (_gate)
        {
            if (ActiveInnerCount > 0)
            {
                ActiveInnerCount--;
            }

            shouldComplete = result.IsFailure || (OuterCompleted && ActiveInnerCount == 0);
        }

        return shouldComplete ? FinishAsync(result) : default;
    }

    /// <summary>Finishes the flat-map sequence and disposes tracked subscriptions.</summary>
    /// <param name="result">The optional result to forward.</param>
    /// <returns>A task representing the asynchronous teardown.</returns>
    public async ValueTask FinishAsync(Result? result)
    {
        lock (_gate)
        {
            if (Disposed)
            {
                RoutePostDisposalException(result);
                return;
            }

            Disposed = true;
        }

        await Cancellation.CancelAsync().ConfigureAwait(false);
        await InnerObservers.DisposeAsync().ConfigureAwait(false);
        await OuterObserver.DisposeAsync().ConfigureAwait(false);
        if (result is not null)
        {
            await Observer.OnCompletedAsync(result.Value).ConfigureAwait(false);
        }

#if NETCOREAPP3_0_OR_GREATER || NETSTANDARD2_1_OR_GREATER
        await ExternalLinkRegistration.DisposeAsync().ConfigureAwait(false);
#else
        ExternalLinkRegistration.Dispose();
#endif
        Cancellation.Dispose();
        ObserverGate.Dispose();
    }

    /// <summary>Routes post-disposal failures to the unhandled exception handler.</summary>
    /// <param name="result">The completion result.</param>
    private static void RoutePostDisposalException(Result? result)
    {
        if (result?.Exception is not { } exception)
        {
            return;
        }

        UnhandledExceptionHandler.ReportUnhandledException(exception);
    }
}
