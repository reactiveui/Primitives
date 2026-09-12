// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Async.Disposables;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Coordinates outer and inner subscriptions for flat-map operations.</summary>
/// <typeparam name="TResult">The result element type.</typeparam>
[System.Diagnostics.DebuggerDisplay("FlatMapCoordinator: ActiveInnerCount = {ActiveInnerCount}, OuterCompleted = {OuterCompleted}, Disposed = {Disposed}")]
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

    /// <summary>Takes ownership of the outer subscription, disposing it immediately when this coordinator has finished. Callable once per coordinator.</summary>
    /// <param name="observer">The outer subscription to own.</param>
    /// <returns>A task that completes once the subscription has been stored or disposed.</returns>
    /// <exception cref="InvalidOperationException">Thrown when an outer subscription is set twice.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask SetOuterObserverAsync(IAsyncDisposable observer) =>
        OuterObserver.SetDisposableAsync(observer);

    /// <summary>Cancels this coordinator's lifetime when <paramref name="external"/> is cancelled. A token that
    /// cannot be cancelled is ignored; a token cancelled at call time cancels the lifetime immediately.</summary>
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

    /// <summary>Subscribes to an inner sequence and counts it as active until it completes. A failure raised while
    /// subscribing finishes the whole sequence with that failure; a call after teardown is a no-op.</summary>
    /// <param name="inner">The inner sequence.</param>
    /// <returns>A task that completes once the inner sequence has been subscribed.</returns>
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

    /// <summary>Forwards an inner value downstream, serialized against the other inner sequences so the observer is never entered concurrently.</summary>
    /// <param name="value">The value.</param>
    /// <returns>A task that completes once the observer has accepted the value.</returns>
    public async ValueTask RelayNextAsync(TResult value)
    {
        using (await ObserverGate.EnterAsync(DisposeToken).ConfigureAwait(false))
        {
            await Observer.OnNextAsync(value, DisposeToken).ConfigureAwait(false);
        }
    }

    /// <summary>Forwards a non-terminal error downstream under the same serialization as values, leaving the sequence running.</summary>
    /// <param name="error">The error.</param>
    /// <returns>A task that completes once the observer has accepted the error.</returns>
    public async ValueTask RelayErrorAsync(Exception error)
    {
        using (await ObserverGate.EnterAsync(DisposeToken).ConfigureAwait(false))
        {
            await Observer.OnErrorResumeAsync(error, DisposeToken).ConfigureAwait(false);
        }
    }

    /// <summary>Records outer completion, finishing the sequence when the result is a failure or no inner sequence is active, and otherwise waiting for the active inner sequences.</summary>
    /// <param name="result">The completion result.</param>
    /// <returns>A task that completes once the sequence has finished, or immediately when it continues.</returns>
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

    /// <summary>Records one inner sequence finishing, completing the sequence when the result is a failure or when the outer source has completed and no inner sequence remains.</summary>
    /// <param name="result">The completion result.</param>
    /// <returns>A task that completes once the sequence has finished, or immediately when it continues.</returns>
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

    /// <summary>Completes the downstream observer and disposes every tracked subscription. Idempotent; a failure handed in after teardown goes to the unhandled exception handler.</summary>
    /// <param name="result">The result to forward, or <see langword="null"/> to tear down without completing
    /// the observer.</param>
    /// <returns>A task that completes once teardown has finished.</returns>
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
