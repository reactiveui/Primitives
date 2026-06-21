// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Disposables;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Coordinates retry subscriptions for <see cref="ReattemptSignal{T}"/>.</summary>
/// <typeparam name="T">The element type.</typeparam>
public sealed class ReattemptSubscription<T> : IAsyncDisposable
{
    /// <summary>Initializes a new instance of the <see cref="ReattemptSubscription{T}"/> class.</summary>
    /// <param name="source">The source sequence.</param>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="retryCount">The number of retry attempts.</param>
    /// <param name="cancellationToken">The subscribe-time cancellation token.</param>
    public ReattemptSubscription(
        IObservableAsync<T> source,
        IObserverAsync<T> observer,
        int retryCount,
        CancellationToken cancellationToken)
    {
        Source = source;
        Observer = observer;
        Remaining = retryCount;
        CancellationToken = cancellationToken;
    }

    /// <summary>Gets the source sequence.</summary>
    private IObservableAsync<T> Source { get; }

    /// <summary>Gets the downstream observer.</summary>
    private IObserverAsync<T> Observer { get; }

    /// <summary>Gets the current source subscription slot.</summary>
    private SingleReplaceableDisposableAsync CurrentSubscription { get; } = new();

    /// <summary>Gets the subscribe-time cancellation token.</summary>
    private CancellationToken CancellationToken { get; }

    /// <summary>Gets or sets the remaining retry attempts.</summary>
    private int Remaining { get; set; }

    /// <inheritdoc/>
    public ValueTask DisposeAsync() => CurrentSubscription.DisposeAsync();

    /// <summary>Subscribes to the source once.</summary>
    /// <returns>A task representing the asynchronous subscription.</returns>
    public async ValueTask SubscribeOnceAsync()
    {
        try
        {
            ReattemptWitness<T> reattemptObserver = new(this);
            await CurrentSubscription.SetDisposableAsync(reattemptObserver).ConfigureAwait(false);
            var subscription = await Source.SubscribeAsync(reattemptObserver, CancellationToken).ConfigureAwait(false);
            await reattemptObserver.AssignSourceSubscriptionAsync(subscription).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Cooperative subscription cancellation.
        }
        catch (Exception e)
        {
            await Observer.OnCompletedAsync(Result.Failure(e)).ConfigureAwait(false);
        }
    }

    /// <summary>Relays a source value to the downstream observer.</summary>
    /// <param name="value">The value to relay.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous notification.</returns>
    public ValueTask RelayNextAsync(T value, CancellationToken cancellationToken) =>
        Observer.OnNextAsync(value, cancellationToken);

    /// <summary>Relays a non-terminal source error to the downstream observer.</summary>
    /// <param name="error">The error to relay.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous notification.</returns>
    public ValueTask RelayErrorAsync(Exception error, CancellationToken cancellationToken) =>
        Observer.OnErrorResumeAsync(error, cancellationToken);

    /// <summary>Handles source completion, retrying terminal failures while attempts remain.</summary>
    /// <param name="result">The source completion result.</param>
    /// <returns>A task representing the asynchronous completion handling.</returns>
    public async ValueTask CompleteAttemptAsync(Result result)
    {
        if (result.IsSuccess || Remaining <= 0)
        {
            await Observer.OnCompletedAsync(result).ConfigureAwait(false);
            return;
        }

        Remaining--;
        await SubscribeOnceAsync().ConfigureAwait(false);
    }
}
