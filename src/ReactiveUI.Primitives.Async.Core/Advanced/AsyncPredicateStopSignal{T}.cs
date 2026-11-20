// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Emits source items until an async predicate returns true.</summary>
/// <typeparam name="T">The element type.</typeparam>
[System.Diagnostics.DebuggerDisplay("AsyncPredicateStopSignal: AsyncPredicate = {AsyncPredicate}")]
public sealed class AsyncPredicateStopSignal<T> : IObservableAsync<T>
{
    /// <summary>Initializes a new instance of the <see cref="AsyncPredicateStopSignal{T}"/> class.</summary>
    /// <param name="source">The source sequence.</param>
    /// <param name="asyncPredicate">Predicate that signals when to stop.</param>
    public AsyncPredicateStopSignal(
        IObservableAsync<T> source,
        Func<T, CancellationToken, ValueTask<bool>> asyncPredicate)
        : this(source, asyncPredicate, default)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="AsyncPredicateStopSignal{T}"/> class.</summary>
    /// <param name="source">The source sequence.</param>
    /// <param name="asyncPredicate">Predicate that signals when to stop.</param>
    /// <param name="stopToken">A token whose cancellation also completes the sequence.</param>
    public AsyncPredicateStopSignal(
        IObservableAsync<T> source,
        Func<T, CancellationToken, ValueTask<bool>> asyncPredicate,
        CancellationToken stopToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(asyncPredicate);
        Source = source;
        AsyncPredicate = asyncPredicate;
        StopToken = stopToken;
    }

    /// <summary>Gets the async predicate that signals when to stop emitting items.</summary>
    internal Func<T, CancellationToken, ValueTask<bool>> AsyncPredicate { get; }

    /// <summary>Gets the source observable sequence.</summary>
    internal IObservableAsync<T> Source { get; }

    /// <summary>Gets a token whose cancellation also completes the sequence.</summary>
    internal CancellationToken StopToken { get; }

    /// <inheritdoc/>
    ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken)
    {
        AsyncPredicateStopCoordinator<T> subscription = new(this, observer);
        return SubscriptionHelper.SubscribeAndDisposeOnFailureAsync(
            subscription,
            () => subscription.SubscribeSourcesAsync(cancellationToken));
    }
}
