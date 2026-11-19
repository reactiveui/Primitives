// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Async observable that emits items from the source until the specified predicate returns true.</summary>
/// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
[System.Diagnostics.DebuggerDisplay("PredicateStopSignal: Predicate = {Predicate}")]
public sealed class PredicateStopSignal<T> : IObservableAsync<T>
{
    /// <summary>Initializes a new instance of the <see cref="PredicateStopSignal{T}"/> class.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="predicate">The predicate that signals when to stop emitting items.</param>
    public PredicateStopSignal(IObservableAsync<T> source, Func<T, bool> predicate)
        : this(source, predicate, default)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="PredicateStopSignal{T}"/> class.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="predicate">The predicate that signals when to stop emitting items.</param>
    /// <param name="stopToken">A token whose cancellation also completes the sequence.</param>
    public PredicateStopSignal(
        IObservableAsync<T> source,
        Func<T, bool> predicate,
        CancellationToken stopToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(predicate);
        Source = source;
        Predicate = predicate;
        StopToken = stopToken;
    }

    /// <summary>Gets the predicate that signals when to stop emitting items.</summary>
    internal Func<T, bool> Predicate { get; }

    /// <summary>Gets the source observable sequence.</summary>
    internal IObservableAsync<T> Source { get; }

    /// <summary>Gets a token whose cancellation also completes the sequence.</summary>
    internal CancellationToken StopToken { get; }

    /// <inheritdoc/>
    ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken)
    {
        PredicateStopCoordinator<T> subscription = new(this, observer);
        return SubscriptionHelper.SubscribeAndDisposeOnFailureAsync(
            subscription,
            () => subscription.SubscribeSourcesAsync(cancellationToken));
    }
}
