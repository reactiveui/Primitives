// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>An observable that projects source values to inner observables and merges those inner observables.</summary>
/// <typeparam name="TSource">The source element type.</typeparam>
/// <typeparam name="TResult">The result element type.</typeparam>
[System.Diagnostics.DebuggerDisplay("Source = {Source}, SyncSelector = {SyncSelector}, AsyncSelector = {AsyncSelector}")]
public sealed class FlatMapSignal<TSource, TResult> : IObservableAsync<TResult>
{
    /// <summary>Initializes a new instance of the <see cref="FlatMapSignal{TSource,TResult}"/> class.</summary>
    /// <param name="source">The source sequence.</param>
    /// <param name="selector">The synchronous projection.</param>
    public FlatMapSignal(IObservableAsync<TSource> source, Func<TSource, IObservableAsync<TResult>> selector)
    {
        Source = source;
        SyncSelector = selector;
    }

    /// <summary>Initializes a new instance of the <see cref="FlatMapSignal{TSource,TResult}"/> class.</summary>
    /// <param name="source">The source sequence.</param>
    /// <param name="selector">The asynchronous projection.</param>
    public FlatMapSignal(
        IObservableAsync<TSource> source,
        Func<TSource, CancellationToken, ValueTask<IObservableAsync<TResult>>> selector)
    {
        Source = source;
        AsyncSelector = selector;
    }

    /// <summary>Gets the source sequence.</summary>
    private IObservableAsync<TSource> Source { get; }

    /// <summary>Gets the synchronous projection.</summary>
    private Func<TSource, IObservableAsync<TResult>>? SyncSelector { get; }

    /// <summary>Gets the asynchronous projection.</summary>
    private Func<TSource, CancellationToken, ValueTask<IObservableAsync<TResult>>>? AsyncSelector { get; }

    /// <inheritdoc/>
    ValueTask<IAsyncDisposable> IObservableAsync<TResult>.SubscribeAsync(
        IObserverAsync<TResult> observer,
        CancellationToken cancellationToken)
    {
        FlatMapCoordinator<TResult> coordinator = new(observer);
        coordinator.LinkExternalCancellation(cancellationToken);

        return SubscriptionHelper.SubscribeAndDisposeOnFailureAsync(
            coordinator,
            async () =>
            {
                FlatMapWitness<TSource, TResult> sourceObserver = new(coordinator, SyncSelector, AsyncSelector);
                await coordinator.SetOuterObserverAsync(sourceObserver).ConfigureAwait(false);
                var subscription = await Source.SubscribeAsync(sourceObserver, cancellationToken).ConfigureAwait(false);
                await sourceObserver.AssignSourceSubscriptionAsync(subscription).ConfigureAwait(false);
            });
    }
}
