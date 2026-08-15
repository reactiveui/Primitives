// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Disposables;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Async observable that combines latest values from an enumerable of sources and projects through a selector.</summary>
/// <typeparam name="TSource">The source element type.</typeparam>
/// <typeparam name="TResult">The projected result type.</typeparam>
[System.Diagnostics.DebuggerDisplay("SourceCount = {Sources.Length}")]
public sealed class SyncLatestEnumerableSignal<TSource, TResult> : IObservableAsync<TResult>
{
    /// <summary>Initializes a new instance of the <see cref="SyncLatestEnumerableSignal{TSource, TResult}"/> class.</summary>
    /// <param name="sources">The source sequences to combine.</param>
    /// <param name="resultSelector">The result selector.</param>
    /// <exception cref="ArgumentNullException"><paramref name="sources"/> is <see langword="null"/>.</exception>
    public SyncLatestEnumerableSignal(
        IEnumerable<IObservableAsync<TSource>> sources,
        Func<IReadOnlyList<TSource>, TResult> resultSelector)
    {
        ArgumentExceptionHelper.ThrowIfNull(resultSelector);

        Sources = (sources as IObservableAsync<TSource>[])
                  ?? [.. sources ?? throw new ArgumentNullException(nameof(sources))];
        ResultSelector = resultSelector;
    }

    /// <summary>Gets the source sequences.</summary>
    private IObservableAsync<TSource>[] Sources { get; }

    /// <summary>Gets the result selector.</summary>
    private Func<IReadOnlyList<TSource>, TResult> ResultSelector { get; }

    /// <inheritdoc/>
    async ValueTask<IAsyncDisposable> IObservableAsync<TResult>.SubscribeAsync(
        IObserverAsync<TResult> observer,
        CancellationToken cancellationToken)
    {
        if (Sources.Length == 0)
        {
            await observer.OnCompletedAsync(Result.Success).ConfigureAwait(false);
            return DisposableAsync.Empty;
        }

        SyncLatestEnumerableCoordinator<TSource, TResult> subscription = new(Sources, observer, ResultSelector);
        return await SubscriptionHelper.SubscribeAndDisposeOnFailureAsync(
            subscription,
            () => subscription.SubscribeSourcesAsync(cancellationToken)).ConfigureAwait(false);
    }
}
