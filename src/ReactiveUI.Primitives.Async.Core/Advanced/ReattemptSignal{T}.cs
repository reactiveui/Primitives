// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>An observable that resubscribes to the source after terminal failures.</summary>
/// <typeparam name="T">The element type.</typeparam>
public sealed class ReattemptSignal<T> : IObservableAsync<T>
{
    /// <summary>Initializes a new instance of the <see cref="ReattemptSignal{T}"/> class.</summary>
    /// <param name="source">The source sequence.</param>
    /// <param name="retryCount">The number of retry attempts.</param>
    public ReattemptSignal(IObservableAsync<T> source, int retryCount)
    {
        Source = source;
        RetryCount = retryCount;
    }

    /// <summary>Gets the source sequence.</summary>
    private IObservableAsync<T> Source { get; }

    /// <summary>Gets the retry count.</summary>
    private int RetryCount { get; }

    /// <inheritdoc/>
    async ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken)
    {
        ReattemptSubscription<T> subscription = new(Source, observer, RetryCount, cancellationToken);
        await subscription.SubscribeOnceAsync().ConfigureAwait(false);
        return subscription;
    }
}
