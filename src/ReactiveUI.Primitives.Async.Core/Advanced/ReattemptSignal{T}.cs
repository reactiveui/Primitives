// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>An observable that resubscribes to the source after terminal failures.</summary>
/// <typeparam name="T">The element type.</typeparam>
[System.Diagnostics.DebuggerDisplay("ReattemptSignal: Source = {Source}, RetryCount = {RetryCount}")]
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

    /// <summary>Creates a <see cref="ReattemptSignal{T}"/> that retries up to <paramref name="retryCount"/> extra times.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="retryCount">The non-negative number of extra retry attempts.</param>
    /// <returns>A configured <see cref="ReattemptSignal{T}"/>.</returns>
    public static ReattemptSignal<T> Create(IObservableAsync<T> source, int retryCount)
    {
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegative(retryCount);

        return new(source, retryCount);
    }

    /// <summary>Creates a <see cref="ReattemptSignal{T}"/> that retries on error up to <see cref="int.MaxValue"/> times.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <returns>A configured <see cref="ReattemptSignal{T}"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ReattemptSignal<T> CreateForever(IObservableAsync<T> source) =>
        new(source, int.MaxValue);

    /// <summary>Creates an observable sequence that runs <paramref name="source"/> up to <paramref name="retryCount"/> times in total.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="retryCount">The non-negative total run count.</param>
    /// <returns>An empty sequence when <paramref name="retryCount"/> is zero; otherwise a <see cref="ReattemptSignal{T}"/>.</returns>
    public static IObservableAsync<T> CreateTotalRuns(IObservableAsync<T> source, int retryCount)
    {
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegative(retryCount);

        return retryCount == 0
            ? SignalAsync.Empty<T>()
            : new ReattemptSignal<T>(source, retryCount - 1);
    }

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
