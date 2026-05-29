// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Internal;

namespace ReactiveUI.Primitives.Async;

/// <summary>
/// Provides extension methods for working with asynchronous observable sequences.
/// </summary>
/// <remarks>The ObservableAsync class contains static methods that extend the functionality of asynchronous
/// observables, enabling advanced composition and control over asynchronous data streams. These methods are intended
/// for use with types that implement asynchronous observer patterns.</remarks>
public static partial class ObservableAsync
{
    /// <summary>
    /// Returns an observable sequence that yields control to the current thread's scheduler before emitting items from
    /// the source sequence.
    /// </summary>
    /// <remarks>This method can be used to ensure that the source sequence's emissions are scheduled
    /// asynchronously, which may help avoid stack overflows or improve responsiveness in certain scenarios.</remarks>
    /// <typeparam name="T">The type of the elements in the observable sequence.</typeparam>
    /// <param name="this">The source observable sequence to yield from.</param>
    /// <returns>An observable sequence that emits the same elements as the source, but yields control to the scheduler before
    /// each emission.</returns>
    public static IObservableAsync<T> Yield<T>(this IObservableAsync<T> @this)
    {
        ArgumentExceptionHelper.ThrowIfNull(@this);

        return new YieldObservable<T>(@this);
    }

    /// <summary>
    /// An observable that yields control to the current scheduler before forwarding source emissions.
    /// </summary>
    /// <typeparam name="T">The type of elements in the observable sequence.</typeparam>
    /// <param name="source">The source observable to yield from.</param>
    internal sealed class YieldObservable<T>(IObservableAsync<T> source) : ObservableAsync<T>
    {
        /// <inheritdoc/>
        protected override ValueTask<IAsyncDisposable> SubscribeAsyncCore(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            var currentContext = AsyncContext.GetCurrent();
            return source.SubscribeAsync(
                new ObserveOnAsyncObservable<T>.ObserveOnObserver(observer, currentContext, true),
                cancellationToken);
        }
    }
}
