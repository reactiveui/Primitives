// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Attaches sources to a <see cref="ISyncLatestCoordinator{TResult}"/> that combines their latest values.</summary>
public static class SyncLatestCoordinatorExtensions
{
    /// <summary>Wiring for a source that fills one of a coordinator's latest-value slots.</summary>
    /// <typeparam name="TSource">The source element type.</typeparam>
    /// <param name="source">The source to wire.</param>
    extension<TSource>(IObservableAsync<TSource> source)
    {
        /// <summary>Subscribes this source to its slot, so its values are combined and its completion is counted.</summary>
        /// <typeparam name="TResult">The coordinator's downstream element type.</typeparam>
        /// <param name="coordinator">The coordinator that owns the slot.</param>
        /// <param name="sourceIndex">The 0-based index of this source's slot.</param>
        /// <param name="recordValue">Stores this source's latest value in the coordinator's slot.</param>
        /// <param name="cancellationToken">A token to cancel the subscription.</param>
        /// <returns>This source's subscription, which the caller stores in the coordinator's lifecycle slot.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/>, <paramref name="coordinator"/> or <paramref name="recordValue"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="sourceIndex"/> is outside the coordinator's sources.</exception>
        public ValueTask<IAsyncDisposable> SubscribeToSlotAsync<TResult>(
            ISyncLatestCoordinator<TResult> coordinator,
            int sourceIndex,
            Action<TSource> recordValue,
            CancellationToken cancellationToken)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return source.SubscribeAsync(
                SyncLatestSlot.CreateWitness(coordinator, sourceIndex, recordValue),
                cancellationToken);
        }
    }

    /// <summary>Wiring for a coordinator that combines the latest value of several sources.</summary>
    /// <typeparam name="TResult">The downstream element type.</typeparam>
    /// <param name="coordinator">The coordinator whose sources are subscribed.</param>
    extension<TResult>(ISyncLatestCoordinator<TResult> coordinator)
    {
        /// <summary>Subscribes every source in index order, storing each subscription in the coordinator's lifecycle slots.</summary>
        /// <param name="cancellationToken">A token to cancel the subscriptions.</param>
        /// <returns>A task that completes once every source is subscribed.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="coordinator"/> is <see langword="null"/>.</exception>
        public ValueTask SubscribeSourcesAsync(CancellationToken cancellationToken)
        {
            ArgumentExceptionHelper.ThrowIfNull(coordinator);

            return SyncLatestCoordinator.SubscribeSourcesAsync(coordinator, cancellationToken);
        }
    }
}
