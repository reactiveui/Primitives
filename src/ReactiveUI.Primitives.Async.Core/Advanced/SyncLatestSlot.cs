// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Builds the observers that fill a <see cref="ISyncLatestCoordinator{TResult}"/>'s latest-value slots.</summary>
/// <remarks>
/// A coordinator owns one slot per source and emits its projection once every slot is filled. Creating a slot's observer
/// from its 0-based index keeps completion bits out of a custom coordinator.
/// </remarks>
public static class SyncLatestSlot
{
    /// <summary>Creates the observer that records one source's latest value and reports its completion.</summary>
    /// <typeparam name="TSource">The source element type.</typeparam>
    /// <typeparam name="TResult">The coordinator's downstream element type.</typeparam>
    /// <param name="coordinator">The coordinator that owns the slot.</param>
    /// <param name="sourceIndex">The 0-based index of the source's slot.</param>
    /// <param name="recordValue">Stores the source's latest value in the coordinator's slot.</param>
    /// <returns>The observer to subscribe to the source.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="coordinator"/> or <paramref name="recordValue"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="sourceIndex"/> is outside the coordinator's sources.</exception>
    public static SyncLatestIndexedWitness<TSource, TResult> CreateWitness<TSource, TResult>(
        ISyncLatestCoordinator<TResult> coordinator,
        int sourceIndex,
        Action<TSource> recordValue)
    {
        ArgumentExceptionHelper.ThrowIfNull(coordinator);
        ArgumentExceptionHelper.ThrowIfNull(recordValue);
        ThrowIfIndexOutOfRange(coordinator, sourceIndex);

        return new(coordinator, 1 << sourceIndex, recordValue);
    }

    /// <summary>Throws when a source index falls outside the coordinator's slots.</summary>
    /// <typeparam name="TResult">The downstream element type.</typeparam>
    /// <param name="coordinator">The coordinator whose slots bound the index.</param>
    /// <param name="sourceIndex">The 0-based index to check.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="sourceIndex"/> is outside the coordinator's sources.</exception>
    private static void ThrowIfIndexOutOfRange<TResult>(ISyncLatestCoordinator<TResult> coordinator, int sourceIndex)
    {
        var count = coordinator.Lifecycle.Subscriptions.Length;
        if (sourceIndex < 0 || sourceIndex >= count)
        {
            throw new ArgumentOutOfRangeException(
                nameof(sourceIndex),
                sourceIndex,
                $"The coordinator combines {count} sources, so the index must be between 0 and {count - 1}.");
        }
    }
}
