// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Async.Internal;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Shared subscribe behaviour for the arity-specific <c>SyncLatest</c> coordinators.</summary>
public static class SyncLatestCoordinator
{
    /// <summary>Subscribes to every source in index order, storing each subscription in the lifecycle's slots.</summary>
    /// <typeparam name="TResult">The downstream element type.</typeparam>
    /// <param name="coordinator">The coordinator whose sources are subscribed.</param>
    /// <param name="cancellationToken">A token to cancel the subscription.</param>
    /// <returns>A task representing the asynchronous subscribe operation.</returns>
    public static ValueTask SubscribeSourcesAsync<TResult>(
        ISyncLatestCoordinator<TResult> coordinator,
        CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(coordinator);
        return SubscribeSourcesCoreAsync(coordinator, cancellationToken);
    }

    /// <summary>Creates a sequence emitting <see langword="true"/> when the latest value from every source is <see langword="false"/>.</summary>
    /// <param name="sources">The boolean source sequences.</param>
    /// <returns>A sequence of aggregate boolean states.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservableAsync<bool> CreateAllFalse(IEnumerable<IObservableAsync<bool>> sources) =>
        SyncLatestAggregationHelper.CreateAllMatching(sources, expected: false);

    /// <summary>Creates a sequence emitting <see langword="true"/> when the latest value from every source is <see langword="true"/>.</summary>
    /// <param name="sources">The boolean source sequences.</param>
    /// <returns>A sequence of aggregate boolean states.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservableAsync<bool> CreateAllTrue(IEnumerable<IObservableAsync<bool>> sources) =>
        SyncLatestAggregationHelper.CreateAllMatching(sources, expected: true);

    /// <summary>Creates a sequence emitting the minimum of the latest values from the supplied struct-valued source sequences.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The first source sequence.</param>
    /// <param name="sources">Additional source sequences.</param>
    /// <returns>A sequence emitting the minimum of the latest values.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservableAsync<T> CreateMin<T>(IObservableAsync<T> source, params IObservableAsync<T>[] sources)
        where T : struct =>
        SyncLatestAggregationHelper.CreateExtremum(source, sources, findMaximum: false);

    /// <summary>Creates a sequence emitting the maximum of the latest values from the supplied struct-valued source sequences.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The first source sequence.</param>
    /// <param name="sources">Additional source sequences.</param>
    /// <returns>A sequence emitting the maximum of the latest values.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservableAsync<T> CreateMax<T>(IObservableAsync<T> source, params IObservableAsync<T>[] sources)
        where T : struct =>
        SyncLatestAggregationHelper.CreateExtremum(source, sources, findMaximum: true);

    /// <summary>Subscribes to every source after <paramref name="coordinator"/> has been validated.</summary>
    /// <typeparam name="TResult">The downstream element type.</typeparam>
    /// <param name="coordinator">The coordinator whose sources are subscribed.</param>
    /// <param name="cancellationToken">A token to cancel the subscription.</param>
    /// <returns>A task representing the asynchronous subscribe operation.</returns>
    private static async ValueTask SubscribeSourcesCoreAsync<TResult>(
        ISyncLatestCoordinator<TResult> coordinator,
        CancellationToken cancellationToken)
    {
        var subscriptions = coordinator.Lifecycle.Subscriptions;
        for (var i = 0; i < subscriptions.Length; i++)
        {
            subscriptions[i] = await coordinator.SubscribeAtAsync(i, cancellationToken).ConfigureAwait(false);
        }
    }
}
