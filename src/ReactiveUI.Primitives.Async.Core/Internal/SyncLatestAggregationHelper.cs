// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Advanced;

namespace ReactiveUI.Primitives.Async.Internal;

/// <summary>Shared boolean-conjunction and min/max aggregation builders for <see cref="SyncLatestCoordinator"/>.</summary>
internal static class SyncLatestAggregationHelper
{
    /// <summary>Creates a sequence emitting <see langword="true"/> when the latest value from every source equals <paramref name="expected"/>.</summary>
    /// <param name="sources">The boolean source sequences.</param>
    /// <param name="expected">The required boolean value across every source.</param>
    /// <returns>A sequence of aggregate boolean states.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="sources"/> is <see langword="null"/>.</exception>
    internal static IObservableAsync<bool> CreateAllMatching(IEnumerable<IObservableAsync<bool>> sources, bool expected)
    {
        ArgumentExceptionHelper.ThrowIfNull(sources);

        var materializedSources = (sources as IObservableAsync<bool>[]) ?? [.. sources];
        if (materializedSources.Length == 0)
        {
            return new SignalAsync.ReturnSignalAsync<bool>(true);
        }

        return expected
            ? new SyncLatestEnumerableSignal<bool, bool>(materializedSources, AllTrue)
            : new SyncLatestEnumerableSignal<bool, bool>(materializedSources, AllFalse);
    }

    /// <summary>Creates a sequence emitting either the minimum or maximum of the latest values from the supplied struct-valued source sequences.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The first source sequence.</param>
    /// <param name="sources">Additional source sequences.</param>
    /// <param name="findMaximum"><see langword="true"/> to select the maximum; <see langword="false"/> to select the minimum.</param>
    /// <returns>A sequence emitting the selected extremum of the latest values.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="sources"/> is <see langword="null"/>.</exception>
    internal static IObservableAsync<T> CreateExtremum<T>(
        IObservableAsync<T> source,
        IObservableAsync<T>[] sources,
        bool findMaximum)
        where T : struct
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(sources);

        if (sources.Length == 0)
        {
            return source;
        }

        var allSources = new IObservableAsync<T>[sources.Length + 1];
        allSources[0] = source;
        sources.CopyTo(allSources, 1);
#if NET11_0_OR_GREATER
        return findMaximum
            ? new SyncLatestEnumerableSignal<T, T>(allSources, static values => ((ReadOnlySpan<T>)(T[])values).Max())
            : new SyncLatestEnumerableSignal<T, T>(allSources, static values => ((ReadOnlySpan<T>)(T[])values).Min());
#else
        return findMaximum
            ? new SyncLatestEnumerableSignal<T, T>(allSources, SelectMax)
            : new SyncLatestEnumerableSignal<T, T>(allSources, SelectMin);
#endif
    }

    /// <summary>Determines whether every element in <paramref name="values"/> is <see langword="true"/>.</summary>
    /// <param name="values">The latest boolean values.</param>
    /// <returns><see langword="true"/> when all elements are <see langword="true"/>; otherwise, <see langword="false"/>.</returns>
    internal static bool AllTrue(IReadOnlyList<bool> values)
    {
        for (var i = 0; i < values.Count; i++)
        {
            if (!values[i])
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>Determines whether every element in <paramref name="values"/> is <see langword="false"/>.</summary>
    /// <param name="values">The latest boolean values.</param>
    /// <returns><see langword="true"/> when all elements are <see langword="false"/>; otherwise, <see langword="false"/>.</returns>
    internal static bool AllFalse(IReadOnlyList<bool> values)
    {
        for (var i = 0; i < values.Count; i++)
        {
            if (values[i])
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>Selects the minimum element from <paramref name="values"/> using <see cref="Comparer{T}.Default"/>.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="values">The non-empty latest values.</param>
    /// <returns>The minimum element.</returns>
    internal static T SelectMin<T>(IReadOnlyList<T> values)
    {
        var best = values[0];
        for (var i = 1; i < values.Count; i++)
        {
            if (Comparer<T>.Default.Compare(values[i], best) < 0)
            {
                best = values[i];
            }
        }

        return best;
    }

    /// <summary>Selects the maximum element from <paramref name="values"/> using <see cref="Comparer{T}.Default"/>.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="values">The non-empty latest values.</param>
    /// <returns>The maximum element.</returns>
    internal static T SelectMax<T>(IReadOnlyList<T> values)
    {
        var best = values[0];
        for (var i = 1; i < values.Count; i++)
        {
            if (Comparer<T>.Default.Compare(values[i], best) > 0)
            {
                best = values[i];
            }
        }

        return best;
    }
}
