// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
#if REACTIVE_SHIM
using ReactiveUI.Primitives.Reactive.Advanced;
#else
using ReactiveUI.Primitives.Advanced;
#endif

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Internal;
#else
namespace ReactiveUI.Primitives.Internal;
#endif

/// <summary>Emits synchronous <c>Switch</c> subscriptions over an indexable enumerable of <see cref="RangeSignal"/> sources directly at subscribe time.</summary>
internal static class SwitchRangeOptimizer
{
    /// <summary>Pushes every range directly to <paramref name="observer"/> when <paramref name="sources"/> holds only <see cref="RangeSignal"/> elements at subscribe time.</summary>
    /// <typeparam name="T">The inner element type.</typeparam>
    /// <param name="sources">The outer observable of inner observables.</param>
    /// <param name="observer">The downstream observer.</param>
    /// <returns><see langword="true"/> when <paramref name="sources"/> was emitted directly; otherwise, <see langword="false"/>.</returns>
    internal static bool TrySubscribeRanges<T>(
        IObservable<IObservable<T>> sources,
        IObserver<T> observer)
    {
        if (typeof(T) != typeof(int)
            || sources is not FromEnumerableSignal<IObservable<T>> enumerable
            || !enumerable.TryGetReadOnlyValues(out var innerSources))
        {
            return false;
        }

        for (var i = 0; i < innerSources.Count; i++)
        {
            if (innerSources[i] is not RangeSignal)
            {
                return false;
            }
        }

        var intObserver = Unsafe.As<IObserver<int>>(observer);
        for (var rangeIndex = 0; rangeIndex < innerSources.Count; rangeIndex++)
        {
            if (innerSources[rangeIndex] is not RangeSignal range)
            {
                continue;
            }

            var start = range.Start;
            var count = range.Count;
            for (var i = 0; i < count; i++)
            {
                intObserver.OnNext(start + i);
            }
        }

        observer.OnCompleted();
        return true;
    }
}
