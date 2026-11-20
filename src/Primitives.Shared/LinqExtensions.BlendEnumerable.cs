// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
using ReactiveUI.Primitives.Reactive.Advanced;

namespace ReactiveUI.Primitives.Reactive;
#else
using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives;
#endif

/// <summary>Enumerable source overloads for blend operators.</summary>
public static partial class LinqExtensions
{
    /// <summary>Operators for enumerable observable sources.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="sources">The observable sources.</param>
    extension<T>(IEnumerable<IObservable<T>> sources)
    {
        /// <summary>Concurrently merges the supplied observable sources.</summary>
        /// <returns>An observable that forwards values from every source.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> Blend() => new EnumerableBlendSignal<T>(sources);

        /// <summary>Concurrently merges the supplied observable sources with a maximum number of active subscriptions.</summary>
        /// <param name="maxConcurrent">The maximum number of sources to subscribe to at the same time.</param>
        /// <returns>An observable that forwards values from every source.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> Blend(int maxConcurrent) =>
            MaxConcurrentEnumerableBlendSignal<T>.Create(sources, maxConcurrent);
    }
}
