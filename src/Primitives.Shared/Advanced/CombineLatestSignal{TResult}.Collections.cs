// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>A combine-latest signal, carrying the factory for a variable number of same-typed sources.</summary>
[System.Diagnostics.DebuggerDisplay("CombineLatestSignal: Connect = {_connect}")]
public sealed partial class CombineLatestSignal<TResult>
{
    /// <summary>Creates a combine-latest signal over a variable number of same-typed sources.</summary>
    /// <typeparam name="T">The element type shared by every source.</typeparam>
    /// <param name="sources">The source observables.</param>
    /// <param name="selector">The selector that projects the subscription's latest-value slots.</param>
    /// <returns>The combine-latest signal.</returns>
    internal static CombineLatestSignal<TResult> Create<T>(
        IObservable<T>[] sources,
        Func<CombineLatestSlot<TResult, T>[], TResult> selector) =>
        new(coordinator =>
        {
            var slots = new CombineLatestSlot<TResult, T>[sources.Length];
            for (var i = 0; i < sources.Length; i++)
            {
                slots[i] = coordinator.Attach(sources[i]);
            }

            return () => selector(slots);
        });
}
