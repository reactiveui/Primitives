// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Implemented by single-stage synchronous operators that can fuse a trailing synchronous <c>Map</c> or <c>Select</c>.</summary>
/// <typeparam name="T">The output type of the first stage.</typeparam>
internal interface IMapFusableAsync<T>
{
    /// <summary>Fuses a synchronous projection onto this stage without adding a second witness layer.</summary>
    /// <typeparam name="TDest">The final projected element type.</typeparam>
    /// <param name="selector">The trailing synchronous selector.</param>
    /// <returns>The fused observable sequence, or <see langword="null"/> when fusion is not applicable.</returns>
    IObservableAsync<TDest>? TryFuseMap<TDest>(Func<T, TDest> selector);
}
