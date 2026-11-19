// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Type-erased fusion hooks exposed by a single-stage <c>Map</c> / <c>Select</c> signal.</summary>
/// <typeparam name="TResult">The output element type of the map stage.</typeparam>
internal interface IMapFusable<TResult>
{
    /// <summary>Fuses a subsequent <c>Map</c> / <c>Select</c> stage with this projection.</summary>
    /// <typeparam name="TNext">The next projected type.</typeparam>
    /// <param name="nextSelector">The second projection function.</param>
    /// <returns>A single two-stage fused projection signal.</returns>
    IObservable<TNext> FuseMap<TNext>(Func<TResult, TNext> nextSelector);

    /// <summary>Fuses a subsequent <c>Keep</c> / <c>Where</c> predicate with this projection.</summary>
    /// <param name="predicate">The filter predicate applied to projected values.</param>
    /// <returns>A single fused map-then-keep signal.</returns>
    IObservable<TResult> FuseKeep(Func<TResult, bool> predicate);

    /// <summary>Fuses a subsequent <c>KeepNotNull</c> / <c>WhereNotNull</c> filter with this projection.</summary>
    /// <typeparam name="TNonNull">The non-null reference result type.</typeparam>
    /// <returns>A single fused map-then-not-null signal.</returns>
    IObservable<TNonNull> FuseKeepNotNull<TNonNull>()
        where TNonNull : class;

    /// <summary>Fuses a subsequent <c>KeepType</c> / <c>OfType</c> filter with this projection.</summary>
    /// <typeparam name="TTarget">The target type to match.</typeparam>
    /// <returns>A single fused map-then-type-test signal.</returns>
    IObservable<TTarget> FuseKeepType<TTarget>();

    /// <summary>Fuses a subsequent <c>CastTo</c> / <c>Cast</c> conversion with this projection.</summary>
    /// <typeparam name="TTarget">The target cast type.</typeparam>
    /// <returns>A single fused map-then-cast signal.</returns>
    IObservable<TTarget> FuseCast<TTarget>();

    /// <summary>Fuses a subsequent <c>SwitchTo</c> / <c>Switch</c> operator into <see cref="SwitchMapSignal{TSource, TResult}"/>.</summary>
    /// <typeparam name="TInner">The inner sequence element type.</typeparam>
    /// <returns>A single fused switch-map signal.</returns>
    IObservable<TInner> FuseSwitch<TInner>();
}
