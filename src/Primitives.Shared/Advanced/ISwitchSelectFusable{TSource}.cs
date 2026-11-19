// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Type-erased hook allowing <c>KeepNotNull().Map(selector).SwitchTo()</c> to collapse into <c>SwitchSelect</c>.</summary>
/// <typeparam name="TSource">The non-null source type.</typeparam>
internal interface ISwitchSelectFusable<out TSource>
{
    /// <summary>Creates a <see cref="SwitchMapSignal{TSource, TResult}"/> that skips null values from the raw source.</summary>
    /// <typeparam name="TInner">The inner sequence element type.</typeparam>
    /// <param name="selector">The inner sequence projection.</param>
    /// <returns>A single fused switch-select signal.</returns>
    IObservable<TInner> FuseSwitchSelect<TInner>(Func<TSource, IObservable<TInner>> selector);
}
