// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Extensions.Reactive.Operators;
#else
namespace ReactiveUI.Primitives.Extensions.Operators;
#endif

/// <summary>Shared, cached observable singletons for frequently emitted trivial values.</summary>
public static class CachedObservables
{
    /// <summary>Gets a cached observable that synchronously emits a single <see cref="RxVoid.Default"/> and completes.</summary>
    public static IObservable<RxVoid> UnitDefault { get; } = new SingleValueObservable<RxVoid>(RxVoid.Default);
}
