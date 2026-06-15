// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Async.Reactive;
#else
namespace ReactiveUI.Primitives.Async;
#endif

/// <summary>Provides factory methods that emit shim-typed values.</summary>
public static partial class SignalAsyncReactiveExtensions
{
    /// <summary>Emits a single <see cref="RxVoid"/> value.</summary>
    /// <returns>An observable sequence that emits a single <see cref="RxVoid"/> value.</returns>
    public static IObservableAsync<RxVoid> EmitRxVoid() => SignalAsync.Emit(RxVoid.Default);
}
