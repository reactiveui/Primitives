// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Internal;
#else
namespace ReactiveUI.Primitives.Internal;
#endif

/// <summary>Groups non-capturing value, error, and completion callbacks for one side of a binary coordinator.</summary>
/// <typeparam name="TOwner">The owning coordinator or witness type.</typeparam>
/// <typeparam name="T">The observed element type.</typeparam>
/// <param name="OnNext">The value callback.</param>
/// <param name="OnError">The error callback.</param>
/// <param name="OnCompleted">The completion callback.</param>
/// <param name="DisposeArmOnCompleted">When <see langword="true"/>, disposes this arm's upstream subscription before invoking <paramref name="OnCompleted"/>.</param>
internal readonly record struct BinaryArmCallbacks<TOwner, T>(
    Action<TOwner, T> OnNext,
    Action<TOwner, Exception> OnError,
    Action<TOwner> OnCompleted,
    bool DisposeArmOnCompleted = false);
