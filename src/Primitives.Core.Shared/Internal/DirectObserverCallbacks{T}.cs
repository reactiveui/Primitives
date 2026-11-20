// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if PRIMITIVES_CORE
namespace ReactiveUI.Primitives.Core.Internal;
#elif REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Internal;
#else
namespace ReactiveUI.Primitives.Internal;
#endif

/// <summary>Groups validated value, error, and completion callbacks for direct inline subscription emission.</summary>
/// <typeparam name="T">The emitted element type.</typeparam>
/// <param name="OnNext">The value callback.</param>
/// <param name="OnError">The error callback.</param>
/// <param name="OnCompleted">The completion callback.</param>
internal readonly record struct DirectObserverCallbacks<T>(
    Action<T> OnNext,
    Action<Exception> OnError,
    Action OnCompleted);
