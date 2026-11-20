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

/// <summary>Carries two adjacent fused operator delegates without allocating a closure.</summary>
/// <typeparam name="TFirst">The first delegate type.</typeparam>
/// <typeparam name="TSecond">The second delegate type.</typeparam>
/// <param name="First">The first stage delegate.</param>
/// <param name="Second">The second stage delegate.</param>
internal readonly record struct StagePair<TFirst, TSecond>(TFirst First, TSecond Second);
