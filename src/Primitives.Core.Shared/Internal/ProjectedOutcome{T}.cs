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

/// <summary>Represents the result of evaluating a fused projection-and-filter step for one element.</summary>
/// <typeparam name="T">The projected element type.</typeparam>
/// <param name="HasValue"><see langword="true"/> when the element passed the filter and <paramref name="Value"/> should be forwarded.</param>
/// <param name="Value">The projected element when <paramref name="HasValue"/> is <see langword="true"/>.</param>
internal readonly record struct ProjectedOutcome<T>(bool HasValue, T Value);
