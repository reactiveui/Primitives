// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Internal;
#else
namespace ReactiveUI.Primitives.Internal;
#endif

/// <summary>Holds two matched <see cref="RangeSignal"/> instances and the integer-specialized binary projection delegate.</summary>
/// <typeparam name="TResult">The projected result type.</typeparam>
/// <param name="Left">The left integer range source.</param>
/// <param name="Right">The right integer range source.</param>
/// <param name="Selector">The integer-specialized projection delegate.</param>
internal readonly record struct BinaryIntRanges<TResult>(
    RangeSignal Left,
    RangeSignal Right,
    Func<int, int, TResult> Selector);
