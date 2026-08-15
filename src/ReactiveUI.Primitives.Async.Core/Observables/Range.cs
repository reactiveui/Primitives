// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides static methods for creating and manipulating asynchronous observable sequences.</summary>
/// <remarks>The SignalAsync class offers factory methods and utilities for working with asynchronous
/// observables, enabling reactive programming patterns with support for asynchronous event streams. Members of this
/// class are thread-safe and designed for use in concurrent environments.</remarks>
public static partial class SignalAsync
{
    /// <summary>Creates an observable sequence that emits a range of consecutive integer values, starting from the specified value.</summary>
    /// <param name="start">The value of the first integer in the sequence.</param>
    /// <param name="count">The number of sequential integers to emit. Must be non-negative.</param>
    /// <returns>An observable sequence that emits integers from <paramref name="start"/> to <paramref name="start"/> + <paramref
    /// name="count"/> - 1, in order.</returns>
    /// <remarks>The sequence completes after emitting all values. If <paramref name="count"/> is zero, the
    /// sequence completes immediately without emitting any values. The operation supports cancellation via the
    /// observer's cancellation token.</remarks>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservableAsync<int> Sequence(int start, int count) => new SequenceSignal(start, count);

    /// <summary>Creates an observable sequence that emits a range of consecutive integer values, starting from the specified value.</summary>
    /// <param name="start">The value of the first integer in the sequence.</param>
    /// <param name="count">The number of sequential integers to emit. Must be non-negative.</param>
    /// <returns>An observable sequence that emits integers from <paramref name="start"/> to <paramref name="start"/> + <paramref
    /// name="count"/> - 1, in order.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Design",
        "SST2318:Members should not have identical bodies",
        Justification =
            "Range is the System.Reactive name for Sequence. Both operators intentionally build the same signal "
            + "directly rather than one forwarding to the other, so the Rx-named alias costs nothing at the call site.")]
    public static IObservableAsync<int> Range(int start, int count) => new SequenceSignal(start, count);
}
