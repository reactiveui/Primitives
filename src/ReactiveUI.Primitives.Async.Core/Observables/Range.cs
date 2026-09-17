// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides static methods for creating and manipulating asynchronous observable sequences.</summary>
public static partial class SignalAsync
{
    /// <summary>Creates an observable sequence that emits a range of consecutive integer values, starting from the specified value.</summary>
    /// <param name="start">The value of the first integer in the sequence.</param>
    /// <param name="count">The number of sequential integers to emit. Must be non-negative.</param>
    /// <returns>An observable sequence that emits integers from <paramref name="start"/> to <paramref name="start"/> + <paramref
    /// name="count"/> - 1, in order.</returns>
    /// <remarks>A <paramref name="count"/> of zero completes the sequence without emitting anything.</remarks>
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
        Justification = "Range is the Rx-compatible alias for Sequence and builds the same signal with no forwarding hop.")]
    public static IObservableAsync<int> Range(int start, int count) => new SequenceSignal(start, count);
}
