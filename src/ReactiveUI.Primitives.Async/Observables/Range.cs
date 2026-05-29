// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async;

/// <summary>
/// Provides static methods for creating and manipulating asynchronous observable sequences.
/// </summary>
/// <remarks>The ObservableAsync class offers factory methods and utilities for working with asynchronous
/// observables, enabling reactive programming patterns with support for asynchronous event streams. Members of this
/// class are thread-safe and designed for use in concurrent environments.</remarks>
public static partial class ObservableAsync
{
    /// <summary>
    /// Creates an observable sequence that emits a range of consecutive integer values, starting from the specified
    /// value.
    /// </summary>
    /// <remarks>The sequence completes after emitting all values. If <paramref name="count"/> is zero, the
    /// sequence completes immediately without emitting any values. The operation supports cancellation via the
    /// observer's cancellation token.</remarks>
    /// <param name="start">The value of the first integer in the sequence.</param>
    /// <param name="count">The number of sequential integers to emit. Must be non-negative.</param>
    /// <returns>An observable sequence that emits integers from <paramref name="start"/> to <paramref name="start"/> + <paramref
    /// name="count"/> - 1, in order.</returns>
    public static IObservableAsync<int> Range(int start, int count) => CreateAsBackgroundJob<int>(
        async (observer, cancellationToken) =>
        {
            for (var i = 0; i < count; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                // Pass CancellationToken.None to the observer so its TryEnter takes the
                // None fast path; the pump loop's ThrowIfCancellationRequested still terminates
                // the sequence on subscription disposal. Without this, every observer in the
                // chain allocated a Linked2CancellationTokenSource per emission, dominating the
                // GC profile of CountAsync / LastAsync / ToListAsync.
                await observer.OnNextAsync(start + i, CancellationToken.None).ConfigureAwait(false);
            }

            await observer.OnCompletedAsync(Result.Success).ConfigureAwait(false);
        },
        true);
}
