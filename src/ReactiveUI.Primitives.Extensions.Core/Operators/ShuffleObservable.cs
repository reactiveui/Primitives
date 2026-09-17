// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>Shuffles each array in place with non-cryptographic randomness and forwards the same instance, passing null through unchanged.</summary>
/// <typeparam name="T">The array element type.</typeparam>
/// <param name="source">The source observable emitting arrays.</param>
public sealed class ShuffleObservable<T>(IObservable<T[]> source) : IObservable<T[]>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T[]> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(observer);
        return source.Subscribe(new ShuffleWitness(observer));
    }

    /// <summary>Observer that reorders each array in place before forwarding it.</summary>
    /// <param name="downstream">The downstream observer receiving shuffled arrays.</param>
    [SuppressMessage(
        "Security",
        "CA5394:Do not use insecure randomness",
        Justification =
            "Array shuffling does not require cryptographic randomness.")]
    private sealed class ShuffleWitness(IObserver<T[]> downstream) : IObserver<T[]>
    {
#if !NET8_0_OR_GREATER
        /// <summary>Per-thread <see cref="Random"/> used by the netfx fallback path.</summary>
        [ThreadStatic]
        private static Random? _threadRandom;
#endif

        /// <inheritdoc/>
        public void OnNext(T[] value)
        {
            if (value is null)
            {
                var output = value!;
                downstream.OnNext(output);
                return;
            }

            ShuffleInPlace(value);

            downstream.OnNext(value);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => downstream.OnError(error);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted() => downstream.OnCompleted();

        /// <summary>Randomly reorders the array in place.</summary>
        /// <param name="array">The array to shuffle in place.</param>
#if NET8_0_OR_GREATER
        [ExcludeFromCodeCoverage]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void ShuffleInPlace(T[] array) => Random.Shared.Shuffle(array);
#else
        private static void ShuffleInPlace(T[] array)
        {
            for (var n = array.Length - 1; n > 0; n--)
            {
                var k = NextIndex(n + 1);
                (array[n], array[k]) = (array[k], array[n]);
            }
        }

        /// <summary>Returns a random index below the exclusive bound.</summary>
        /// <param name="exclusiveUpperBound">The exclusive upper bound.</param>
        /// <returns>A nonnegative index below the bound.</returns>
        [ExcludeFromCodeCoverage]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int NextIndex(int exclusiveUpperBound) => (_threadRandom ??= new()).Next(exclusiveUpperBound);
#endif
    }
}
