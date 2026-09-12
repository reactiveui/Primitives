// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>Shuffles arrays in place using non-cryptographic randomness.</summary>
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

    /// <summary>Randomizes buffered values for non-security use.</summary>
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

#if NET8_0_OR_GREATER
            Random.Shared.Shuffle(value);
#else
            ShuffleInPlace(value);
#endif

            downstream.OnNext(value);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => downstream.OnError(error);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted() => downstream.OnCompleted();

#if !NET8_0_OR_GREATER
        /// <summary>Fisher-Yates over a per-thread <see cref="Random"/> for targets without <c>Random.Shuffle</c>.</summary>
        /// <param name="array">The array to shuffle in place.</param>
        private static void ShuffleInPlace(T[] array)
        {
            // This shuffle requires no cryptographic randomness.
            var random = _threadRandom;
            if (random is null)
            {
                random = new();
                _threadRandom = random;
            }

            for (var n = array.Length - 1; n > 0; n--)
            {
                var k = random.Next(n + 1);
                (array[n], array[k]) = (array[k], array[n]);
            }
        }
#endif
    }
}
