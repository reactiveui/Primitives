// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>
/// Operator that randomly shuffles arrays emitted by the source. The shuffle is not
/// cryptographically secure — callers needing crypto-grade randomness should compose
/// <see cref="System.Security.Cryptography.RandomNumberGenerator"/> themselves.
/// </summary>
/// <typeparam name="T">The array element type.</typeparam>
/// <param name="source">The source observable emitting arrays.</param>
internal sealed class ShuffleObservable<T>(IObservable<T[]> source) : IObservable<T[]>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T[]> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(observer);
        return source.Subscribe(new ShuffleWitness(observer));
    }

    /// <summary>Observer that shuffles arrays in place.</summary>
    /// <param name="downstream">The downstream observer receiving shuffled arrays.</param>
    [SuppressMessage("Security", "CA5394:Do not use insecure randomness", Justification = "Shuffle is non-cryptographic; Random is faster.")]
    private sealed class ShuffleWitness(IObserver<T[]> downstream) : IObserver<T[]>
    {
#if !NET8_0_OR_GREATER
        /// <summary>Per-thread <see cref="Random"/> used by the netfx fallback path.</summary>
        [ThreadStatic]
        [SuppressMessage("Major Code Smell", "S2743:Static fields should not be used in generic types", Justification = "The netfx fallback keeps a per-thread random instance for each closed shuffle witness type.")]
        private static Random? _threadRandom;
#endif

        /// <inheritdoc/>
        public void OnNext(T[] value)
        {
            if (value is null)
            {
                downstream.OnNext(value!);
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
        public void OnError(Exception error) => downstream.OnError(error);

        /// <inheritdoc/>
        public void OnCompleted() => downstream.OnCompleted();

#if !NET8_0_OR_GREATER
        /// <summary>Fisher-Yates over a per-thread <see cref="Random"/> for targets without <c>Random.Shuffle</c>.</summary>
        /// <param name="array">The array to shuffle in place.</param>
        private static void ShuffleInPlace(T[] array)
        {
            var random = _threadRandom;
            if (random is null)
            {
                random = new Random();
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
