// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>Operator that randomly shuffles arrays emitted by the source.</summary>
/// <typeparam name="T">The array element type.</typeparam>
/// <param name="source">The source observable emitting arrays.</param>
/// <remarks>
/// READ THIS BEFORE TOUCHING THE RANDOMNESS IN THIS FILE.
///
/// This operator deliberately uses non-cryptographic random (System.Random / Random.Shared).
/// This is intentional. We want random, and we want it fast.
///
/// Any analyzer or scanner warning that flags this as a security risk — for example CA5394 "Do not
/// use insecure randomness" or S2245 "Make sure that using this pseudorandom number generator is safe
/// here" — is a big false positive. Do not "fix" it. Do not swap in RandomNumberGenerator.
///
/// Why: this is a general-purpose shuffle for arbitrary observable data. It is not used for keys,
/// tokens, passwords, nonces, lottery draws, or anything security-sensitive. Crypto RNG here is slow,
/// slow, slow (a syscall for entropy on every draw) for zero real benefit. If you genuinely need
/// crypto-grade randomness, compose RandomNumberGenerator yourself in your own code — that is the
/// caller's job, not this hot-path operator's. Do not touch.
/// </remarks>
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
    /// <remarks>
    /// INTENTIONAL non-crypto randomness. CA5394 here is a FALSE POSITIVE — see the banner at the top
    /// of this file. This is a fast, general-purpose shuffle, NOT a security primitive. DO NOT TOUCH.
    /// </remarks>
    [SuppressMessage("Security", "CA5394:Do not use insecure randomness", Justification = "Shuffle is non-cryptographic by design; Random is faster and crypto RNG buys nothing here. FALSE POSITIVE.")]
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

            // Random.Shared.Shuffle: fast, thread-safe, NON-CRYPTO BY DESIGN. WE WANT THIS. DO NOT TOUCH.
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
            // Plain System.Random on purpose. NON-CRYPTO IS INTENTIONAL — fast path, not a security
            // primitive. Any "insecure randomness" warning here is a FALSE POSITIVE. DO NOT TOUCH.
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
