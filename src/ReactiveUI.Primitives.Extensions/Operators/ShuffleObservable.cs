// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Security.Cryptography;
using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>Operator that randomly shuffles arrays emitted by the source using unbiased random indices.</summary>
/// <typeparam name="T">The array element type.</typeparam>
/// <param name="source">The source observable emitting arrays.</param>
internal sealed class ShuffleObservable<T>(IObservable<T[]> source) : IObservable<T[]>
{
#if !NET8_0_OR_GREATER
    /// <summary>Number of values representable by a 32-bit unsigned integer.</summary>
    private const ulong UInt32ValueCount = 4_294_967_296UL;
#endif

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T[]> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(observer);
        return source.Subscribe(new ShuffleWitness(observer));
    }

    /// <summary>Observer that shuffles arrays in place.</summary>
    /// <param name="downstream">The downstream observer receiving shuffled arrays.</param>
    private sealed class ShuffleWitness(IObserver<T[]> downstream) : IObserver<T[]>
    {
        /// <inheritdoc/>
        public void OnNext(T[] value)
        {
            if (value is null)
            {
                downstream.OnNext(value!);
                return;
            }

            ShuffleInPlace(value);

            downstream.OnNext(value);
        }

        /// <inheritdoc/>
        public void OnError(Exception error) => downstream.OnError(error);

        /// <inheritdoc/>
        public void OnCompleted() => downstream.OnCompleted();

        /// <summary>Fisher-Yates shuffle using unbiased random indices.</summary>
        /// <param name="array">The array to shuffle in place.</param>
        private static void ShuffleInPlace(T[] array)
        {
#if !NET8_0_OR_GREATER
            using var generator = RandomNumberGenerator.Create();
            var buffer = new byte[sizeof(uint)];
#endif

            for (var n = array.Length - 1; n > 0; n--)
            {
#if NET8_0_OR_GREATER
                var k = RandomNumberGenerator.GetInt32(n + 1);
#else
                var k = NextIndex(generator, buffer, n + 1);
#endif
                (array[n], array[k]) = (array[k], array[n]);
            }
        }

#if !NET8_0_OR_GREATER
        /// <summary>Generates an unbiased index below the exclusive upper bound.</summary>
        /// <param name="generator">The random number generator.</param>
        /// <param name="buffer">Four-byte scratch buffer.</param>
        /// <param name="exclusiveUpperBound">The exclusive upper bound.</param>
        /// <returns>A random index in the requested range.</returns>
        private static int NextIndex(RandomNumberGenerator generator, byte[] buffer, int exclusiveUpperBound)
        {
            var upperBound = (uint)exclusiveUpperBound;
            var limit = UInt32ValueCount - (UInt32ValueCount % upperBound);
            ulong value;

            do
            {
                generator.GetBytes(buffer);
                value = BitConverter.ToUInt32(buffer, 0);
            }
            while (value >= limit);

            return (int)(value % upperBound);
        }
#endif
    }
}
