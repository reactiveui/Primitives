// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides Rx-compatible zip names for asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Zip operators for a first observable source sequence.</summary>
    /// <typeparam name="T1">The type of elements in the first source sequence.</typeparam>
    /// <param name="src1">The first observable sequence.</param>
    extension<T1>(IObservableAsync<T1> src1)
    {
        /// <summary>Combines two observable sequences element-by-element using the specified result selector.</summary>
        /// <typeparam name="T2">The type of elements in the second source sequence.</typeparam>
        /// <typeparam name="TResult">The type of elements in the result sequence.</typeparam>
        /// <param name="second">The second observable sequence.</param>
        /// <param name="resultSelector">A function to apply to each pair of elements.</param>
        /// <returns>An observable sequence whose elements are the pair-wise combination of source elements.</returns>
        public IObservableAsync<TResult> Zip<T2, TResult>(
            IObservableAsync<T2> second,
            Func<T1, T2, TResult> resultSelector)
        {
            ArgumentExceptionHelper.ThrowIfNull(src1);
            ArgumentExceptionHelper.ThrowIfNull(second);
            ArgumentExceptionHelper.ThrowIfNull(resultSelector);

            return new ZipSignal<T1, T2, TResult>(src1, second, resultSelector);
        }

        /// <summary>Combines two observable sequences element-by-element into pairs.</summary>
        /// <typeparam name="T2">The type of elements in the second source sequence.</typeparam>
        /// <param name="second">The second observable sequence.</param>
        /// <returns>An observable sequence of tuples pairing elements from each source.</returns>
        public IObservableAsync<(T1 First, T2 Second)> Zip<T2>(IObservableAsync<T2> second)
        {
            ArgumentExceptionHelper.ThrowIfNull(src1);
            ArgumentExceptionHelper.ThrowIfNull(second);

            return new ZipSignal<T1, T2, (T1 First, T2 Second)>(
                src1,
                second,
                static (firstValue, secondValue) => (firstValue, secondValue));
        }
    }
}
