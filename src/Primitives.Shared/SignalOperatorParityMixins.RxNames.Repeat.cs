// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive;
#else
namespace ReactiveUI.Primitives;
#endif

/// <summary>System.Reactive-named repetition operators for observable sources.</summary>
public static partial class LinqExtensions
{
    /// <summary>System.Reactive-named repetition operators for an observable source sequence.</summary>
    /// <param name="source">The source sequence.</param>
    /// <typeparam name="T">The value type.</typeparam>
    extension<T>(IObservable<T> source)
    {
        /// <summary>Repeats the source sequence indefinitely.</summary>
        /// <returns>An observable sequence that repeats the source sequence indefinitely.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        public IObservable<T> Repeat()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new RepeatSourceSignal<T>(source, null);
        }

        /// <summary>Repeats the source sequence a fixed number of times.</summary>
        /// <param name="repeatCount">The number of times to repeat the source sequence.</param>
        /// <returns>An observable sequence that repeats the source sequence <paramref name="repeatCount"/> times.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="repeatCount"/> is less than zero.</exception>
        public IObservable<T> Repeat(int repeatCount)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentOutOfRangeExceptionHelper.ThrowIfNegative(repeatCount);

            return repeatCount == 0 ? ImmutableEmptySignal<T>.Instance : new RepeatSourceSignal<T>(source, repeatCount);
        }
    }
}
