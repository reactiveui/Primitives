// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides Rx-compatible bind names for asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Bind operators for an observable source sequence.</summary>
    /// <param name="this">The source observable sequence.</param>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    extension<T>(IObservableAsync<T> @this)
    {
        /// <summary>Projects and merges inner async observable sequences.</summary>
        /// <typeparam name="TResult">The result element type.</typeparam>
        /// <param name="selector">The projection producing an inner sequence for each value.</param>
        /// <returns>An observable sequence of merged inner values.</returns>
        public IObservableAsync<TResult> Bind<TResult>(Func<T, IObservableAsync<TResult>> selector)
        {
            ArgumentExceptionHelper.ThrowIfNull(selector);

            var bindSelector = selector;
            return new FlatMapSignal<T, TResult>(@this, bindSelector);
        }
    }
}
