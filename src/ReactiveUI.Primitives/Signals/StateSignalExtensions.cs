// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Signals;

/// <summary>State projection helpers.</summary>
public static class StateSignalExtensions
{
    /// <summary>State projection operators for an observable source sequence.</summary>
    /// <param name="source">The source sequence.</param>
    /// <typeparam name="TSource">The source value type.</typeparam>
    extension<TSource>(IObservable<TSource> source)
    {
        /// <summary>Projects an observable sequence into a read-only state signal.</summary>
        /// <typeparam name="TResult">The projected value type.</typeparam>
        /// <param name="initialValue">The initial projected value.</param>
        /// <param name="selector">The projection function.</param>
        /// <returns>A read-only projected state.</returns>
        public ReadOnlyState<TResult> ToReadOnlyState<TResult>(
            TResult initialValue,
            Func<TSource, TResult> selector)
        {
            if (source is null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            if (selector is null)
            {
                throw new ArgumentNullException(nameof(selector));
            }

            return new(
                Signal.CreateSafe<TResult>(
                    observer => source.Subscribe(
                        value => observer.OnNext(selector(value)),
                        observer.OnError,
                        observer.OnCompleted)),
                initialValue);
        }
    }
}
