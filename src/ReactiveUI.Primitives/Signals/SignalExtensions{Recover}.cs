// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Signals.Core;

namespace ReactiveUI.Primitives.Signals;

/// <summary>Error-recovery extension operators.</summary>
public static partial class SignalExtensions
{
    /// <summary>Error-handling operators for a sequence of observable source sequences.</summary>
    /// <param name="sources">Observable sequences to catch exceptions for.</param>
    /// <typeparam name="TSource">The value type.</typeparam>
    extension<TSource>(IEnumerable<IObservable<TSource>> sources)
    {
        /// <summary>Continues an observable sequence that is terminated by an exception with the next observable sequence.</summary>
        /// <returns>An observable sequence containing elements from consecutive source sequences until a source sequence terminates successfully.</returns>
        /// <exception cref="ArgumentExceptionHelper"><paramref name="sources"/> is null.</exception>
        public IObservable<TSource> Recover()
        {
            ArgumentExceptionHelper.ThrowIfNull(sources);

            return new CatchSignal<TSource>(sources);
        }
    }

    /// <summary>Error-handling and cleanup operators for an observable source sequence.</summary>
    /// <param name="source">Source sequence to recover or clean up.</param>
    /// <typeparam name="TSource">The value type.</typeparam>
    extension<TSource>(IObservable<TSource> source)
    {
        /// <summary>
        /// Continues an observable sequence that is terminated by an exception of the specified type with the observable sequence produced by the handler.
        /// </summary>
        /// <typeparam name="TException">The type of the exception to catch and handle. Needs to derive from <see cref="Exception"/>.</typeparam>
        /// <param name="handler">Exception handler function, producing another observable sequence.</param>
        /// <returns>
        /// An observable sequence containing the source sequence's elements, followed by the handler sequence's elements when an exception occurs.
        /// </returns>
        /// <exception cref="ArgumentExceptionHelper"><paramref name="source"/> or <paramref name="handler"/> is null.</exception>
        public IObservable<TSource> Recover<TException>(Func<TException, IObservable<TSource>> handler)
            where TException : Exception
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(handler);

            return new RecoverSignal<TSource, TException>(source, handler);
        }

        /// <summary>Finallies the specified finally action.</summary>
        /// <param name="finallyAction">The finally action.</param>
        /// <returns>An observable sequence containing elements from consecutive source sequences until a source sequence terminates successfully.</returns>
        public IObservable<TSource> OnCleanup(Action finallyAction) =>
            new FinallySignal<TSource>(source, finallyAction);
    }
}
