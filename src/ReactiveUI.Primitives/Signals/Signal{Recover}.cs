// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Signals.Core;

namespace ReactiveUI.Primitives.Signals;

/// <summary>
/// Signals.
/// </summary>
public static partial class Signal
{
    /// <summary>
    /// Continues an observable sequence that is terminated by an exception of the specified type with the observable sequence produced by the handler.
    /// </summary>
    /// <typeparam name="TSource">The type of the elements in the source sequence and sequences returned by the exception handler function.</typeparam>
    /// <typeparam name="TException">The type of the exception to catch and handle. Needs to derive from <see cref="Exception"/>.</typeparam>
    /// <param name="source">Source sequence.</param>
    /// <param name="handler">Exception handler function, producing another observable sequence.</param>
    /// <returns>
    /// An observable sequence containing the source sequence's elements, followed by the handler sequence's elements when an exception occurs.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="handler"/> is null.</exception>
    public static IObservable<TSource> Recover<TSource, TException>(this IObservable<TSource> source, Func<TException, IObservable<TSource>> handler)
        where TException : Exception
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (handler == null)
        {
            throw new ArgumentNullException(nameof(handler));
        }

        return new RecoverSignal<TSource, TException>(source, handler);
    }

    /// <summary>
    /// Continues an observable sequence that is terminated by an exception with the next observable sequence.
    /// </summary>
    /// <typeparam name="TSource">The type of the elements in the source and handler sequences.</typeparam>
    /// <param name="sources">Observable sequences to catch exceptions for.</param>
    /// <returns>An observable sequence containing elements from consecutive source sequences until a source sequence terminates successfully.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="sources"/> is null.</exception>
    public static IObservable<TSource> Recover<TSource>(params IObservable<TSource>[] sources)
    {
        if (sources == null)
        {
            throw new ArgumentNullException(nameof(sources));
        }

        return new CatchSignal<TSource>(sources);
    }

    /// <summary>
    /// Continues an observable sequence that is terminated by an exception with the next observable sequence.
    /// </summary>
    /// <typeparam name="TSource">The type of the elements in the source and handler sequences.</typeparam>
    /// <param name="sources">Observable sequences to catch exceptions for.</param>
    /// <returns>An observable sequence containing elements from consecutive source sequences until a source sequence terminates successfully.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="sources"/> is null.</exception>
    public static IObservable<TSource> Recover<TSource>(this IEnumerable<IObservable<TSource>> sources)
    {
        if (sources == null)
        {
            throw new ArgumentNullException(nameof(sources));
        }

        return new CatchSignal<TSource>(sources);
    }

    /// <summary>
    /// Finallies the specified finally action.
    /// </summary>
    /// <typeparam name="T">The type of the elements in the source and handler sequences.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="finallyAction">The finally action.</param>
    /// <returns>An observable sequence containing elements from consecutive source sequences until a source sequence terminates successfully.</returns>
    public static IObservable<T> OnCleanup<T>(this IObservable<T> source, Action finallyAction) =>
        new FinallySignal<T>(source, finallyAction);
}
