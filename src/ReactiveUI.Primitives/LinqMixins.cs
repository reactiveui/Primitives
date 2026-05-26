// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives;

/// <summary>
/// SelectMixins.
/// </summary>
public static partial class LinqMixins
{
    /// <summary>
    /// Selects the specified selector.
    /// </summary>
    /// <typeparam name="TSource">The type of the source.</typeparam>
    /// <typeparam name="TResult">The type of the result.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="selector">The selector.</param>
    /// <returns>A ISignals.</returns>
    /// <exception cref="System.ArgumentNullException">
    /// source
    /// or
    /// selector.
    /// </exception>
    public static IObservable<TResult> Select<TSource, TResult>(this IObservable<TSource> source, Func<TSource, TResult> selector)
        => (source ?? throw new ArgumentNullException(nameof(source))).Map(selector ?? throw new ArgumentNullException(nameof(selector)));

    /// <summary>
    /// Buffers the specified count.
    /// </summary>
    /// <typeparam name="TSource">The type of the source.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="count">The count of each buffer.</param>
    /// <returns>An Signals sequence of buffers.</returns>
    /// <exception cref="System.ArgumentNullException">source.</exception>
    /// <exception cref="System.ArgumentOutOfRangeException">count.</exception>
    public static IObservable<IList<TSource>> Buffer<TSource>(this IObservable<TSource> source, int count)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (count <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(count));
        }

        return new BufferSignal<TSource, IList<TSource>>(source, count, 0);
    }

    /// <summary>
    /// Buffers the specified count then skips the specified count, then repeats.
    /// </summary>
    /// <typeparam name="TSource">The type of the source.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="count">Length of each buffer before being skipped.</param>
    /// <param name="skip">Number of elements to skip between creation of consecutive buffers.</param>
    /// <returns>An Signals sequence of buffers taking the count then skipping the skipped value, the sequecnce is then repeated.</returns>
    /// <exception cref="System.ArgumentNullException">source.</exception>
    /// <exception cref="System.ArgumentOutOfRangeException">
    /// count
    /// or
    /// skip.
    /// </exception>
    public static IObservable<IList<TSource>> Buffer<TSource>(this IObservable<TSource> source, int count, int skip)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (count <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(count));
        }

        if (skip <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(skip));
        }

        return new BufferSignal<TSource, IList<TSource>>(source, count, skip);
    }

    /// <summary>
    /// Disposes the IDisposable with the disposables instance.
    /// </summary>
    /// <param name="disposable">The disposable.</param>
    /// <param name="disposables">The disposables.</param>
    /// <returns>An IDisposable.</returns>
    public static IDisposable DisposeWith(this IDisposable disposable, MultipleDisposable disposables)
    {
        disposables?.Add(disposable);
        return disposable;
    }

    /// <summary>
    /// Disposes the with.
    /// </summary>
    /// <param name="disposable">The disposable.</param>
    /// <returns>A SingleDisposable.</returns>
    public static SingleDisposable DisposeWith(this IDisposable disposable) =>
        new(disposable);

    /// <summary>
    /// Disposes the with.
    /// </summary>
    /// <param name="disposable">The disposable.</param>
    /// <param name="action">The action.</param>
    /// <returns>A SingleDisposable.</returns>
    public static SingleDisposable DisposeWith(this IDisposable disposable, Action? action) =>
        new(disposable, action);

    /// <summary>
    /// Wheres the specified predicate.
    /// </summary>
    /// <typeparam name="T">The Type.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="predicate">The predicate.</param>
    /// <returns>An ISignals.</returns>
    /// <exception cref="System.ArgumentNullException">
    /// source
    /// or
    /// predicate.
    /// </exception>
    public static IObservable<T> Where<T>(this IObservable<T> source, Func<T, bool> predicate)
        => (source ?? throw new ArgumentNullException(nameof(source))).Keep(predicate ?? throw new ArgumentNullException(nameof(predicate)));
}
