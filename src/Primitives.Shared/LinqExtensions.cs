// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive;
#else
namespace ReactiveUI.Primitives;
#endif

/// <summary>Miscellaneous Primitives extensions.</summary>
public static partial class LinqExtensions
{
    /// <summary>Disposal-tracking operators for a disposable.</summary>
    /// <param name="disposable">The disposable.</param>
    extension(IDisposable disposable)
    {
        /// <summary>Disposes the IDisposable with the disposables instance.</summary>
        /// <param name="disposables">The disposables.</param>
        /// <returns>An IDisposable.</returns>
        public IDisposable DisposeWith(MultipleDisposable disposables)
        {
            disposables?.Add(disposable);
            return disposable;
        }

        /// <summary>Disposes the with.</summary>
        /// <returns>A SingleDisposable.</returns>
        public SingleDisposable DisposeWith() =>
            new(disposable);

        /// <summary>Disposes the with.</summary>
        /// <param name="action">The action.</param>
        /// <returns>A SingleDisposable.</returns>
        public SingleDisposable DisposeWith(Action? action) =>
            new(disposable, action);
    }

    /// <summary>Buffering operators for an observable source sequence.</summary>
    /// <typeparam name="TSource">The type of the source.</typeparam>
    /// <param name="source">The source.</param>
    extension<TSource>(IObservable<TSource> source)
    {
        /// <summary>Buffers the specified count.</summary>
        /// <param name="count">The count of each buffer.</param>
        /// <returns>An Signals sequence of buffers.</returns>
        /// <exception cref="ArgumentExceptionHelper">source.</exception>
        /// <exception cref="ArgumentOutOfRangeExceptionHelper">count.</exception>
        public IObservable<IList<TSource>> Buffer(int count)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(count);

            return new BufferCountSignal<TSource>(source, count, 0);
        }

        /// <summary>Buffers the specified count then skips the specified count, then repeats.</summary>
        /// <param name="count">Length of each buffer before being skipped.</param>
        /// <param name="skip">Number of elements to skip between creation of consecutive buffers.</param>
        /// <returns>An Signals sequence of buffers taking the count then skipping the skipped value, the sequecnce is then repeated.</returns>
        /// <exception cref="ArgumentExceptionHelper">source.</exception>
        /// <exception cref="ArgumentOutOfRangeExceptionHelper">
        /// count
        /// or
        /// skip.
        /// </exception>
        public IObservable<IList<TSource>> Buffer(int count, int skip)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(count);

            ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(skip);

            return new BufferCountSignal<TSource>(source, count, skip);
        }
    }
}
