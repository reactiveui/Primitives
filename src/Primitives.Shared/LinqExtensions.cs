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
    /// <summary>Disposal operators for a disposable.</summary>
    /// <param name="disposable">The disposable.</param>
    extension(IDisposable disposable)
    {
        /// <summary>Wraps the disposable so that disposing the wrapper disposes it exactly once.</summary>
        /// <returns>A <see cref="SingleDisposable"/> that owns this disposable.</returns>
        public SingleDisposable DisposeWith() =>
            new(disposable);

        /// <summary>Wraps the disposable and runs an action just before it is disposed.</summary>
        /// <param name="action">The action to run on disposal, or <see langword="null"/> to run nothing extra.</param>
        /// <returns>A <see cref="SingleDisposable"/> that owns this disposable and first invokes <paramref name="action"/>.</returns>
        public SingleDisposable DisposeWith(Action? action) =>
            new(disposable, action);
    }

    /// <summary>Buffering operators for an observable source sequence.</summary>
    /// <typeparam name="TSource">The type of the source.</typeparam>
    /// <param name="source">The source.</param>
    extension<TSource>(IObservable<TSource> source)
    {
        /// <summary>Groups the source values into consecutive, non-overlapping buffers of a fixed size.</summary>
        /// <param name="count">The number of values in each buffer.</param>
        /// <returns>An observable sequence of buffers; the final buffer is shorter when the source ends mid-window.</returns>
        /// <exception cref="ArgumentExceptionHelper"><paramref name="source"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentOutOfRangeExceptionHelper"><paramref name="count"/> is zero or negative.</exception>
        public IObservable<IList<TSource>> Buffer(int count)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(count);

            return new BufferCountSignal<TSource>(source, count, 0);
        }

        /// <summary>Groups values into fixed-size buffers, opening a new buffer every <paramref name="skip"/> values.</summary>
        /// <param name="count">The number of values in each buffer.</param>
        /// <param name="skip">The number of values between the starts of consecutive buffers; a value below <paramref name="count"/> makes buffers overlap.</param>
        /// <returns>An observable sequence of buffers, each opened <paramref name="skip"/> values after the one before it.</returns>
        /// <exception cref="ArgumentExceptionHelper"><paramref name="source"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentOutOfRangeExceptionHelper"><paramref name="count"/> or <paramref name="skip"/> is zero or negative.</exception>
        public IObservable<IList<TSource>> Buffer(int count, int skip)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(count);

            ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(skip);

            return new BufferCountSignal<TSource>(source, count, skip);
        }
    }

    /// <summary>Disposal-tracking operators for a disposable.</summary>
    /// <typeparam name="T">The disposable type.</typeparam>
    /// <param name="disposable">The disposable.</param>
    extension<T>(T disposable)
        where T : IDisposable
    {
        /// <summary>Adds the disposable to a composite that will dispose it.</summary>
        /// <param name="disposables">The composite taking ownership of the disposable.</param>
        /// <returns>The same disposable, so the call can be chained onto its creation.</returns>
        public T DisposeWith(MultipleDisposable disposables)
        {
            ArgumentExceptionHelper.ThrowIfNull(disposables);

            disposables.Add(disposable);
            return disposable;
        }
    }
}
