// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides Rx-compatible catch names for asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Catch operators for an observable source sequence.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>
        /// Creates a new observable sequence that continues with a handler-provided sequence when an exception occurs
        /// in the source sequence.
        /// </summary>
        /// <param name="handler">A function that receives the exception thrown by the source sequence and returns an alternative observable
        /// sequence to continue with.</param>
        /// <returns>An observable sequence that emits items from the source sequence, or from the handler-provided sequence if
        /// an exception is encountered.</returns>
        /// <exception cref="ArgumentNullException">Thrown if the source sequence or <paramref name="handler"/> is null.</exception>
        public IObservableAsync<T> Catch(Func<Exception, IObservableAsync<T>> handler)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(handler);

            var catchHandler = handler;
            return new CatchSignal<T>(source, catchHandler, null);
        }

        /// <summary>
        /// Creates a new observable sequence that continues with a handler-provided sequence when an exception occurs
        /// in the source sequence.
        /// </summary>
        /// <remarks>Use this method to recover from errors in the source sequence by switching to an
        /// alternative observable sequence. The handler function is called with the exception, allowing custom error
        /// recovery logic. If the handler itself throws an exception, the resulting sequence completes with that
        /// exception.</remarks>
        /// <param name="handler">A function that receives the exception thrown by the source sequence and returns an alternative observable
        /// sequence to continue with.</param>
        /// <param name="onErrorResume">An optional asynchronous callback invoked when an error occurs. If not specified, the observer's default
        /// error handler is used.</param>
        /// <returns>An observable sequence that emits items from the source sequence, or from the handler-provided sequence if
        /// an exception is encountered.</returns>
        /// <exception cref="ArgumentNullException">Thrown if the source sequence or <paramref name="handler"/> is null.</exception>
        public IObservableAsync<T> Catch(
            Func<Exception, IObservableAsync<T>> handler,
            Func<Exception, CancellationToken, ValueTask>? onErrorResume)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(handler);

            return new CatchSignal<T>(source, handler, onErrorResume);
        }
    }
}
