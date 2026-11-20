// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives;

/// <summary>Provides subscription extension methods for observables.</summary>
public static class SubscribeExtensions
{
    /// <summary>Exception helpers for a nullable exception receiver.</summary>
    /// <param name="exception">The receiver exception, which may be <see langword="null"/>.</param>
    extension(Exception? exception)
    {
        /// <summary>Rethrows the exception with its original stack trace, doing nothing when there is none.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Rethrow() => Witness.RethrowIfNotNull(exception);
    }

    /// <summary>Subscription operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <param name="source">Signals sequence to subscribe to.</param>
    extension<T>(IObservable<T> source)
    {
        /// <summary>Subscribes without any handlers, so the source sequence runs for its side effects alone.</summary>
        /// <returns>A handle that unsubscribes from the source sequence when disposed.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <c>null</c>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IDisposable Subscribe() => Witness.Subscribe(source);

        /// <summary>Subscribes a value callback; a terminal error is rethrown to the producer and completion is ignored.</summary>
        /// <param name="onNext">The callback invoked for each value.</param>
        /// <returns>A handle that unsubscribes from the source sequence when disposed.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IDisposable Subscribe(Action<T> onNext) => Witness.Subscribe(source, onNext);

        /// <summary>Subscribes value and error callbacks; completion is ignored.</summary>
        /// <param name="onNext">The callback invoked for each value.</param>
        /// <param name="onError">The callback invoked with the terminal error.</param>
        /// <returns>A handle that unsubscribes from the source sequence when disposed.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IDisposable Subscribe(Action<T> onNext, Action<Exception> onError) =>
            Witness.Subscribe(source, onNext, onError);

        /// <summary>Subscribes value and completion callbacks; a terminal error is rethrown to the producer.</summary>
        /// <param name="onNext">The callback invoked for each value.</param>
        /// <param name="onCompleted">The callback invoked when the sequence completes.</param>
        /// <returns>A handle that unsubscribes from the source sequence when disposed.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IDisposable Subscribe(Action<T> onNext, Action onCompleted) =>
            Witness.Subscribe(source, onNext, onCompleted);

        /// <summary>Subscribes value, error, and completion callbacks.</summary>
        /// <param name="onNext">The callback invoked for each value.</param>
        /// <param name="onError">The callback invoked with the terminal error.</param>
        /// <param name="onCompleted">The callback invoked when the sequence completes.</param>
        /// <returns>A handle that unsubscribes from the source sequence when disposed.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IDisposable Subscribe(Action<T> onNext, Action<Exception> onError, Action onCompleted) =>
            Witness.Subscribe(source, onNext, onError, onCompleted);

        /// <summary>Subscribes without any handlers, under the Primitives-specific name.</summary>
        /// <returns>A handle that unsubscribes from the source sequence when disposed.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <c>null</c>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IDisposable SubscribePrimitives() => Witness.Subscribe(source);

        /// <summary>Subscribes a value callback, under the Primitives-specific name.</summary>
        /// <param name="onNext">The callback invoked for each value.</param>
        /// <returns>A handle that unsubscribes from the source sequence when disposed.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IDisposable SubscribePrimitives(Action<T> onNext) => Witness.Subscribe(source, onNext);

        /// <summary>Subscribes value and error callbacks, under the Primitives-specific name.</summary>
        /// <param name="onNext">The callback invoked for each value.</param>
        /// <param name="onError">The callback invoked with the terminal error.</param>
        /// <returns>A handle that unsubscribes from the source sequence when disposed.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IDisposable SubscribePrimitives(Action<T> onNext, Action<Exception> onError) =>
            Witness.Subscribe(source, onNext, onError);

        /// <summary>Subscribes value and completion callbacks, under the Primitives-specific name.</summary>
        /// <param name="onNext">The callback invoked for each value.</param>
        /// <param name="onCompleted">The callback invoked when the sequence completes.</param>
        /// <returns>A handle that unsubscribes from the source sequence when disposed.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IDisposable SubscribePrimitives(Action<T> onNext, Action onCompleted) =>
            Witness.Subscribe(source, onNext, onCompleted);

        /// <summary>Subscribes value, error, and completion callbacks, under the Primitives-specific name.</summary>
        /// <param name="onNext">The callback invoked for each value.</param>
        /// <param name="onError">The callback invoked with the terminal error.</param>
        /// <param name="onCompleted">The callback invoked when the sequence completes.</param>
        /// <returns>A handle that unsubscribes from the source sequence when disposed.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IDisposable SubscribePrimitives(Action<T> onNext, Action<Exception> onError, Action onCompleted) =>
            Witness.Subscribe(source, onNext, onError, onCompleted);
    }
}
