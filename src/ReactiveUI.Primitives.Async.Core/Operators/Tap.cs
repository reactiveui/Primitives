// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides extension methods for working with asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Side-effect (Tap/Do) operators that invoke callbacks for each notification of an observable source sequence.</summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Invokes asynchronous side effects while preserving the source values.</summary>
        /// <param name="onNext">The asynchronous action invoked for each value.</param>
        /// <param name="onErrorResume">The asynchronous action invoked on a resumable error.</param>
        /// <param name="onCompleted">The asynchronous action invoked on completion.</param>
        /// <returns>An observable sequence identical to the source.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> Tap(
            Func<T, CancellationToken, ValueTask>? onNext,
            Func<Exception, CancellationToken, ValueTask>? onErrorResume,
            Func<Result, ValueTask>? onCompleted) =>
            TapAsyncSignal<T>.Create(source, onNext, onErrorResume, onCompleted);

        /// <summary>Invokes an action for each value while preserving the source values.</summary>
        /// <param name="onNext">The action invoked for each value.</param>
        /// <returns>An observable sequence identical to the source.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> Tap(Action<T> onNext) =>
            TapSyncSignal<T>.Create(source, onNext);

        /// <summary>Invokes side effects while preserving the source values.</summary>
        /// <param name="onNext">The action invoked for each value.</param>
        /// <param name="onError">The action invoked on an error.</param>
        /// <param name="onCompleted">The action invoked on completion.</param>
        /// <returns>An observable sequence identical to the source.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> Tap(
            Action<T> onNext,
            Action<Exception> onError,
            Action onCompleted) =>
            TapSyncSignal<T>.Create(source, onNext, onError, onCompleted);

        /// <summary>Invokes the specified asynchronous actions for each element, error, or completion notification in the observable sequence without modifying the sequence.</summary>
        /// <param name="onNext">An asynchronous callback to invoke for each element in the sequence.</param>
        /// <param name="onErrorResume">An optional asynchronous callback to invoke if an error occurs in the sequence.</param>
        /// <param name="onCompleted">An optional asynchronous callback to invoke when the sequence completes.</param>
        /// <returns>An observable sequence that is identical to the source sequence but invokes the specified callbacks.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> Do(
            Func<T, CancellationToken, ValueTask>? onNext,
            Func<Exception, CancellationToken, ValueTask>? onErrorResume,
            Func<Result, ValueTask>? onCompleted) =>
            TapAsyncSignal<T>.Create(source, onNext, onErrorResume, onCompleted);

        /// <summary>Invokes the specified asynchronous action for each element in the observable sequence without modifying the sequence.</summary>
        /// <param name="onNext">An asynchronous callback to invoke for each element in the sequence.</param>
        /// <returns>An observable sequence that is identical to the source sequence but invokes the specified callback.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> Do(Func<T, CancellationToken, ValueTask>? onNext) =>
            TapAsyncSignal<T>.Create(source, onNext);

        /// <summary>Invokes the specified actions in response to notifications from the observable sequence without modifying the sequence itself.</summary>
        /// <param name="onNext">An action to invoke for each element in the sequence as it is emitted.</param>
        /// <param name="onErrorResume">An action to invoke if an error occurs in the sequence.</param>
        /// <param name="onCompleted">An action to invoke when the sequence completes, receiving the final result.</param>
        /// <returns>An observable sequence that is identical to the source sequence but invokes the specified actions.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> Do(
            Action<T>? onNext,
            Action<Exception>? onErrorResume,
            Action<Result>? onCompleted) =>
            TapSyncSignal<T>.Create(source, onNext, onErrorResume, onCompleted);

        /// <summary>Returns an observable sequence that is identical to the source sequence and performs no side effects.</summary>
        /// <returns>An observable sequence that is identical to the source sequence.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> Do() =>
            TapSyncSignal<T>.Identity(source);
    }
}
