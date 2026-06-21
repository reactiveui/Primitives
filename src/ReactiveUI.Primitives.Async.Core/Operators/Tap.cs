// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides extension methods for working with asynchronous observable sequences.</summary>
/// <remarks>The methods in this class enable the addition of side effects, such as logging or resource
/// management, to asynchronous observable sequences without modifying their elements or control flow. These methods are
/// intended to be used as part of a fluent query or processing pipeline for asynchronous observables.</remarks>
public static partial class SignalAsyncExtensions
{
    /// <summary>Side-effect (Tap/Do) operators that invoke callbacks for each notification of an observable source sequence.</summary>
    /// <param name="this">The source observable sequence.</param>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    extension<T>(IObservableAsync<T> @this)
    {
        /// <summary>Invokes asynchronous side effects while preserving the source values.</summary>
        /// <param name="onNext">The asynchronous action invoked for each value.</param>
        /// <param name="onErrorResume">The asynchronous action invoked on a resumable error.</param>
        /// <param name="onCompleted">The asynchronous action invoked on completion.</param>
        /// <returns>An observable sequence identical to the source.</returns>
        public IObservableAsync<T> Tap(
            Func<T, CancellationToken, ValueTask>? onNext,
            Func<Exception, CancellationToken, ValueTask>? onErrorResume,
            Func<Result, ValueTask>? onCompleted) =>
            onNext is null && onErrorResume is null && onCompleted is null
                ? @this
                : new TapAsyncSignal<T>(@this, onNext, onErrorResume, onCompleted);

        /// <summary>Invokes an action for each value while preserving the source values.</summary>
        /// <param name="onNext">The action invoked for each value.</param>
        /// <returns>An observable sequence identical to the source.</returns>
        public IObservableAsync<T> Tap(Action<T> onNext) =>
            onNext is null ? @this : new TapSyncSignal<T>(@this, onNext, null, null);

        /// <summary>Invokes side effects while preserving the source values.</summary>
        /// <param name="onNext">The action invoked for each value.</param>
        /// <param name="onError">The action invoked on an error.</param>
        /// <param name="onCompleted">The action invoked on completion.</param>
        /// <returns>An observable sequence identical to the source.</returns>
        public IObservableAsync<T> Tap(
            Action<T> onNext,
            Action<Exception> onError,
            Action onCompleted) =>
            new TapSyncSignal<T>(@this, onNext, onError, _ => onCompleted());

        /// <summary>
        /// Invokes the specified asynchronous actions for each element, error, or completion notification in the
        /// observable sequence without modifying the sequence.
        /// </summary>
        /// <param name="onNext">An asynchronous callback to invoke for each element in the sequence.</param>
        /// <param name="onErrorResume">An optional asynchronous callback to invoke if an error occurs in the sequence.</param>
        /// <param name="onCompleted">An optional asynchronous callback to invoke when the sequence completes.</param>
        /// <returns>An observable sequence that is identical to the source sequence but invokes the specified callbacks.</returns>
        public IObservableAsync<T> Do(
            Func<T, CancellationToken, ValueTask>? onNext,
            Func<Exception, CancellationToken, ValueTask>? onErrorResume,
            Func<Result, ValueTask>? onCompleted) =>
            @this.Tap(onNext, onErrorResume, onCompleted);

        /// <summary>Invokes the specified asynchronous action for each element in the observable sequence without modifying the sequence.</summary>
        /// <param name="onNext">An asynchronous callback to invoke for each element in the sequence.</param>
        /// <returns>An observable sequence that is identical to the source sequence but invokes the specified callback.</returns>
        public IObservableAsync<T> Do(Func<T, CancellationToken, ValueTask>? onNext) =>
            @this.Tap(onNext, null, null);

        /// <summary>
        /// Invokes the specified actions in response to notifications from the observable sequence without modifying
        /// the sequence itself.
        /// </summary>
        /// <param name="onNext">An action to invoke for each element in the sequence as it is emitted.</param>
        /// <param name="onErrorResume">An action to invoke if an error occurs in the sequence.</param>
        /// <param name="onCompleted">An action to invoke when the sequence completes, receiving the final result.</param>
        /// <returns>An observable sequence that is identical to the source sequence but invokes the specified actions.</returns>
        public IObservableAsync<T> Do(
            Action<T>? onNext,
            Action<Exception>? onErrorResume,
            Action<Result>? onCompleted) =>
            onNext is null && onErrorResume is null && onCompleted is null
                ? @this
                : new TapSyncSignal<T>(@this, onNext, onErrorResume, onCompleted);

        /// <summary>Returns an observable sequence that is identical to the source sequence and performs no side effects.</summary>
        /// <returns>An observable sequence that is identical to the source sequence.</returns>
        public IObservableAsync<T> Do() =>
            @this;
    }

    /// <summary>An observable that invokes asynchronous side-effect callbacks for each notification.</summary>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="onNext">An asynchronous callback to invoke for each element, or null to take no action on elements.</param>
    /// <param name="onErrorResume">An asynchronous callback to invoke on error, or null to take no action on errors.</param>
    /// <param name="onCompleted">An asynchronous callback to invoke on completion, or null to take no action on completion.</param>
    internal sealed class TapAsyncSignal<T>(
        IObservableAsync<T> source,
        Func<T, CancellationToken, ValueTask>? onNext,
        Func<Exception, CancellationToken, ValueTask>? onErrorResume,
        Func<Result, ValueTask>? onCompleted) : IObservableAsync<T>
    {
        /// <inheritdoc/>
        ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            AsyncSideEffectWitness doObserver = new(observer, onNext, onErrorResume, onCompleted);
            return source.SubscribeAsync(doObserver, cancellationToken);
        }

        /// <summary>A witness that invokes asynchronous side-effect callbacks before forwarding notifications.</summary>
        /// <param name="observer">The downstream observer to forward notifications to.</param>
        /// <param name="onNext">An asynchronous callback to invoke for each element, or null to take no action on elements.</param>
        /// <param name="onErrorResume">An asynchronous callback to invoke on error, or null to take no action on errors.</param>
        /// <param name="onCompleted">An asynchronous callback to invoke on completion, or null to take no action on completion.</param>
        internal sealed class AsyncSideEffectWitness(
            IObserverAsync<T> observer,
            Func<T, CancellationToken, ValueTask>? onNext,
            Func<Exception, CancellationToken, ValueTask>? onErrorResume,
            Func<Result, ValueTask>? onCompleted) : ForwardingWitnessAsync<T>(observer)
        {
            /// <inheritdoc/>
            protected override async ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken)
            {
                if (onNext is not null)
                {
                    await onNext(value, cancellationToken).ConfigureAwait(false);
                }

                await Downstream.OnNextAsync(value, cancellationToken).ConfigureAwait(false);
            }

            /// <inheritdoc/>
            protected override async ValueTask OnErrorResumeAsyncCore(
                Exception error,
                CancellationToken cancellationToken)
            {
                if (onErrorResume is not null)
                {
                    await onErrorResume(error, cancellationToken).ConfigureAwait(false);
                }

                await Downstream.OnErrorResumeAsync(error, cancellationToken).ConfigureAwait(false);
            }

            /// <inheritdoc/>
            protected override async ValueTask OnCompletedAsyncCore(Result result)
            {
                if (onCompleted is not null)
                {
                    await onCompleted(result).ConfigureAwait(false);
                }

                await Downstream.OnCompletedAsync(result).ConfigureAwait(false);
            }
        }
    }

    /// <summary>An observable that invokes synchronous side-effect callbacks for each notification.</summary>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="onNext">An action to invoke for each element, or null to take no action on elements.</param>
    /// <param name="onErrorResume">An action to invoke on error, or null to take no action on errors.</param>
    /// <param name="onCompleted">An action to invoke on completion, or null to take no action on completion.</param>
    internal sealed class TapSyncSignal<T>(
        IObservableAsync<T> source,
        Action<T>? onNext,
        Action<Exception>? onErrorResume,
        Action<Result>? onCompleted) : IObservableAsync<T>
    {
        /// <inheritdoc/>
        ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            SyncSideEffectWitness doObserver = new(observer, onNext, onErrorResume, onCompleted);
            return source.SubscribeAsync(doObserver, cancellationToken);
        }

        /// <summary>An observer that invokes synchronous side-effect callbacks before forwarding notifications.</summary>
        /// <param name="observer">The downstream observer to forward notifications to.</param>
        /// <param name="onNext">An action to invoke for each element, or null to take no action on elements.</param>
        /// <param name="onErrorResume">An action to invoke on error, or null to take no action on errors.</param>
        /// <param name="onCompleted">An action to invoke on completion, or null to take no action on completion.</param>
        internal sealed class SyncSideEffectWitness(
            IObserverAsync<T> observer,
            Action<T>? onNext,
            Action<Exception>? onErrorResume,
            Action<Result>? onCompleted) : ForwardingWitnessAsync<T>(observer)
        {
            /// <inheritdoc/>
            protected override ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken)
            {
                onNext?.Invoke(value);
                return Downstream.OnNextAsync(value, cancellationToken);
            }

            /// <inheritdoc/>
            protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken)
            {
                onErrorResume?.Invoke(error);
                return Downstream.OnErrorResumeAsync(error, cancellationToken);
            }

            /// <inheritdoc/>
            protected override ValueTask OnCompletedAsyncCore(Result result)
            {
                onCompleted?.Invoke(result);
                return Downstream.OnCompletedAsync(result);
            }
        }
    }
}
