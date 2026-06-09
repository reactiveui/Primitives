// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Internals;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides extension methods for working with asynchronous observable sequences.</summary>
/// <remarks>The methods in this class enable the addition of side effects, such as logging or resource
/// management, to asynchronous observable sequences without modifying their elements or control flow. These methods are
/// intended to be used as part of a fluent query or processing pipeline for asynchronous observables.</remarks>
public static partial class SignalAsyncExtensions
{
    /// <summary>Side-effect operators that invoke callbacks for each notification of an observable source sequence.</summary>
    /// <param name="this">The source observable sequence.</param>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    extension<T>(IObservableAsync<T> @this)
    {
        /// <summary>
        /// Invokes the specified asynchronous actions for each element, error, or completion notification in the
        /// observable sequence without modifying the sequence.
        /// </summary>
        /// <remarks>Use this method to perform side effects such as logging, resource cleanup, or
        /// notification in response to elements, errors, or completion events in the sequence. The callbacks are
        /// invoked asynchronously and do not alter the elements or flow of the sequence.</remarks>
        /// <param name="onNext">An asynchronous callback to invoke for each element in the sequence. Receives the element and a cancellation
        /// token. If null, no action is taken on elements.</param>
        /// <param name="onErrorResume">An optional asynchronous callback to invoke if an error occurs in the sequence. Receives the exception and a
        /// cancellation token. If null, errors are not handled by this observer.</param>
        /// <param name="onCompleted">An optional asynchronous callback to invoke when the sequence completes. Receives the result of the
        /// sequence. If null, no action is taken on completion.</param>
        /// <returns>An observable sequence that is identical to the source sequence but invokes the specified callbacks for side
        /// effects.</returns>
        public IObservableAsync<T> Do(
            Func<T, CancellationToken, ValueTask>? onNext,
            Func<Exception, CancellationToken, ValueTask>? onErrorResume,
            Func<Result, ValueTask>? onCompleted) =>
            new DoAsyncSignal<T>(@this, onNext, onErrorResume, onCompleted);

        /// <summary>Invokes the specified asynchronous action for each element in the observable sequence without modifying the sequence.</summary>
        /// <param name="onNext">An asynchronous callback to invoke for each element in the sequence. Receives the element and a cancellation
        /// token. If null, no action is taken on elements.</param>
        /// <returns>An observable sequence that is identical to the source sequence but invokes the specified callback for side
        /// effects.</returns>
        public IObservableAsync<T> Do(Func<T, CancellationToken, ValueTask>? onNext) =>
            new DoAsyncSignal<T>(@this, onNext, null, null);

        /// <summary>
        /// Invokes the specified actions in response to notifications from the observable sequence without modifying
        /// the sequence itself.
        /// </summary>
        /// <remarks>Use this method to perform side effects such as logging, monitoring, or debugging in
        /// response to sequence events without altering the sequence's behavior. The returned observable passes through
        /// all elements and notifications unchanged.</remarks>
        /// <param name="onNext">An action to invoke for each element in the sequence as it is emitted. If null, no action is taken on
        /// element emission.</param>
        /// <param name="onErrorResume">An action to invoke if an error occurs in the sequence. Receives the exception that caused the error. If
        /// null, no action is taken on error.</param>
        /// <param name="onCompleted">An action to invoke when the sequence completes, receiving the final result. If null, no action is taken on
        /// completion.</param>
        /// <returns>An observable sequence that is identical to the source sequence but invokes the specified actions for each
        /// notification.</returns>
        public IObservableAsync<T> Do(
            Action<T>? onNext,
            Action<Exception>? onErrorResume,
            Action<Result>? onCompleted) => new DoSyncSignal<T>(@this, onNext, onErrorResume, onCompleted);

        /// <summary>Returns an observable sequence that is identical to the source sequence and performs no side effects.</summary>
        /// <returns>An observable sequence that is identical to the source sequence.</returns>
        public IObservableAsync<T> Do() =>
            new DoSyncSignal<T>(@this, null, null, null);
    }

    /// <summary>An observable that invokes asynchronous side-effect callbacks for each notification.</summary>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="onNext">An asynchronous callback to invoke for each element, or null to take no action on elements.</param>
    /// <param name="onErrorResume">An asynchronous callback to invoke on error, or null to take no action on errors.</param>
    /// <param name="onCompleted">An asynchronous callback to invoke on completion, or null to take no action on completion.</param>
    internal sealed class DoAsyncSignal<T>(
        IObservableAsync<T> source,
        Func<T, CancellationToken, ValueTask>? onNext,
        Func<Exception, CancellationToken, ValueTask>? onErrorResume,
        Func<Result, ValueTask>? onCompleted) : SignalAsync<T>
    {
        /// <inheritdoc/>
        protected override ValueTask<IAsyncDisposable> SubscribeAsyncCore(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            var doObserver = new AsyncSideEffectWitness(observer, onNext, onErrorResume, onCompleted);
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
    internal sealed class DoSyncSignal<T>(
        IObservableAsync<T> source,
        Action<T>? onNext,
        Action<Exception>? onErrorResume,
        Action<Result>? onCompleted) : SignalAsync<T>
    {
        /// <inheritdoc/>
        protected override ValueTask<IAsyncDisposable> SubscribeAsyncCore(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            var doObserver = new SyncSideEffectWitness(observer, onNext, onErrorResume, onCompleted);
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
