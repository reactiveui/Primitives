// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Async.Disposables;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides extension methods for composing and handling asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Error-handling operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Creates a new observable sequence that continues with a handler-provided sequence when an exception occurs in the source sequence.</summary>
        /// <param name="handler">A function that receives the exception thrown by the source sequence and returns an alternative observable
        /// sequence to continue with.</param>
        /// <returns>An observable sequence that emits items from the source sequence, or from the handler-provided sequence if
        /// an exception is encountered.</returns>
        /// <exception cref="ArgumentNullException">Thrown if the source sequence or <paramref name="handler"/> is null.</exception>
        /// <remarks>If the handler throws, the resulting sequence completes with that exception.</remarks>
        public IObservableAsync<T> Recover(Func<Exception, IObservableAsync<T>> handler)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(handler);

            return new CatchSignal<T>(source, handler, null);
        }

        /// <summary>Resumes with a fallback sequence after a terminal failure.</summary>
        /// <param name="fallback">The fallback sequence used after a failure.</param>
        /// <returns>An observable sequence that resumes with the fallback on failure.</returns>
        public IObservableAsync<T> Resume(IObservableAsync<T> fallback)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new CatchSignal<T>(source, _ => fallback, null);
        }

        /// <summary>Continues the observable sequence with an alternative sequence provided by the specified handler when an error occurs, and ignores the error after invoking the handler.</summary>
        /// <param name="handler">A function that receives the exception and returns an alternative observable sequence to resume with after
        /// an error occurs.</param>
        /// <returns>An observable sequence that resumes with the sequence returned by the handler when an error is encountered,
        /// and ignores the error after handling.</returns>
        /// <remarks>An error-resume notification is reported to the global unhandled-exception handler rather than
        /// forwarded downstream, so subscribers never observe it.</remarks>
        public IObservableAsync<T> CatchAndIgnoreErrorResume(Func<Exception, IObservableAsync<T>> handler)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(handler);

            return new CatchSignal<T>(
                source,
                handler,
                static (error, _) =>
                {
                    UnhandledExceptionHandler.ReportUnhandledException(error);
                    return default;
                });
        }
    }

    /// <summary>Subscribes the handler-produced fallback observable when the source completes with a failure.</summary>
    /// <typeparam name="T">The element type of the source sequence.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="handler">The fallback handler invoked with the source exception when the source completes with a failure.</param>
    /// <param name="onErrorResume">Optional asynchronous error-resume callback. When <see langword="null"/>, error-resume notifications are
    /// forwarded straight to the downstream observer.</param>
    internal sealed class CatchSignal<T>(
        IObservableAsync<T> source,
        Func<Exception, IObservableAsync<T>> handler,
        Func<Exception, CancellationToken, ValueTask>? onErrorResume) : IObservableAsync<T>
    {
        /// <inheritdoc/>
        async ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            CatchWitness sink = new(observer, handler, onErrorResume, cancellationToken);

            if (observer is IWitnessAsync<T> downstreamBase)
            {
                downstreamBase.LinkUpstreamCancellation(sink.InternalDisposedToken);
            }

            var subscription = await source.SubscribeAsync(sink, cancellationToken).ConfigureAwait(false);
            await sink.AssignSourceSubscriptionAsync(subscription).ConfigureAwait(false);
            return sink;
        }

        /// <summary>Forwards values, routes error-resume to the callback or downstream, and swaps in the fallback on failure.</summary>
        /// <param name="downstream">The downstream witness.</param>
        /// <param name="handler">The fallback factory.</param>
        /// <param name="onErrorResume">Optional async error-resume callback.</param>
        /// <param name="subscribeToken">The subscribe-time cancellation token, linked into the dispose chain and reused for the handler
        /// subscription.</param>
        [DebuggerDisplay("CatchWitness: {_witness}")]
        internal sealed class CatchWitness(
            IObserverAsync<T> downstream,
            Func<Exception, IObservableAsync<T>> handler,
            Func<Exception, CancellationToken, ValueTask>? onErrorResume,
            CancellationToken subscribeToken) : IWitnessAsync<T>
        {
            /// <summary>Holds the handler-produced subscription, assigned at most once, so it disposes with the sink.</summary>
            private readonly SingleAssignmentDisposableAsync _handlerDisposable = new();

            /// <summary>The subscribe-time token, reused when subscribing the fallback handler observable.</summary>
            private readonly CancellationToken _subscribeToken = subscribeToken;

            /// <summary>The notification gate, cancellation link and disposal state.</summary>
            private WitnessAsyncState _witness = new(subscribeToken);

            /// <inheritdoc/>
            ref WitnessAsyncState IWitnessState.Witness => ref _witness;

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask OnNextAsync(T value, CancellationToken cancellationToken) =>
                WitnessAsync.OnNextAsync(this, value, cancellationToken);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken) =>
                WitnessAsync.OnErrorResumeAsync(this, error, cancellationToken);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask OnCompletedAsync(Result result) => WitnessAsync.OnCompletedAsync(this, result);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ValueTask IWitnessAsync<T>.OnNextAsyncCore(T value, CancellationToken cancellationToken) =>
                downstream.OnNextAsync(value, cancellationToken);

            /// <inheritdoc/>
            ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
                onErrorResume is null
                    ? downstream.OnErrorResumeAsync(error, cancellationToken)
                    : onErrorResume(error, cancellationToken);

            /// <inheritdoc/>
            async ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result)
            {
                if (result.IsSuccess)
                {
                    await downstream.OnCompletedAsync(result).ConfigureAwait(false);
                    return;
                }

                try
                {
                    var handlerObservable = handler(result.Exception);
                    var handlerSubscription =
                        await handlerObservable.SubscribeAsync(downstream.Wrap(), _subscribeToken)
                            .ConfigureAwait(false);
                    await _handlerDisposable.SetDisposableAsync(handlerSubscription).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    await downstream.OnCompletedAsync(Result.Failure(e)).ConfigureAwait(false);
                }
            }

            /// <inheritdoc/>
            public async ValueTask DisposeAsync()
            {
                try
                {
                    await _handlerDisposable.DisposeAsync().ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    UnhandledExceptionHandler.ReportUnhandledException(e);
                }

                await WitnessAsync.DisposeStateAsync(this).ConfigureAwait(false);
            }
        }
    }
}
