// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Disposables;
using ReactiveUI.Primitives.Internal;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides extension methods for composing and handling asynchronous observable sequences.</summary>
/// <remarks>The methods in this class enable advanced error handling and composition scenarios for asynchronous
/// observables. These extensions are intended to be used with the SignalAsync{T} type to facilitate robust,
/// composable, and resilient asynchronous data streams.</remarks>
public static partial class SignalAsyncExtensions
{
    /// <summary>Error-handling operators for an observable source sequence.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    extension<T>(IObservableAsync<T> source)
    {
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
        /// <returns>An observable sequence that emits items from the source sequence, or from the handler-provided sequence if
        /// an exception is encountered.</returns>
        /// <exception cref="ArgumentNullException">Thrown if the source sequence or <paramref name="handler"/> is null.</exception>
        public IObservableAsync<T> Catch(Func<Exception, IObservableAsync<T>> handler)
            => source.Catch(handler, null);

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

        /// <summary>
        /// Continues the observable sequence with an alternative sequence provided by the specified handler when an
        /// error occurs, and ignores the error after invoking the handler.
        /// </summary>
        /// <remarks>If an error occurs and the handler is invoked, the error is also reported to the
        /// global unhandled exception handler before being ignored. This method allows the sequence to continue without
        /// propagating the error to subscribers.</remarks>
        /// <param name="handler">A function that receives the exception and returns an alternative observable sequence to resume with after
        /// an error occurs.</param>
        /// <returns>An observable sequence that resumes with the sequence returned by the handler when an error is encountered,
        /// and ignores the error after handling.</returns>
        public IObservableAsync<T> CatchAndIgnoreErrorResume(Func<Exception, IObservableAsync<T>> handler) =>
            source.Catch(handler, static (error, _) =>
            {
                UnhandledExceptionHandler.ReportUnhandledException(error);
                return default;
            });
    }

    /// <summary>
    /// Observable wrapper for <see cref="Catch{T}(IObservableAsync{T}, Func{Exception,IObservableAsync{T}}, Func{Exception,CancellationToken,ValueTask}?)"/>.
    /// Allocates one observable wrapper and one sealed observer per subscription — no per-emission closure or
    /// state-machine box from the previous <c>Create&lt;T&gt;((observer, token) =&gt; ...)</c> pattern.
    /// </summary>
    /// <typeparam name="T">The element type of the source sequence.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="handler">The fallback handler invoked with the source exception when the source completes with a failure.</param>
    /// <param name="onErrorResume">Optional asynchronous error-resume callback. When <see langword="null"/>, error-resume notifications are
    /// forwarded straight to the downstream observer.</param>
    internal sealed class CatchSignal<T>(
        IObservableAsync<T> source,
        Func<Exception, IObservableAsync<T>> handler,
        Func<Exception, CancellationToken, ValueTask>? onErrorResume) : SignalAsync<T>
    {
        /// <inheritdoc/>
        protected override async ValueTask<IAsyncDisposable> SubscribeAsyncCore(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            var sink = new CatchWitness(observer, handler, onErrorResume, cancellationToken);

            // Wire sink's dispose token into the downstream's link chain so the downstream's hot path
            // recognises this token without allocating a per-emission linked CTS.
            if (observer is ObserverAsync<T> downstreamBase)
            {
                downstreamBase.LinkUpstreamCancellation(sink.InternalDisposedToken);
            }

            var subscription = await source.SubscribeAsync(sink, cancellationToken).ConfigureAwait(false);
            await sink.AssignSourceSubscriptionAsync(subscription).ConfigureAwait(false);
            return sink;
        }

        /// <summary>Per-subscription witness that forwards <c>OnNext</c> verbatim, delegates error-resume to the
        /// supplied callback (or the downstream when none was supplied), and on a failed completion subscribes the
        /// handler-produced fallback observable in place of forwarding the failure.</summary>
        /// <param name="downstream">The downstream witness.</param>
        /// <param name="handler">The fallback factory.</param>
        /// <param name="onErrorResume">Optional async error-resume callback.</param>
        /// <param name="subscribeToken">The subscribe-time cancellation token, linked into the dispose chain and reused for the handler
        /// subscription.</param>
        internal sealed class CatchWitness(
            IObserverAsync<T> downstream,
            Func<Exception, IObservableAsync<T>> handler,
            Func<Exception, CancellationToken, ValueTask>? onErrorResume,
            CancellationToken subscribeToken) : ObserverAsync<T>(subscribeToken)
        {
            /// <summary>Holds the handler-produced subscription so it disposes with the sink. Single-assignment
            /// because the handler is subscribed at most once (on a failed source completion).</summary>
            private readonly SingleAssignmentDisposableAsync _handlerDisposable = new();

            /// <summary>The subscribe-time token, reused when subscribing the fallback handler observable.</summary>
            private readonly CancellationToken _subscribeToken = subscribeToken;

            /// <inheritdoc/>
            protected override ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken) =>
                downstream.OnNextAsync(value, cancellationToken);

            /// <inheritdoc/>
            protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
                onErrorResume is null
                    ? downstream.OnErrorResumeAsync(error, cancellationToken)
                    : onErrorResume(error, cancellationToken);

            /// <inheritdoc/>
            protected override async ValueTask OnCompletedAsyncCore(Result result)
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
                        await handlerObservable.SubscribeAsync(downstream.Wrap(), _subscribeToken).ConfigureAwait(false);
                    await _handlerDisposable.SetDisposableAsync(handlerSubscription).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    await downstream.OnCompletedAsync(Result.Failure(e)).ConfigureAwait(false);
                }
            }

            /// <inheritdoc/>
            protected override async ValueTask DisposeAsyncCore()
            {
                try
                {
                    await _handlerDisposable.DisposeAsync().ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    UnhandledExceptionHandler.ReportUnhandledException(e);
                }

                await base.DisposeAsyncCore().ConfigureAwait(false);
            }
        }
    }
}
