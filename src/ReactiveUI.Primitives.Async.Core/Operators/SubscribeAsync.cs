// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides extension methods for subscribing to asynchronous observable sequences using various delegate-based overloads.</summary>
/// <remarks>The methods in this class enable consumers to subscribe to an asynchronous observable sequence by
/// specifying delegate handlers for item notifications, error handling, and completion. These overloads offer both
/// synchronous and asynchronous delegate options, allowing for flexible integration with different programming models.
/// All subscriptions return an <see cref="IAsyncDisposable"/> that should be disposed to terminate the subscription and
/// release resources. These methods are intended to simplify the process of observing asynchronous streams without
/// requiring explicit implementation of observer interfaces.</remarks>
public static partial class SignalAsyncExtensions
{
    /// <summary>Delegate-based subscription operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>
        /// Subscribes to the asynchronous data source and invokes the specified callbacks for each item, error, or
        /// completion notification.
        /// </summary>
        /// <param name="onNextAsync">A delegate that is invoked asynchronously for each item received from the data source. The delegate receives
        /// the item and a cancellation token.</param>
        /// <param name="onErrorResumeAsync">An optional delegate that is invoked asynchronously if an error occurs during data processing. The delegate
        /// receives the exception and a cancellation token. If null, errors are not handled by the subscriber.</param>
        /// <param name="onCompletedAsync">An optional delegate that is invoked asynchronously when the data source completes successfully. The
        /// delegate receives a result indicating the completion status. If null, no action is taken on completion.</param>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the subscription and any in-progress callbacks.</param>
        /// <returns>A value task that represents the asynchronous operation. The result is an <see cref="IAsyncDisposable"/>
        /// that can be disposed to unsubscribe from the data source.</returns>
        /// <exception cref="ArgumentExceptionHelper">Thrown if the underlying data source is null.</exception>
        /// <remarks>The returned <see cref="IAsyncDisposable"/> should be disposed when the subscription
        /// is no longer needed to release resources and stop receiving notifications. Callbacks may be invoked
        /// concurrently; implement thread safety in the provided delegates if required.</remarks>
        public ValueTask<IAsyncDisposable> SubscribeAsync(
            Func<T, CancellationToken, ValueTask> onNextAsync,
            Func<Exception, CancellationToken, ValueTask>? onErrorResumeAsync,
            Func<Result, ValueTask>? onCompletedAsync,
            CancellationToken cancellationToken)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(onNextAsync);

            CallbackWitnessAsync<T> observer = new(onNextAsync, onErrorResumeAsync, onCompletedAsync);
            return source.SubscribeAsync(observer, cancellationToken);
        }

        /// <summary>Subscribes to the asynchronous data source and invokes the specified callbacks for each item or error notification.</summary>
        /// <param name="onNextAsync">A delegate that is invoked asynchronously for each item received from the data source.</param>
        /// <param name="onErrorResumeAsync">An optional delegate that is invoked asynchronously if an error occurs during data processing.</param>
        /// <returns>A value task that represents the asynchronous operation. The result is an <see cref="IAsyncDisposable"/>
        /// that can be disposed to unsubscribe from the data source.</returns>
        /// <exception cref="ArgumentExceptionHelper">Thrown if the underlying data source is null.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<IAsyncDisposable> SubscribeAsync(
            Func<T, CancellationToken, ValueTask> onNextAsync,
            Func<Exception, CancellationToken, ValueTask>? onErrorResumeAsync) =>
            source.SubscribeAsync(onNextAsync, onErrorResumeAsync, null, CancellationToken.None);

        /// <summary>
        /// Subscribes to the asynchronous data source and invokes the specified callbacks for each item, error, or
        /// completion notification.
        /// </summary>
        /// <param name="onNextAsync">A delegate that is invoked asynchronously for each item received from the data source.</param>
        /// <param name="onErrorResumeAsync">An optional delegate that is invoked asynchronously if an error occurs during data processing.</param>
        /// <param name="onCompletedAsync">An optional delegate that is invoked asynchronously when the data source completes successfully.</param>
        /// <returns>A value task that represents the asynchronous operation. The result is an <see cref="IAsyncDisposable"/>
        /// that can be disposed to unsubscribe from the data source.</returns>
        /// <exception cref="ArgumentExceptionHelper">Thrown if the underlying data source is null.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<IAsyncDisposable> SubscribeAsync(
            Func<T, CancellationToken, ValueTask> onNextAsync,
            Func<Exception, CancellationToken, ValueTask>? onErrorResumeAsync,
            Func<Result, ValueTask>? onCompletedAsync) =>
            source.SubscribeAsync(onNextAsync, onErrorResumeAsync, onCompletedAsync, CancellationToken.None);

        /// <summary>Subscribes to the observable sequence and invokes the specified action for each element received.</summary>
        /// <param name="onNext">An action to invoke for each element in the sequence. Cannot be null.</param>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the subscription operation.</param>
        /// <returns>A value task that represents the asynchronous subscription operation. The result contains an <see
        /// cref="IAsyncDisposable"/> that can be disposed to unsubscribe from the sequence.</returns>
        /// <exception cref="ArgumentExceptionHelper">Thrown if <paramref name="onNext"/> is null.</exception>
        public ValueTask<IAsyncDisposable> SubscribeAsync(
            Action<T> onNext,
            CancellationToken cancellationToken)
        {
            ArgumentExceptionHelper.ThrowIfNull(onNext);

            CallbackWitnessAsync<T> observer = new((x, _) =>
            {
                onNext(x);
                return default;
            });

            return source.SubscribeAsync(observer, cancellationToken);
        }

        /// <summary>Subscribes to the observable sequence and invokes the specified action for each element received.</summary>
        /// <param name="onNext">An action to invoke for each element in the sequence. Cannot be null.</param>
        /// <returns>A value task that represents the asynchronous subscription operation. The result contains an <see
        /// cref="IAsyncDisposable"/> that can be disposed to unsubscribe from the sequence.</returns>
        /// <exception cref="ArgumentExceptionHelper">Thrown if <paramref name="onNext"/> is null.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<IAsyncDisposable> SubscribeAsync(Action<T> onNext) =>
            source.SubscribeAsync(onNext, CancellationToken.None);

        /// <summary>
        /// Subscribes to the observable sequence asynchronously, invoking the specified callbacks for each element,
        /// error, or completion notification.
        /// </summary>
        /// <param name="onNext">An action to invoke for each element in the sequence. Cannot be null.</param>
        /// <param name="onErrorResume">An optional action to invoke if an error occurs during the sequence. If null, errors are not handled by the
        /// subscriber.</param>
        /// <param name="onCompleted">An optional action to invoke when the sequence completes. If null, completion is not handled by the
        /// subscriber.</param>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the subscription.</param>
        /// <returns>A value task that represents the asynchronous subscription operation. The result is an <see
        /// cref="IAsyncDisposable"/> that can be disposed to unsubscribe from the sequence.</returns>
        /// <exception cref="ArgumentExceptionHelper">Thrown if <paramref name="onNext"/> is null, or if the underlying source is null.</exception>
        /// <remarks>The returned <see cref="IAsyncDisposable"/> should be disposed when the subscription
        /// is no longer needed to release resources and stop receiving notifications. This method enables asynchronous,
        /// push-based event handling for observable sequences.</remarks>
        public ValueTask<IAsyncDisposable> SubscribeAsync(
            Action<T> onNext,
            Action<Exception>? onErrorResume,
            Action<Result>? onCompleted,
            CancellationToken cancellationToken)
        {
            ArgumentExceptionHelper.ThrowIfNull(onNext);
            ArgumentExceptionHelper.ThrowIfNull(source);

            static ValueTask OnErrorResumeAsync(Exception e, Action<Exception> onErrorResume)
            {
                onErrorResume(e);
                return default;
            }

            static ValueTask OnCompletedAsync(Result x, Action<Result> onCompleted)
            {
                onCompleted(x);
                return default;
            }

            CallbackWitnessAsync<T> observer = new(
                (x, _) =>
                {
                    onNext(x);
                    return default;
                },
                onErrorResume is null ? null : (e, _) => OnErrorResumeAsync(e, onErrorResume),
                onCompleted is null ? null : x => OnCompletedAsync(x, onCompleted));

            return source.SubscribeAsync(observer, cancellationToken);
        }

        /// <summary>Subscribes to the source without handling any items asynchronously.</summary>
        /// <returns>A value task that represents the asynchronous subscription operation. The result is an <see
        /// cref="IAsyncDisposable"/> that can be disposed to unsubscribe.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<IAsyncDisposable> SubscribeAsync() =>
            source.SubscribeAsync(static (_, _) => default, CancellationToken.None);

        /// <summary>Subscribes asynchronously to receive notifications for each item published by the source.</summary>
        /// <param name="onNextAsync">A delegate that is invoked asynchronously for each item published. The delegate receives the item and a
        /// cancellation token, and returns a ValueTask that completes when processing is finished. Cannot be null.</param>
        /// <returns>A ValueTask that represents the asynchronous subscription operation. The result contains an IAsyncDisposable
        /// that can be disposed to unsubscribe from the source.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<IAsyncDisposable> SubscribeAsync(Func<T, CancellationToken, ValueTask> onNextAsync) =>
            source.SubscribeAsync(onNextAsync, CancellationToken.None);

        /// <summary>
        /// Subscribes asynchronously to receive notifications for each item in the sequence using the specified
        /// asynchronous callback.
        /// </summary>
        /// <param name="onNextAsync">A function to invoke asynchronously for each item in the sequence. The function receives the item and a
        /// cancellation token, and returns a ValueTask that completes when processing is finished.</param>
        /// <param name="cancellationToken">A token that can be used to cancel the subscription operation.</param>
        /// <returns>A ValueTask that represents the asynchronous subscription operation. The result is an IAsyncDisposable that
        /// can be disposed to unsubscribe from the sequence.</returns>
        /// <exception cref="ArgumentExceptionHelper">Thrown if the underlying source is null.</exception>
        public ValueTask<IAsyncDisposable> SubscribeAsync(
            Func<T, CancellationToken, ValueTask> onNextAsync,
            CancellationToken cancellationToken)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(onNextAsync);

            CallbackWitnessAsync<T> observer = new(onNextAsync);
            return source.SubscribeAsync(observer, cancellationToken);
        }
    }
}
