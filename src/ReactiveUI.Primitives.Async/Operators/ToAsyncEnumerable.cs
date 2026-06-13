// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides extension methods for working with asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Asynchronous-enumerable conversion operators for an observable source sequence.</summary>
    /// <param name="this">The asynchronous observable to convert into an asynchronous enumerable.</param>
    /// <typeparam name="T">The type of elements contained within the asynchronous observable and the resulting
    /// asynchronous enumerable.</typeparam>
    extension<T>(IObservableAsync<T> @this)
    {
        /// <summary>
        /// Converts the specified asynchronous observable sequence to an asynchronous enumerable sequence, enabling
        /// consumption using asynchronous iteration.
        /// </summary>
        /// <remarks>
        /// The resulting asynchronous enumerable sequence reflects the items and completion behavior of the source
        /// asynchronous observable. The buffering behavior is determined by the channel created by the provided
        /// <paramref name="channelFactory"/>.
        /// </remarks>
        /// <param name="channelFactory">A factory function that produces a channel to buffer elements, controlling
        /// the buffering and backpressure behavior between the asynchronous observable and the asynchronous enumerable.</param>
        /// <returns>An asynchronous enumerable sequence that yields elements from the asynchronous observable. The
        /// enumeration completes when the source observable completes, or an unhandled error occurs.</returns>
        /// <exception cref="ArgumentExceptionHelper">Thrown when <paramref name="this"/> or <paramref name="channelFactory"/>
        /// is null.</exception>
        public IAsyncEnumerable<T> ToAsyncEnumerable(
            Func<Channel<T>> channelFactory)
            => @this.ToAsyncEnumerable(channelFactory, null);

        /// <summary>
        /// Converts the specified observable sequence to an asynchronous enumerable sequence, enabling consumption using
        /// asynchronous iteration.
        /// </summary>
        /// <remarks>The returned asynchronous enumerable reflects the items and completion behavior of the source
        /// observable. The buffering and concurrency characteristics depend on the channel created by <paramref
        /// name="channelFactory"/>. If <paramref name="onErrorResume"/> is provided, it can be used to suppress or handle
        /// errors from the observable; otherwise, errors are propagated to the enumerator.</remarks>
        /// <param name="channelFactory">A factory function that creates a new channel used to buffer items between the observable and the asynchronous
        /// enumerable. The channel controls the buffering and backpressure behavior.</param>
        /// <param name="onErrorResume">An optional asynchronous callback invoked when an error occurs in the observable sequence. If provided, this
        /// function can handle the exception and determine how the sequence should resume or complete. If null, the
        /// sequence completes with the error.</param>
        /// <returns>An asynchronous enumerable sequence that yields the elements produced by the observable sequence. The
        /// enumeration completes when the observable completes or an unhandled error occurs.</returns>
        /// <exception cref="ArgumentExceptionHelper">Thrown if <paramref name="this"/> or <paramref name="channelFactory"/> is null.</exception>
        public IAsyncEnumerable<T> ToAsyncEnumerable(
            Func<Channel<T>> channelFactory,
            Func<Exception, CancellationToken, ValueTask>? onErrorResume)
        {
            ArgumentExceptionHelper.ThrowIfNull(@this);
            ArgumentExceptionHelper.ThrowIfNull(channelFactory);

            return ReadObservableValuesAsync(@this, channelFactory, onErrorResume);

            static async IAsyncEnumerable<T> ReadObservableValuesAsync(
                IObservableAsync<T> @this,
                Func<Channel<T>> channelFactory,
                Func<Exception, CancellationToken, ValueTask>? onErrorResume,
                [EnumeratorCancellation] CancellationToken cancellationToken = default)
            {
                var channel = channelFactory();
                var onErrorResumeAsync = onErrorResume ?? ((e, _) =>
                {
                    channel.Writer.Complete(e);
                    return default;
                });

                await using var subscription = await @this.SubscribeAsync(
                    channel.Writer.WriteAsync,
                    onErrorResumeAsync,
                    result =>
                    {
                        channel.Writer.Complete(result.Exception);
                        return default;
                    },
                    cancellationToken).ConfigureAwait(false);

                await foreach (var x in channel.Reader.ReadAllAsync(cancellationToken))
                {
                    yield return x;
                }
            }
        }
    }
}
