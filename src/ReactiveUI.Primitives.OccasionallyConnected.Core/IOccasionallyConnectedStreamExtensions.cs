// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Convenience overloads for <see cref="IOccasionallyConnectedStream{TState, TInput}"/>.</summary>
public static class IOccasionallyConnectedStreamExtensions
{
    /// <summary>Convenience overloads for a local-first stream.</summary>
    /// <typeparam name="TState">The local state type.</typeparam>
    /// <typeparam name="TInput">The input value type.</typeparam>
    /// <param name="stream">The local-first stream.</param>
    extension<TState, TInput>(IOccasionallyConnectedStream<TState, TInput> stream)
    {
        /// <summary>Publishes a value without options or a cancellation token.</summary>
        /// <param name="value">The value to publish.</param>
        /// <returns>The local publish receipt.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<PublishReceipt> PublishAsync(TInput value) =>
            stream.PublishAsync(value, null, CancellationToken.None);

        /// <summary>Publishes a value with explicit options and no cancellation token.</summary>
        /// <param name="value">The value to publish.</param>
        /// <param name="options">The publish options.</param>
        /// <returns>The local publish receipt.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<PublishReceipt> PublishAsync(TInput value, RemotePublishOptions? options) =>
            stream.PublishAsync(value, options, CancellationToken.None);

        /// <summary>Publishes a value with a cancellation token and no options.</summary>
        /// <param name="value">The value to publish.</param>
        /// <param name="cancellationToken">The token used to cancel admission and persistence.</param>
        /// <returns>The local publish receipt.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<PublishReceipt> PublishAsync(TInput value, CancellationToken cancellationToken) =>
            stream.PublishAsync(value, null, cancellationToken);

        /// <summary>Starts stream synchronization work without a cancellation token.</summary>
        /// <returns>A task representing the asynchronous operation.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask StartAsync() => stream.StartAsync(CancellationToken.None);

        /// <summary>Stops stream synchronization work without a cancellation token.</summary>
        /// <returns>A task representing the asynchronous operation.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask StopAsync() => stream.StopAsync(CancellationToken.None);
    }
}
