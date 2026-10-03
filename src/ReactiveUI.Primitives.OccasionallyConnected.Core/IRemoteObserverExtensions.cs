// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Convenience overloads for <see cref="IRemoteObserver{T}"/>.</summary>
public static class IRemoteObserverExtensions
{
    /// <summary>Convenience overloads for a remote observer.</summary>
    /// <typeparam name="T">The published value type.</typeparam>
    /// <param name="observer">The remote observer.</param>
    extension<T>(IRemoteObserver<T> observer)
    {
        /// <summary>Publishes a value according to its delivery policy without a cancellation token.</summary>
        /// <param name="value">The value to publish.</param>
        /// <param name="options">The publish options.</param>
        /// <returns>The local publish receipt.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<PublishReceipt> PublishAsync(T value, RemotePublishOptions options) =>
            observer.PublishAsync(value, options, CancellationToken.None);

        /// <summary>Creates a bounded synchronous producer bridge with default admission options.</summary>
        /// <param name="options">The publish options used for values accepted by the bridge.</param>
        /// <returns>An observer that admits values to this remote observer.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObserver<T> AsObserver(RemotePublishOptions options) =>
            observer.AsObserver(options, null);
    }
}
