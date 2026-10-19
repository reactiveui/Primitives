// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Publishes values to a remote stream.</summary>
/// <typeparam name="T">The published value type.</typeparam>
public interface IRemoteObserver<T>
{
    /// <summary>Publishes a value according to its delivery policy and returns its local admission receipt.</summary>
    /// <param name="value">The value to publish.</param>
    /// <param name="options">The publish options.</param>
    /// <param name="cancellationToken">The cancellation token for admission and persistence.</param>
    /// <returns>The local publish receipt.</returns>
    ValueTask<PublishReceipt> PublishAsync(
        T value,
        RemotePublishOptions options,
        CancellationToken cancellationToken);

    /// <summary>Creates a bounded synchronous producer bridge.</summary>
    /// <param name="options">The publish options used for values accepted by the bridge.</param>
    /// <param name="inputOptions">The bounded admission options for the bridge.</param>
    /// <returns>An observer that admits values to this remote observer.</returns>
    /// <remarks>
    /// <para><see cref="IObserver{T}.OnNext(T)"/> performs bounded in-memory admission.</para>
    /// <para><see cref="IObserver{T}.OnError(Exception)"/> reports a producer fault, and <see cref="IObserver{T}.OnCompleted"/> closes only that producer.</para>
    /// <para>Callers that require a durable receipt must enable durable publishing and use <see cref="PublishAsync(T, RemotePublishOptions, CancellationToken)"/>.</para>
    /// </remarks>
    IObserver<T> AsObserver(RemotePublishOptions options, ObserverInputOptions? inputOptions);
}
