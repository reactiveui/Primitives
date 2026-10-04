// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Provides decoded remote messages after they have been committed and deduplicated locally.</summary>
/// <typeparam name="T">The decoded remote message value type.</typeparam>
public interface IRemoteObservable<T>
{
    /// <summary>Subscribes to decoded remote messages that were committed after deduplication.</summary>
    /// <param name="options">The logical remote subscription options.</param>
    /// <returns>An observable sequence of committed, deduplicated remote messages.</returns>
    IObservable<RemoteMessage<T>> SubscribeRemote(RemoteSubscriptionOptions options);
}
