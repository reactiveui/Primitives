// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Exposes committed local state paired with the durable pending queue snapshot from the same mutation boundary.</summary>
/// <typeparam name="TState">The local state type.</typeparam>
internal interface IOccasionallyConnectedCommittedStateQueueSnapshots<TState>
{
    /// <summary>Gets paired committed-state and pending-queue snapshots.</summary>
    IObservable<OccasionallyConnectedCommittedStateQueueSnapshot<TState>> CommittedStateQueueSnapshots { get; }
}
