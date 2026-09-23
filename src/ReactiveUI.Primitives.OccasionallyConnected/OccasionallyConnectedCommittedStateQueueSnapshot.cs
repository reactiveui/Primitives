// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes a committed local state paired with the durable pending queue snapshot observed after the same mutation.</summary>
/// <typeparam name="TState">The local state type.</typeparam>
/// <param name="State">The committed local state.</param>
/// <param name="Pending">The pending queue summary captured after the same mutation committed.</param>
internal sealed record OccasionallyConnectedCommittedStateQueueSnapshot<TState>(TState State, PendingSyncSummary Pending);
