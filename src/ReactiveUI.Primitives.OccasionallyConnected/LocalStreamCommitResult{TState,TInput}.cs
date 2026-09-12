// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes a committed local operation and the state it produced.</summary>
/// <typeparam name="TState">The projected local state type.</typeparam>
/// <typeparam name="TInput">The local input type.</typeparam>
/// <param name="Receipt">The durable publish receipt.</param>
/// <param name="Operation">The committed operation envelope.</param>
/// <param name="Input">The decoded immutable input used for projection.</param>
/// <param name="State">The current state after the commit.</param>
internal sealed record LocalStreamCommitResult<TState, TInput>(
    PublishReceipt Receipt,
    SyncOperation Operation,
    TInput Input,
    LocalStreamCommitterState<TState> State);
