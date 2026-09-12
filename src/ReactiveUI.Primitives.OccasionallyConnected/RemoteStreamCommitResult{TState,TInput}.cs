// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes a committed remote batch and the state it produced.</summary>
/// <typeparam name="TState">The projected local state type.</typeparam>
/// <typeparam name="TInput">The remote input type.</typeparam>
/// <param name="Receipt">The durable remote apply receipt.</param>
/// <param name="Batch">The filtered batch committed to the local store.</param>
/// <param name="Inputs">The decoded immutable inputs used for projection.</param>
/// <param name="State">The current state after the remote apply.</param>
internal sealed record RemoteStreamCommitResult<TState, TInput>(
    RemoteApplyResult Receipt,
    RemoteEventBatch Batch,
    IReadOnlyList<TInput> Inputs,
    LocalStreamCommitterState<TState> State);
