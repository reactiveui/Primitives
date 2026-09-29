// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.OccasionallyConnected.Reactive;
#else
namespace ReactiveUI.Primitives.OccasionallyConnected;
#endif

/// <summary>Describes a committed stream snapshot recovery transition.</summary>
/// <typeparam name="TState">The projected local state type.</typeparam>
/// <param name="State">The committed stream state.</param>
/// <param name="Acknowledgement">The remote acknowledgement for the recovered cursor.</param>
/// <param name="QueueSnapshot">The queue aggregate after recovery.</param>
internal sealed record SnapshotRecoveryStreamCommitResult<TState>(
    LocalStreamCommitterState<TState> State,
    ReceiveAcknowledgement Acknowledgement,
    QueueDiagnosticSnapshot QueueSnapshot);
