// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Snapshot recovery result fixtures for public projection controls.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>Creates a recovered checkpoint with one terminal rejection.</summary>
    /// <param name="operationId">The rejected operation.</param>
    /// <param name="reasonCode">The durable result reason.</param>
    /// <returns>The remote recovery response.</returns>
    private static async ValueTask<RemoteSnapshotRecoveryResult> CreateRejectedSnapshotRecoveryResultAsync(
        OperationId operationId,
        string reasonCode)
    {
        var payload = await new ReceiveCounterSerializer()
            .SerializeAsync(SnapshotRecoveryCounterStateContractId, ExpectedSingleOperation, new ReceiveCounterState(ExpectedSingleOperation), CancellationToken.None)
            .ConfigureAwait(false);
        return new()
        {
            Status = RemoteSnapshotRecoveryStatus.Recovered,
            Checkpoint = CreateRecoveredCheckpoint(payload),
            OperationDispositions =
            [
                new() { OperationId = operationId, Kind = SnapshotOperationDispositionKind.TerminalRejected, Result = new(operationId, OperationResultKind.Rejected, reasonCode, ServerVersion: null) },
            ],
        };
    }
}
