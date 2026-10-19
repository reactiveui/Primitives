// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using static ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.DurableHttpLostAckProofEvaluator;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

/// <summary>Runs the durable HTTP lost acknowledgement recovery scenario.</summary>
internal static partial class DurableHttpLostAckScenario
{
    /// <summary>Waits for a persisted writer proof from a supplied durable reader.</summary>
    /// <param name="readProof">The reader for the initialized SQLite store.</param>
    /// <param name="beforeRestart">The pre-restart persisted proof.</param>
    /// <param name="telemetry">The public stream telemetry.</param>
    /// <param name="cancellationToken">The scenario deadline token.</param>
    /// <returns>The synchronized durable writer proof.</returns>
    /// <exception cref="InvalidOperationException">The retry did not durably synchronize before the scenario deadline.</exception>
    internal static async ValueTask<ClientStoreProof> WaitForWriterDurableSynchronizationWithReaderAsync(
        Func<CancellationToken, ValueTask<ClientStoreProof>> readProof,
        ClientStoreProof beforeRestart,
        StreamTelemetry telemetry,
        CancellationToken cancellationToken)
    {
        ClientStoreProof? lastObserved = null;
        try
        {
            while (true)
            {
                var proof = await readProof(cancellationToken).ConfigureAwait(false);
                lastObserved = proof;
                if (IsDurablySynchronized(beforeRestart, proof))
                {
                    return proof;
                }

                await Task.Delay(TimeSpan.FromMilliseconds(ProofPollMilliseconds), cancellationToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException exception)
        {
            if (lastObserved is null)
            {
                throw new InvalidOperationException("Retry ended before a durable writer sample could be read.", exception);
            }

            throw new InvalidOperationException(
                $"Retry did not durably synchronize: status={lastObserved.OperationStatus?.State}, attempt={lastObserved.OperationStatus?.Attempt}, "
                + $"cursorAdvanced={CursorAdvanced(beforeRestart, lastObserved)}, cursor={lastObserved.ServerCursor}, "
                + $"snapshot={lastObserved.SnapshotCounter}, pendingCount={lastObserved.PendingCount}, pending={Format(lastObserved.PendingOperationId)}, "
                + $"retryDue={lastObserved.RetryState?.DueUtc:O}, faults={telemetry.FaultCodes}.",
                exception);
        }
    }
}
