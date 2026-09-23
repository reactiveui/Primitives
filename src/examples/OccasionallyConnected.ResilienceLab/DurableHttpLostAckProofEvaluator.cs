// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

/// <summary>Evaluates observed durable client state without advancing either client's state machine.</summary>
internal static class DurableHttpLostAckProofEvaluator
{
    /// <summary>Determines whether a durable proof is still pending and retryable.</summary>
    /// <param name="proof">The recovered store proof.</param>
    /// <returns>Whether the operation can be retried with its original identity.</returns>
    internal static bool IsPendingRetryable(ClientStoreProof proof) =>
        proof.PendingCount == 1
        && proof.PendingOperationId.HasValue
        && proof.RetryState is { DueUtc: not null }
        && proof.OperationStatus is
        {
            Attempt: > 0,
            State: SyncOperationState.SavedLocally or SyncOperationState.QueuedForUpload or SyncOperationState.Uploading,
        };

    /// <summary>Compares the active pending proof with fresh SQLite proof after client closure.</summary>
    /// <param name="beforeClose">The active-store proof.</param>
    /// <param name="afterClose">The reopened-store proof.</param>
    /// <returns>Whether the pending identity, retry deadline, and subscription persisted.</returns>
    internal static bool PendingSurvivesClose(ClientStoreProof beforeClose, ClientStoreProof afterClose) =>
        IsPendingRetryable(beforeClose)
        && IsPendingRetryable(afterClose)
        && HasSamePendingIdentity(beforeClose, afterClose)
        && HasSameRetryState(beforeClose, afterClose);

    /// <summary>Compares operation, sequence, and subscription identity across closure.</summary>
    /// <param name="beforeClose">The active-store proof.</param>
    /// <param name="afterClose">The reopened-store proof.</param>
    /// <returns>Whether the three identities match.</returns>
    internal static bool HasSamePendingIdentity(ClientStoreProof beforeClose, ClientStoreProof afterClose) =>
        beforeClose.PendingOperationId == afterClose.PendingOperationId
        && beforeClose.PendingClientSequence == afterClose.PendingClientSequence
        && beforeClose.SubscriptionId == afterClose.SubscriptionId;

    /// <summary>Compares upload state, attempt, and retry deadline across closure.</summary>
    /// <param name="beforeClose">The active-store proof.</param>
    /// <param name="afterClose">The reopened-store proof.</param>
    /// <returns>Whether the retry state matches.</returns>
    internal static bool HasSameRetryState(ClientStoreProof beforeClose, ClientStoreProof afterClose) =>
        beforeClose.OperationStatus?.State == afterClose.OperationStatus?.State
        && beforeClose.OperationStatus?.Attempt == afterClose.OperationStatus?.Attempt
        && beforeClose.RetryState?.DueUtc == afterClose.RetryState?.DueUtc;

    /// <summary>Determines whether the final cursor advanced from the before-restart proof.</summary>
    /// <param name="beforeRestart">The before-restart proof.</param>
    /// <param name="finalStore">The final proof.</param>
    /// <returns>Whether a distinct nonempty cursor was persisted.</returns>
    internal static bool CursorAdvanced(ClientStoreProof beforeRestart, ClientStoreProof finalStore) =>
        !string.IsNullOrWhiteSpace(finalStore.ServerCursor)
        && !string.Equals(beforeRestart.ServerCursor, finalStore.ServerCursor, StringComparison.Ordinal);

    /// <summary>Determines whether both clients restored the authoritative counter snapshot.</summary>
    /// <param name="writerFinal">The final writer proof.</param>
    /// <param name="observerFinal">The final observer proof.</param>
    /// <returns>Whether each snapshot contains the single durable effect.</returns>
    internal static bool SnapshotRestored(ClientStoreProof writerFinal, ClientStoreProof observerFinal) =>
        writerFinal.SnapshotCounter == 1 && observerFinal.SnapshotCounter == 1;

    /// <summary>Formats an optional operation identifier.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <returns>The stable identifier or an empty string.</returns>
    internal static string Format(OperationId? operationId) => operationId.HasValue ? Format(operationId.Value) : string.Empty;

    /// <summary>Formats an operation identifier.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <returns>The identifier in D format.</returns>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    internal static string Format(OperationId operationId) => operationId.Value.ToString("D");

    /// <summary>Formats the non-pending durable state for diagnostics.</summary>
    /// <param name="proof">The store proof.</param>
    /// <returns>The operation state or missing marker.</returns>
    internal static string FormatPendingState(ClientStoreProof proof) =>
        proof.OperationStatus is null ? "missing" : proof.OperationStatus.State.ToString();

    /// <summary>Formats cursor progress for diagnostics.</summary>
    /// <param name="beforeRestart">The before-restart proof.</param>
    /// <param name="finalStore">The final proof.</param>
    /// <returns>The cursor comparison.</returns>
    internal static string FormatCursorProgress(ClientStoreProof beforeRestart, ClientStoreProof finalStore) =>
        $"before={beforeRestart.ServerCursor ?? "null"};after={finalStore.ServerCursor ?? "null"}";

    /// <summary>Formats snapshot state for diagnostics.</summary>
    /// <param name="writerFinal">The final writer proof.</param>
    /// <param name="observerFinal">The final observer proof.</param>
    /// <returns>The two snapshot values.</returns>
    internal static string FormatSnapshotProof(ClientStoreProof writerFinal, ClientStoreProof observerFinal) =>
        $"writer={FormatCounter(writerFinal.SnapshotCounter)};observer={FormatCounter(observerFinal.SnapshotCounter)}";

    /// <summary>Formats an optional counter.</summary>
    /// <param name="counter">The counter.</param>
    /// <returns>The numeric counter or missing marker.</returns>
    internal static string FormatCounter(long? counter) =>
        counter.HasValue ? Convert.ToString(counter.Value, CultureInfo.InvariantCulture) : "missing";
}
