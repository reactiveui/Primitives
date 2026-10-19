// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.Tests;

/// <summary>Checks durable proof predicates against immutable persisted-state samples.</summary>
public sealed class DurableHttpLostAckProofEvaluatorTests
{
    /// <summary>The expected single durable effect.</summary>
    private const int SingleEffect = 1;

    /// <summary>A changed client sequence or retry deadline.</summary>
    private const int ChangedSequence = 2;

    /// <summary>The cursor after a durable receive.</summary>
    private const string NextCursor = "cursor-next";

    /// <summary>The first client's current durable sample.</summary>
    private static readonly ClientStoreProof Ready = CreateReadyProof();

    /// <summary>Gets the required persisted operation status.</summary>
    private static SyncOperationStatus ReadyStatus => Ready.OperationStatus ?? throw new InvalidOperationException("The ready proof has no operation status.");

    /// <summary>Gets the required persisted retry state.</summary>
    private static RetryState ReadyRetryState => Ready.RetryState ?? throw new InvalidOperationException("The ready proof has no retry state.");

    /// <summary>Pending proof requires the original identity, retry deadline, and active upload state.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task PendingRetryabilityRejectsIncompleteOrTerminalState()
    {
        await Assert.That(DurableHttpLostAckProofEvaluator.IsPendingRetryable(Ready)).IsTrue();
        await Assert.That(DurableHttpLostAckProofEvaluator.IsPendingRetryable(Ready with { PendingCount = 0 })).IsFalse();
        await Assert.That(DurableHttpLostAckProofEvaluator.IsPendingRetryable(Ready with { PendingOperationId = null })).IsFalse();
        await Assert.That(DurableHttpLostAckProofEvaluator.IsPendingRetryable(Ready with { RetryState = null })).IsFalse();
        await Assert.That(DurableHttpLostAckProofEvaluator.IsPendingRetryable(Ready with { RetryState = RetryState.Start(DateTimeOffset.UnixEpoch) })).IsFalse();
        await Assert.That(DurableHttpLostAckProofEvaluator.IsPendingRetryable(Ready with { OperationStatus = null })).IsFalse();
        await Assert.That(DurableHttpLostAckProofEvaluator.IsPendingRetryable(Ready with { OperationStatus = ReadyStatus with { Attempt = 0 } })).IsFalse();
        await Assert.That(DurableHttpLostAckProofEvaluator.IsPendingRetryable(Ready with { OperationStatus = ReadyStatus with { State = SyncOperationState.Synchronized } })).IsFalse();
        await Assert.That(DurableHttpLostAckProofEvaluator.IsPendingRetryable(Ready with { OperationStatus = ReadyStatus with { State = SyncOperationState.QueuedForUpload } })).IsTrue();
        await Assert.That(DurableHttpLostAckProofEvaluator.IsPendingRetryable(Ready with { OperationStatus = ReadyStatus with { State = SyncOperationState.Uploading } })).IsTrue();
    }

    /// <summary>A restarted client must retain the same pending operation, sequence, subscription, and retry state.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task PendingSurvivalDetectsEachPersistedMismatch()
    {
        await Assert.That(DurableHttpLostAckProofEvaluator.PendingSurvivesClose(Ready, Ready)).IsTrue();
        await Assert.That(DurableHttpLostAckProofEvaluator.PendingSurvivesClose(Ready with { PendingCount = 0 }, Ready)).IsFalse();
        await Assert.That(DurableHttpLostAckProofEvaluator.PendingSurvivesClose(Ready, Ready with { PendingCount = 0 })).IsFalse();
        await Assert.That(DurableHttpLostAckProofEvaluator.PendingSurvivesClose(Ready, Ready with { PendingOperationId = OperationId.New() })).IsFalse();
        await Assert.That(DurableHttpLostAckProofEvaluator.PendingSurvivesClose(Ready, Ready with { PendingClientSequence = ChangedSequence })).IsFalse();
        await Assert.That(DurableHttpLostAckProofEvaluator.PendingSurvivesClose(Ready, Ready with { SubscriptionId = SubscriptionId.New() })).IsFalse();
        await Assert.That(DurableHttpLostAckProofEvaluator.PendingSurvivesClose(Ready, Ready with { OperationStatus = ReadyStatus with { State = SyncOperationState.Uploading } })).IsFalse();
        await Assert.That(DurableHttpLostAckProofEvaluator.PendingSurvivesClose(Ready, Ready with { OperationStatus = ReadyStatus with { Attempt = ChangedSequence } })).IsFalse();
        var changedRetry = ReadyRetryState with { DueUtc = DateTimeOffset.UnixEpoch.AddSeconds(ChangedSequence) };
        await Assert.That(DurableHttpLostAckProofEvaluator.PendingSurvivesClose(Ready, Ready with { RetryState = changedRetry })).IsFalse();
    }

    /// <summary>Cursor and snapshot proofs fail on absent or unchanged durable state.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CursorAndSnapshotRequireIndependentDurableProgress()
    {
        await Assert.That(DurableHttpLostAckProofEvaluator.CursorAdvanced(Ready, Ready with { ServerCursor = NextCursor })).IsTrue();
        await Assert.That(DurableHttpLostAckProofEvaluator.CursorAdvanced(Ready, Ready with { ServerCursor = null })).IsFalse();
        await Assert.That(DurableHttpLostAckProofEvaluator.CursorAdvanced(Ready, Ready with { ServerCursor = string.Empty })).IsFalse();
        await Assert.That(DurableHttpLostAckProofEvaluator.CursorAdvanced(Ready, Ready)).IsFalse();
        await Assert.That(DurableHttpLostAckProofEvaluator.SnapshotRestored(Ready, Ready)).IsTrue();
        await Assert.That(DurableHttpLostAckProofEvaluator.SnapshotRestored(Ready with { SnapshotCounter = null }, Ready)).IsFalse();
        await Assert.That(DurableHttpLostAckProofEvaluator.SnapshotRestored(Ready, Ready with { SnapshotCounter = null })).IsFalse();
    }

    /// <summary>Missing durable facts produce truthful diagnostic text.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DiagnosticsDistinguishMissingFromPersistedValues()
    {
        await Assert.That(DurableHttpLostAckProofEvaluator.Format((OperationId?)null)).IsEqualTo(string.Empty);
        await Assert.That(DurableHttpLostAckProofEvaluator.Format(Ready.PendingOperationId)).IsEqualTo(Ready.PendingOperationId?.Value.ToString("D"));
        await Assert.That(DurableHttpLostAckProofEvaluator.FormatPendingState(Ready with { OperationStatus = null })).IsEqualTo("missing");
        await Assert.That(DurableHttpLostAckProofEvaluator.FormatPendingState(Ready)).IsEqualTo(SyncOperationState.SavedLocally.ToString());
        await Assert.That(DurableHttpLostAckProofEvaluator.FormatCursorProgress(Ready with { ServerCursor = null }, Ready with { ServerCursor = null })).IsEqualTo("before=null;after=null");
        await Assert.That(DurableHttpLostAckProofEvaluator.FormatCursorProgress(Ready, Ready with { ServerCursor = NextCursor })).IsEqualTo("before=cursor-first;after=cursor-next");
        await Assert.That(DurableHttpLostAckProofEvaluator.FormatSnapshotProof(Ready with { SnapshotCounter = null }, Ready)).IsEqualTo("writer=missing;observer=1");
        await Assert.That(DurableHttpLostAckProofEvaluator.FormatCounter(Ready.SnapshotCounter)).IsEqualTo("1");
    }

    /// <summary>Creates a valid immutable store sample for negative-state variants.</summary>
    /// <returns>The ready store proof.</returns>
    private static ClientStoreProof CreateReadyProof()
    {
        var operationId = OperationId.New();
        var now = DateTimeOffset.UnixEpoch;
        var status = new SyncOperationStatus(operationId, new("resilience/lost-ack/gcounter"), SyncOperationState.SavedLocally, SingleEffect, now, null);
        var retry = RetryState.Start(now) with { DueUtc = now.AddSeconds(SingleEffect) };
        return new(SubscriptionId.New(), "cursor-first", SingleEffect, operationId, SingleEffect, SingleEffect, status, retry, 0);
    }
}
