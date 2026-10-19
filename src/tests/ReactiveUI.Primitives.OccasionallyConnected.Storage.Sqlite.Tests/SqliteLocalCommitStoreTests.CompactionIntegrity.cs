// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Verifies compaction boundaries against real persisted history.</summary>
public sealed partial class SqliteLocalCommitStoreTests
{
    /// <summary>The history length spanning more than one compaction selection batch.</summary>
    private const int CompactionHistoryLength = 70;

    /// <summary>Verifies malformed requests leave persisted history intact.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task InvalidCompactionBudgetCannotDeleteHistory()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path, new ManualTimeProvider(CompactionNow));
        var operation = CommitOperation(store, Stream, 1, TerminalCompactionPayloadText);
        Action invalid = () => _ = store.Compact(new(Stream, CompactionNow, -1), CompactionRetention, CancellationToken.None);
        await Assert.That(invalid).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(OutboxOperationExists(database.Path, operation.OperationId)).IsTrue();
    }

    /// <summary>Verifies compaction spans bounded selections and preserves restart state.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task CompactionSpansMultipleSelectionsAndPreservesRestartState()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path, new ManualTimeProvider(CompactionNow));
        for (var sequence = 1; sequence <= CompactionHistoryLength; sequence++)
        {
            var operation = CommitOperation(store, Stream, sequence, TerminalCompactionPayloadText);
            SetOperationStateAt(database.Path, operation.OperationId, SyncOperationState.Rejected, CompactionNow.AddDays(EligibleTerminalAgeDays));
        }

        var result = store.Compact(new(Stream, CompactionNow, 0), CompactionRetention, CancellationToken.None);
        await Assert.That(result.RecordsRemoved).IsEqualTo(CompactionHistoryLength - 1);
        using var reopened = CreateInitializedStore(database.Path);
        var recovered = reopened.RecoverStream(Stream, reopened.GetOrCreateSubscriptionId(Stream, null, CancellationToken.None), CancellationToken.None);
        await Assert.That(recovered.Snapshot?.Revision).IsEqualTo(CompactionHistoryLength);
        await Assert.That(recovered.NextClientSequence).IsEqualTo(CompactionHistoryLength + 1);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(reopened.Compact(new(Stream, CompactionNow, 0), CompactionRetention, CancellationToken.None).RecordsRemoved).IsEqualTo(0);
    }
}
