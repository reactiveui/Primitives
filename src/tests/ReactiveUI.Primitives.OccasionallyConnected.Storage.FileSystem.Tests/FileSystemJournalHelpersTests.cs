// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.FileSystem;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.FileSystem.Tests;

/// <summary>Tests defensive journal and retention helpers.</summary>
public sealed class FileSystemJournalHelpersTests
{
    /// <summary>Verifies checksum comparison rejects incompatible lengths.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DifferentChecksumLengthsAreRejected() =>
        await Assert.That(FileSystemJournalHelpers.FixedTimeEquals([], [1])).IsFalse();

    /// <summary>Verifies stream and operation lookups fail closed when durable data is absent.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task MissingDurableEntriesAreRejected()
    {
        var state = new FileSystemLocalStoreAdapter.StoreState();
        await Assert.That(() => FileSystemJournalHelpers.GetStream(state, new("missing"))).Throws<InvalidOperationException>();
        await Assert.That(() => FileSystemJournalHelpers.FindOperation(state, OperationId.New())).Throws<InvalidOperationException>();
    }

    /// <summary>Verifies a server result cannot acknowledge an operation missing from durable state.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ResultWithMissingDurableOperationIsRejected()
    {
        var state = new FileSystemLocalStoreAdapter.StoreState();
        var operationId = OperationId.New();
        var lease = new FileSystemLocalStoreAdapter.LeaseState { OperationIds = [operationId.Value] };
        var result = new RemoteSyncResult(
            Guid.NewGuid(),
            [new(operationId, OperationResultKind.Accepted, null, null)],
            null,
            null);
        await Assert.That(() => FileSystemJournalHelpers.ValidateResult(state, lease, result)).Throws<InvalidOperationException>();
    }

    /// <summary>Verifies retention cannot remove a terminal operation still owned by a lease.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task TerminalLeasedOperationCannotBeCompacted()
    {
        var state = new FileSystemLocalStoreAdapter.StoreState();
        var operation = new SyncOperation
        {
            OperationId = OperationId.New(),
            StreamId = new("leased"),
            ClientSequence = 0,
            TimestampUtc = DateTimeOffset.UnixEpoch,
            Type = SyncOperationType.Update,
            Payload = new("test", 1, "text/plain", "payload"u8.ToArray(), "hash"),
        };
        var status = new FileSystemLocalStoreAdapter.OperationState
        {
            Operation = operation,
            Terminal = true,
            Status = new(operation.OperationId, operation.StreamId, SyncOperationState.Rejected, 0, DateTimeOffset.UnixEpoch, null),
        };
        var leaseId = Guid.NewGuid();
        state.Leases[leaseId] = new() { LeaseId = leaseId, OperationIds = [operation.OperationId.Value], };
        await Assert.That(FileSystemJournalHelpers.CanCompact(state, status, DateTimeOffset.UnixEpoch.AddTicks(1))).IsFalse();
    }
}
