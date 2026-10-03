// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Tests for <see cref="ServerCommitJournalOperations"/>.</summary>
public sealed class ServerCommitJournalOperationsTests
{
    /// <summary>The authenticated tenant.</summary>
    private const string Tenant = "tenant";

    /// <summary>The authenticated client.</summary>
    private const string Client = "client";

    /// <summary>The stream identifier.</summary>
    private const string StreamValue = "stream";

    /// <summary>The second deterministic operation seed.</summary>
    private const int SecondOperationSeed = 2;

    /// <summary>Verifies terminal status rejects revision overflow before duplicate checks.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task GetPreCommitStatusRejectsRevisionOverflow()
    {
        var stream = new ServerCommitStreamRecord { Revision = long.MaxValue };
        var commit = Validation(expectedRevision: long.MaxValue);

        await Assert.That(ServerCommitJournalOperations.GetPreCommitStatus(stream, commit)).IsEqualTo(ServerCommitStatus.RevisionOverflow);
    }

    /// <summary>Verifies terminal status rejects event sequence overflow.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task GetPreCommitStatusRejectsEventSequenceOverflow()
    {
        var stream = new ServerCommitStreamRecord { LastEventSequence = long.MaxValue };
        var commit = Validation(eventCount: 1);

        await Assert.That(ServerCommitJournalOperations.GetPreCommitStatus(stream, commit)).IsEqualTo(ServerCommitStatus.EventSequenceOverflow);
    }

    /// <summary>Verifies append-only state changes can intentionally leave the stamp unchanged.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ApplyStateLeavesAppendOnlyStampUnchangedWhenNoStampIsSupplied()
    {
        var stamp = new ServerWriteStamp(DateTimeOffset.UnixEpoch, "client", new(Guid.Parse("11111111-1111-1111-1111-111111111111")));
        var stream = new ServerCommitStreamRecord { LastWriteStamp = stamp };

        ServerCommitJournalOperations.ApplyState(stream, Validation());

        await Assert.That(stream.LastWriteStamp).IsEqualTo(stamp);
    }

    /// <summary>Verifies retained event identifiers reject a later operation with the same event.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task GetPreCommitStatusRejectsRetainedEventIdentifierConflict()
    {
        var eventId = Guid.Parse("22222222-2222-2222-2222-222222222222");
        var streamKey = new ServerStreamKey(Tenant, new(StreamValue));
        var stream = new ServerCommitStreamRecord { Revision = 1 };
        var first = Entry(OperationKey(1), eventId, "cursor-1");
        ServerCommitJournalOperations.AddLedgerRow(stream, streamKey, first.Commit(DateTimeOffset.UnixEpoch, DateTimeOffset.UnixEpoch.AddMinutes(1)), 1);
        var second = Entry(OperationKey(SecondOperationSeed), eventId, "cursor-2");

        await Assert.That(ServerCommitJournalOperations.GetPreCommitStatus(stream, Validation(expectedRevision: 1, entries: [second])))
            .IsEqualTo(ServerCommitStatus.IntentMismatch);
    }

    /// <summary>Creates a validation result.</summary>
    /// <param name="expectedRevision">The expected revision.</param>
    /// <param name="eventCount">The event count.</param>
    /// <param name="newWriteStamp">The optional write stamp.</param>
    /// <param name="entries">The optional entries.</param>
    /// <returns>The validation result.</returns>
    private static ServerCommitValidationResult Validation(
        long expectedRevision = 0,
        int eventCount = 0,
        ServerWriteStamp? newWriteStamp = null,
        ServerLedgerEntry[]? entries = null) =>
        new()
        {
            StreamKey = new(Tenant, new(StreamValue)),
            ExpectedRevision = expectedRevision,
            NewState = null,
            NewWriteStamp = newWriteStamp,
            Entries = entries ?? [],
            OperationKeys = [],
            EntryBytes = [],
            LedgerBytes = 0,
            EventCount = eventCount,
            LastCursor = null,
            LastCursorBytes = 0,
            StateBytes = 0,
        };

    /// <summary>Creates an operation key.</summary>
    /// <param name="seed">The operation seed.</param>
    /// <returns>The operation key.</returns>
    private static ServerOperationKey OperationKey(int seed) => new(Client, new(new Guid(seed, 0, 0, [0, 0, 0, 0, 0, 0, 0, 1])));

    /// <summary>Creates a ledger entry.</summary>
    /// <param name="key">The operation key.</param>
    /// <param name="eventId">The event identifier.</param>
    /// <param name="cursor">The server cursor.</param>
    /// <returns>The entry.</returns>
    private static ServerLedgerEntry Entry(ServerOperationKey key, Guid eventId, string cursor) =>
        new(
            key,
            new(new byte[ServerCommitFingerprint.Length]),
            new(key.OperationId, OperationResultKind.Accepted, null, "v1"),
            [],
            [
                new(
                    eventId,
                    new(StreamValue),
                    cursor,
                    DateTimeOffset.UnixEpoch,
                    key.OperationId,
                    new("contract", 1, "text/plain", Array.Empty<byte>(), "hash"),
                    new Dictionary<string, string>()),
            ]);
}
