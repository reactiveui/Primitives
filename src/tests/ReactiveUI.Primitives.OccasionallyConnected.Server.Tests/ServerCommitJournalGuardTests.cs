// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Tests for <see cref="ServerCommitJournalGuard"/>.</summary>
public sealed class ServerCommitJournalGuardTests
{
    /// <summary>The authenticated tenant.</summary>
    private const string Tenant = "tenant";

    /// <summary>The authenticated client.</summary>
    private const string Client = "client";

    /// <summary>The first server cursor.</summary>
    private const string Cursor = "cursor";

    /// <summary>The second server cursor.</summary>
    private const string OtherCursor = "cursor-2";

    /// <summary>The first version.</summary>
    private const string Version = "v1";

    /// <summary>The payload contract identifier.</summary>
    private const string Contract = "contract";

    /// <summary>The payload content type.</summary>
    private const string ContentType = "text/plain";

    /// <summary>The payload hash.</summary>
    private const string PayloadHash = "hash";

    /// <summary>The parameter name used for direct validation.</summary>
    private const string ValueParameter = "value";

    /// <summary>The maximum identifier length.</summary>
    private const int MaximumIdentifierCharacters = 256;

    /// <summary>The cursor UTF-8 byte bound.</summary>
    private const int MaximumCursorUtf8Bytes = 4096;

    /// <summary>The default test journal item limit.</summary>
    private const int DefaultLimit = 8;

    /// <summary>The default test journal logical byte limit.</summary>
    private const long DefaultLogicalBytes = 4096;

    /// <summary>The default test retention duration in minutes.</summary>
    private const int DefaultRetentionMinutes = 5;

    /// <summary>The second deterministic operation seed.</summary>
    private const int SecondOperationSeed = 2;

    /// <summary>The valid supplementary code point.</summary>
    private const int ValidSupplementaryCodePoint = 128_512;

    /// <summary>The expected UTF-8 byte count for the valid supplementary code point.</summary>
    private const int SupplementaryUtf8Bytes = 4;

    /// <summary>The invalid high-surrogate character.</summary>
    private const char HighSurrogate = '\ud800';

    /// <summary>The invalid low-surrogate character.</summary>
    private const char LowSurrogate = '\udc00';

    /// <summary>The fixed timestamp.</summary>
    private static readonly DateTimeOffset Start = new(2026, 9, 12, 10, 0, 0, TimeSpan.Zero);

    /// <summary>The stream identifier.</summary>
    private static readonly StreamId Stream = new("stream");

    /// <summary>The other stream identifier.</summary>
    private static readonly StreamId OtherStream = new("stream-b");

    /// <summary>Verifies invalid text and cursor values are rejected.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ValidateTextAndCursorRejectInvalidValues()
    {
        var oversizedIdentity = new string('x', MaximumIdentifierCharacters + 1);
        var oversizedCursor = new string('x', MaximumCursorUtf8Bytes + 1);
        var high = new string(HighSurrogate, 1);
        var low = new string(LowSurrogate, 1);

        await Assert.That(static () => ServerCommitJournalGuard.ValidateText(" ", ValueParameter)).ThrowsExactly<ArgumentException>();
        await Assert.That(() => ServerCommitJournalGuard.ValidateText(oversizedIdentity, ValueParameter)).ThrowsExactly<ArgumentException>();
        await Assert.That(() => ServerCommitJournalGuard.ValidateText(high, ValueParameter)).ThrowsExactly<ArgumentException>();
        await Assert.That(() => ServerCommitJournalGuard.ValidateText(low, ValueParameter)).ThrowsExactly<ArgumentException>();
        await Assert.That(static () => ServerCommitJournalGuard.ValidateCursor(" ")).ThrowsExactly<ArgumentException>();
        await Assert.That(() => ServerCommitJournalGuard.ValidateCursor(oversizedCursor)).ThrowsExactly<ArgumentException>();
        await Assert.That(() => ServerCommitJournalGuard.ValidateCursor(low)).ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies valid supplementary characters are accepted.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ValidateTextAndCursorAcceptValidSurrogatePairs()
    {
        var value = char.ConvertFromUtf32(ValidSupplementaryCodePoint);

        ServerCommitJournalGuard.ValidateText(value, nameof(value));
        ServerCommitJournalGuard.ValidateCursor(value);

        await Assert.That(ServerCommitJournalGuard.GetTextBytes(value)).IsEqualTo(SupplementaryUtf8Bytes);
    }

    /// <summary>Verifies invalid revision, state, operation and stamp inputs fail before mutation.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ValidatePlanRejectsInvalidRevisionStateOperationAndStamp()
    {
        var key = OperationKey(1);
        var otherKey = OperationKey(SecondOperationSeed);
        var journal = CreateJournal();

        await Assert.That(() => journal.TryCommit(new(StreamKey(), -1, null, null, [Entry(key)]))).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(() => journal.TryCommit(new(StreamKey(), 0, State(OtherStream), null, [Entry(key)]))).ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => journal.TryCommit(new(StreamKey(), 0, null, Stamp(otherKey), [Entry(key)]))).ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => journal.TryCommit(new(StreamKey(), 0, null, null, [Entry(new(Client, new(Guid.Empty)))]))).ThrowsExactly<ArgumentException>();
        await Assert.That(() => journal.TryCommit(new(StreamKey(), 0, null, null, [Entry(new(" ", key.OperationId))]))).ThrowsExactly<ArgumentException>();
        await Assert.That(journal.StreamCount).IsEqualTo(0);
    }

    /// <summary>Verifies invalid prepared entry shapes are rejected before mutation.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ValidatePlanRejectsInvalidEntryShapes()
    {
        var key = OperationKey(1);
        var otherKey = OperationKey(SecondOperationSeed);
        var journal = CreateJournal(maximumOperationCaptureCount: 1);
        var duplicateJournal = CreateJournal();
        var committed = Entry(key).Commit(Start, Start.AddMinutes(1));
        var mismatchedResult = new ServerLedgerEntry(
            key,
            Fingerprint(1),
            new(otherKey.OperationId, OperationResultKind.Accepted, null, Version),
            [],
            []);
        var conflicts = new[]
        {
            new ResolvedConflict(key.OperationId, "first", null),
            new ResolvedConflict(key.OperationId, "second", null),
        };
        var mismatchedConflict = new ResolvedConflict(otherKey.OperationId, "other", null);
        var emptySlot = new ServerLedgerEntry[1];

        await Assert.That(() => journal.TryCommit(new(StreamKey(), 0, null, null, [Entry(key), Entry(otherKey)]))).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(() => duplicateJournal.TryCommit(new(StreamKey(), 0, null, null, [Entry(key), Entry(key)]))).ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => journal.TryCommit(new(StreamKey(), 0, null, null, [committed]))).ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => journal.TryCommit(new(StreamKey(), 0, null, null, [mismatchedResult]))).ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => journal.TryCommit(new(StreamKey(), 0, null, null, [Entry(key, conflicts: conflicts)]))).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(() => journal.TryCommit(new(StreamKey(), 0, null, null, [Entry(key, conflicts: [mismatchedConflict])]))).ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => journal.TryCommit(new(StreamKey(), 0, null, null, emptySlot))).ThrowsExactly<ArgumentNullException>();
        await Assert.That(journal.StreamCount).IsEqualTo(0);
    }

    /// <summary>Verifies invalid event identities and causes are rejected before mutation.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ValidatePlanRejectsInvalidEvents()
    {
        var key = OperationKey(1);
        var otherKey = OperationKey(SecondOperationSeed);
        var eventId = Guid.Parse("11111111-1111-1111-1111-111111111111");
        var journal = CreateJournal(maximumEntryEventCount: 1);
        var duplicateJournal = CreateJournal(maximumEntryEventCount: 2);

        await Assert.That(() => journal.TryCommit(new(StreamKey(), 0, null, null, [Entry(key, events: [Event(key, Cursor), Event(key, OtherCursor)])])))
            .ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(() => journal.TryCommit(new(StreamKey(), 0, null, null, [Entry(key, events: [Event(key, Cursor, eventId: Guid.Empty)])])))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => duplicateJournal.TryCommit(new(StreamKey(), 0, null, null, [Entry(key, events: [Event(key, Cursor, eventId), Event(key, OtherCursor, eventId)])])))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => duplicateJournal.TryCommit(new(StreamKey(), 0, null, null, [Entry(key, events: [Event(key, Cursor, eventId), Event(key, Cursor)])])))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => journal.TryCommit(new(StreamKey(), 0, null, null, [Entry(key, events: [Event(key, Cursor, causedBy: otherKey.OperationId)])])))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(journal.TryCommit(new(StreamKey(), 0, null, null, [Entry(key, events: [EventWithoutCause(Cursor)])])).Status)
            .IsEqualTo(ServerCommitStatus.Committed);
    }

    /// <summary>Creates a configured journal.</summary>
    /// <param name="maximumOperationCaptureCount">The operation capture count.</param>
    /// <param name="maximumEntryEventCount">The entry event count.</param>
    /// <returns>The journal.</returns>
    private static InMemoryServerCommitJournal CreateJournal(
        int maximumOperationCaptureCount = DefaultLimit,
        int maximumEntryEventCount = DefaultLimit) =>
        new(new()
        {
            MaximumStreams = DefaultLimit,
            MaximumLedgerEntries = DefaultLimit,
            MaximumEvents = DefaultLimit,
            MaximumLogicalBytes = DefaultLogicalBytes,
            MaximumOperationCaptureCount = maximumOperationCaptureCount,
            MaximumEntryEventCount = maximumEntryEventCount,
            OperationRetention = TimeSpan.FromMinutes(DefaultRetentionMinutes),
            TimeProvider = new ManualTimeProvider(),
        });

    /// <summary>Creates an entry.</summary>
    /// <param name="key">The operation key.</param>
    /// <param name="conflicts">The conflicts.</param>
    /// <param name="events">The events.</param>
    /// <returns>The entry.</returns>
    private static ServerLedgerEntry Entry(
        ServerOperationKey key,
        IReadOnlyList<ResolvedConflict>? conflicts = null,
        IReadOnlyList<RemoteEvent>? events = null) =>
        new(
            key,
            Fingerprint(1),
            new(key.OperationId, OperationResultKind.Accepted, null, Version),
            conflicts ?? [],
            events ?? [Event(key, Cursor)]);

    /// <summary>Creates an event.</summary>
    /// <param name="key">The operation key.</param>
    /// <param name="cursor">The server cursor.</param>
    /// <param name="eventId">The optional event identifier.</param>
    /// <param name="causedBy">The optional causing operation.</param>
    /// <returns>The event.</returns>
    private static RemoteEvent Event(
        ServerOperationKey key,
        string cursor,
        Guid? eventId = null,
        OperationId? causedBy = null) =>
        new(eventId ?? Guid.NewGuid(), Stream, cursor, Start, causedBy ?? key.OperationId, Payload(), new Dictionary<string, string>());

    /// <summary>Creates an event with no causing operation.</summary>
    /// <param name="cursor">The server cursor.</param>
    /// <returns>The event.</returns>
    private static RemoteEvent EventWithoutCause(string cursor) =>
        new(Guid.NewGuid(), Stream, cursor, Start, null, Payload(), new Dictionary<string, string>());

    /// <summary>Creates state for a stream.</summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <returns>The state.</returns>
    private static ServerState State(StreamId streamId) => new(streamId, Version, Payload());

    /// <summary>Creates a write stamp.</summary>
    /// <param name="key">The operation key.</param>
    /// <returns>The write stamp.</returns>
    private static ServerWriteStamp Stamp(ServerOperationKey key) => new(Start, key.ClientId, key.OperationId);

    /// <summary>Creates an operation key.</summary>
    /// <param name="seed">The operation seed.</param>
    /// <returns>The operation key.</returns>
    private static ServerOperationKey OperationKey(int seed) => new(Client, new(new Guid(seed, 0, 0, [0, 0, 0, 0, 0, 0, 0, 1])));

    /// <summary>Creates a stream key.</summary>
    /// <returns>The stream key.</returns>
    private static ServerStreamKey StreamKey() => new(Tenant, Stream);

    /// <summary>Creates a payload envelope.</summary>
    /// <returns>The payload.</returns>
    private static PayloadEnvelope Payload() => new(Contract, 1, ContentType, Array.Empty<byte>(), PayloadHash);

    /// <summary>Creates a fingerprint.</summary>
    /// <param name="seed">The first fingerprint byte.</param>
    /// <returns>The fingerprint.</returns>
    private static ServerCommitFingerprint Fingerprint(byte seed)
    {
        var bytes = new byte[ServerCommitFingerprint.Length];
        bytes[0] = seed;
        return new(bytes);
    }

    /// <summary>Manual time provider for deterministic tests.</summary>
    private sealed class ManualTimeProvider : TimeProvider
    {
        /// <inheritdoc/>
        public override DateTimeOffset GetUtcNow() => Start;
    }
}
