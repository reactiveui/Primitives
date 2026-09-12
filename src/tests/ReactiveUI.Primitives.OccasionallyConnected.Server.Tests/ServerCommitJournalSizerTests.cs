// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Tests for <see cref="ServerCommitJournalSizer"/>.</summary>
public sealed class ServerCommitJournalSizerTests
{
    /// <summary>The valid supplementary code point used in UTF-8 accounting.</summary>
    private const int ValidSupplementaryCodePoint = 128_512;

    /// <summary>The maximum payload length used to prove long widening starts before addition.</summary>
    private const int MaximumPayloadLength = int.MaxValue;

    /// <summary>The expected payload byte count when the payload length reaches the 32-bit limit.</summary>
    private const long ExpectedMaximumPayloadBytes = (long)MaximumPayloadLength + 3;

    /// <summary>Verifies payload byte accounting widens before adding text byte counts.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task GetPayloadBytesWidensBeforeAddingTextAndPayloadLength()
    {
        await Assert.That(ServerCommitJournalSizer.GetPayloadBytes(1, 1, 1, MaximumPayloadLength)).IsEqualTo(ExpectedMaximumPayloadBytes);
    }

    /// <summary>Verifies logical byte addition reports arithmetic overflow.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task AddLogicalBytesRejectsOverflow() =>
        await Assert.That(static () => ServerCommitJournalSizer.AddLogicalBytes(long.MaxValue, 1)).ThrowsExactly<InvalidOperationException>();

    /// <summary>Verifies reason codes are included in retained result byte accounting.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task GetEntryBytesIncludesReasonCodeBytes()
    {
        var key = new ServerOperationKey("client", new(Guid.Parse("11111111-1111-1111-1111-111111111111")));
        var fingerprint = new ServerCommitFingerprint(new byte[ServerCommitFingerprint.Length]);
        var withoutReason = new ServerLedgerEntry(
            key,
            fingerprint,
            new(key.OperationId, OperationResultKind.Rejected, null, "v1"),
            [],
            []);
        var withReason = new ServerLedgerEntry(
            key,
            fingerprint,
            new(key.OperationId, OperationResultKind.Rejected, "reason", "v1"),
            [],
            []);

        await Assert.That(ServerCommitJournalSizer.GetEntryBytes(withReason))
            .IsEqualTo(ServerCommitJournalSizer.GetEntryBytes(withoutReason) + ServerCommitJournalGuard.GetTextBytes("reason"));
    }

    /// <summary>Verifies retained origin identities use their actual strict UTF-8 byte count.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task GetEntryBytesIncludesOriginClientUtf8Bytes()
    {
        var key = new ServerOperationKey("client", OperationId.New());
        var fingerprint = new ServerCommitFingerprint(new byte[ServerCommitFingerprint.Length]);
        var withoutOrigin = new ServerLedgerEntry(
            key,
            fingerprint,
            new(key.OperationId, OperationResultKind.Accepted, null, "v1"),
            [],
            [CreateEvent(key.OperationId)]);
        var clientId = char.ConvertFromUtf32(ValidSupplementaryCodePoint);
        var withOrigin = new ServerLedgerEntry(
            key,
            fingerprint,
            new(key.OperationId, OperationResultKind.Accepted, null, "v1"),
            [],
            [CreateEvent(key.OperationId) with { Origin = new(clientId, key.OperationId) }]);

        await Assert.That(ServerCommitJournalSizer.GetEntryBytes(withOrigin))
            .IsEqualTo(ServerCommitJournalSizer.GetEntryBytes(withoutOrigin) + ServerCommitJournalGuard.GetTextBytes(clientId));
    }

    /// <summary>Creates a representative remote event.</summary>
    /// <param name="operationId">The causing operation identifier.</param>
    /// <returns>The event.</returns>
    private static RemoteEvent CreateEvent(OperationId operationId) =>
        new(
            Guid.NewGuid(),
            new("stream"),
            "cursor",
            DateTimeOffset.UnixEpoch,
            operationId,
            new("contract", 1, "text/plain", Array.Empty<byte>(), "hash"),
            new Dictionary<string, string>());
}
