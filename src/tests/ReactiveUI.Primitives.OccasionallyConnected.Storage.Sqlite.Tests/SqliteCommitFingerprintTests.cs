// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests for <see cref="SqliteCommitFingerprint"/>.</summary>
public sealed class SqliteCommitFingerprintTests
{
    /// <summary>Verifies null and empty base versions produce distinct canonical fingerprints.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenBaseVersionIsNullOrEmpty_ThenFingerprintsRemainDistinct()
    {
        var streamId = new StreamId("sensor/fingerprint");
        var snapshot = new SnapshotMutation(streamId, CreatePayload("snapshot"), FormatVersion: 1, ExpectedRevision: 0);
        var withoutBaseVersion = CreateOperation(streamId) with { BaseVersion = null };
        var withEmptyBaseVersion = withoutBaseVersion with { BaseVersion = string.Empty };

        var nullFingerprint = SqliteCommitFingerprint.Compute(withoutBaseVersion, snapshot);
        var emptyFingerprint = SqliteCommitFingerprint.Compute(withEmptyBaseVersion, snapshot);

        await Assert.That(SqliteCommitFingerprint.Matches(nullFingerprint, emptyFingerprint)).IsFalse();
    }

    /// <summary>Creates a representative operation for fingerprint tests.</summary>
    /// <param name="streamId">The stream identity.</param>
    /// <returns>The operation.</returns>
    private static SyncOperation CreateOperation(StreamId streamId) => new()
    {
        OperationId = OperationId.New(),
        StreamId = streamId,
        ClientSequence = 1,
        TimestampUtc = new(2026, 1, 2, 3, 4, 5, TimeSpan.Zero),
        BaseVersion = "server-a",
        Type = SyncOperationType.Update,
        Payload = CreatePayload("operation"),
        Policy = new(DeliveryGuarantee.AtLeastOnce, OperationDurability.Durable, Priority: 1, ConflictPolicy.Merge),
        Metadata = new Dictionary<string, string>(StringComparer.Ordinal) { ["origin"] = "unit-test" },
    };

    /// <summary>Creates a representative payload envelope.</summary>
    /// <param name="text">The payload text.</param>
    /// <returns>The payload.</returns>
    private static PayloadEnvelope CreatePayload(string text) => new("reading", 1, "application/json", System.Text.Encoding.UTF8.GetBytes(text), $"hash-{text}");
}
