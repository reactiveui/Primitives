// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests for <see cref="SqliteLocalStoreAdapterSizing"/>.</summary>
public sealed class SqliteLocalStoreAdapterSizingTests
{
    /// <summary>The capacity used to reject oversized logical inputs.</summary>
    private const long SmallCapacityBytes = 1024;

    /// <summary>The small capacity used to reject an oversized payload.</summary>
    private const long PayloadCapacityBytes = 1024;

    /// <summary>The capacity that is smaller than a retained operation identifier.</summary>
    private const long IdentifierCapacityBytes = 15;

    /// <summary>The oversized text length used by payload, metadata, and retry-state inputs.</summary>
    private const int OversizedTextLength = 4096;

    /// <summary>The encoded retry input includes the identifier, object shell, timestamp, counters and three absent-value markers.</summary>
    private const long EmptyRetryBytes = 75;

    /// <summary>The payload content used by inputs that fit the configured budget.</summary>
    private const string PayloadText = "payload";

    /// <summary>The invalid event identifier count used to validate preflight argument checks.</summary>
    private const int NegativeEventIdCount = -1;

    /// <summary>A representative stream identity.</summary>
    private static readonly StreamId Stream = new("sensor/temperature");

    /// <summary>Verifies payload sizing rejects input that cannot fit a configured empty worker.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenPayloadExceedsCapacity_ThenSizingRejectsIt()
    {
        var sizing = new SqliteLocalStoreAdapterSizing(PayloadCapacityBytes);
        var payload = CreatePayload(new('p', OversizedTextLength));
        var metadata = new Dictionary<string, string>();
        await Assert.That(sizing.CommitBytes(CreateOperation(CreatePayload(PayloadText), metadata), CreateSnapshot())).IsLessThanOrEqualTo(PayloadCapacityBytes);
        var action = () => _ = sizing.CommitBytes(CreateOperation(payload, metadata), CreateSnapshot());

        var exception = await Assert.That(action).ThrowsExactly<QueueCapacityExceededException>();

        await Assert.That(exception?.CanFitWhenEmpty).IsFalse();
    }

    /// <summary>Verifies metadata sizing stops when a value cannot fit the configured empty worker.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenMetadataExceedsCapacity_ThenSizingRejectsIt()
    {
        var sizing = new SqliteLocalStoreAdapterSizing(SmallCapacityBytes);
        var metadata = new Dictionary<string, string> { ["origin"] = new('m', OversizedTextLength) };
        await Assert.That(sizing.CommitBytes(CreateOperation(CreatePayload(PayloadText), new Dictionary<string, string>()), CreateSnapshot())).IsLessThanOrEqualTo(SmallCapacityBytes);
        var action = () => _ = sizing.CommitBytes(CreateOperation(CreatePayload(PayloadText), metadata), CreateSnapshot());

        var exception = await Assert.That(action).ThrowsExactly<QueueCapacityExceededException>();

        await Assert.That(exception?.CanFitWhenEmpty).IsFalse();
    }

    /// <summary>Verifies retry-state optional fields contribute to the capacity-bound logical metric.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenRetryStateOptionalFieldsExceedCapacity_ThenSizingRejectsIt()
    {
        var sizing = new SqliteLocalStoreAdapterSizing(SmallCapacityBytes);
        await Assert.That(sizing.RetryStateBytes(RetryState.Start(DateTimeOffset.UnixEpoch))).IsEqualTo(EmptyRetryBytes);
        var retryState = new RetryState(
            DateTimeOffset.UnixEpoch,
            DateTimeOffset.UnixEpoch.AddMinutes(1),
            TimeSpan.FromSeconds(1),
            TransientAttemptCount: 1,
            RetryAuthenticationState.RenewalRetryUsed,
            new('r', OversizedTextLength));
        var action = () => _ = sizing.RetryStateBytes(retryState);

        var exception = await Assert.That(action).ThrowsExactly<QueueCapacityExceededException>();

        await Assert.That(exception?.CanFitWhenEmpty).IsFalse();
    }

    /// <summary>Verifies an exact logical capacity accepts nullable retry fields.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenRetryStateExactlyFitsCapacity_ThenSizingAcceptsNullableFields()
    {
        var retryState = RetryState.Start(DateTimeOffset.UnixEpoch);
        var exactSizing = new SqliteLocalStoreAdapterSizing(EmptyRetryBytes);

        var actualBytes = exactSizing.RetryStateBytes(retryState);

        await Assert.That(actualBytes).IsEqualTo(EmptyRetryBytes);
        var tooSmall = new SqliteLocalStoreAdapterSizing(EmptyRetryBytes - 1);
        var action = () => _ = tooSmall.RetryStateBytes(retryState);
        await Assert.That(action).ThrowsExactly<QueueCapacityExceededException>();
    }

    /// <summary>Verifies negative event identifier counts are rejected before sizing arithmetic.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenEventIdCountIsNegative_ThenSizingRejectsIt()
    {
        var sizing = new SqliteLocalStoreAdapterSizing(long.MaxValue);
        var action = () => _ = sizing.EventIdLookupBytes(Stream, NegativeEventIdCount);

        await Assert.That(action).ThrowsExactly<ArgumentOutOfRangeException>();
    }

    /// <summary>Verifies fixed command inputs participate in capacity-bound admission.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenOperationIdentifierExceedsCapacity_ThenSizingRejectsIt()
    {
        var sizing = new SqliteLocalStoreAdapterSizing(IdentifierCapacityBytes);
        var action = () => _ = sizing.OperationIdentifierBytes;

        var exception = await Assert.That(action).ThrowsExactly<QueueCapacityExceededException>();

        await Assert.That(exception?.CanFitWhenEmpty).IsFalse();
    }

    /// <summary>Verifies present optional values are included in the logical metric.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenOptionalValuesArePresent_ThenSizingIncludesThem()
    {
        var sizing = new SqliteLocalStoreAdapterSizing(long.MaxValue);
        var operationId = OperationId.New();
        var payload = CreatePayload("event");
        var remoteEvent = new RemoteEvent(
            Guid.NewGuid(),
            Stream,
            "cursor",
            DateTimeOffset.UnixEpoch,
            operationId,
            payload,
            new Dictionary<string, string>());
        var syncResult = new RemoteSyncResult(
            Guid.NewGuid(),
            [new(operationId, OperationResultKind.Accepted, "accepted", "version")],
            "cursor",
            TimeSpan.FromSeconds(1));

        var subscriptionBytes = sizing.SubscriptionLookupBytes(Stream, SubscriptionId.New());
        var leaseBytes = sizing.LeaseRequestBytes(new(Stream, 1, 1, TimeSpan.FromSeconds(1)));
        var unfilteredLeaseBytes = sizing.LeaseRequestBytes(new(null, 1, 1, TimeSpan.FromSeconds(1)));
        var syncBytes = sizing.SyncResultBytes(syncResult);
        var remoteBytes = sizing.RemoteApplyBytes(
            new(Guid.NewGuid(), Stream, "previous", "next", [remoteEvent]),
            CreateSnapshot());
        var compactionBytes = sizing.CompactionBytes(new(Stream, DateTimeOffset.UnixEpoch, 1));
        var unfilteredCompactionBytes = sizing.CompactionBytes(new(null, DateTimeOffset.UnixEpoch, 1));

        await Assert.That(subscriptionBytes).IsGreaterThan(sizing.SubscriptionLookupBytes(Stream, null));
        await Assert.That(leaseBytes).IsGreaterThan(unfilteredLeaseBytes);
        await Assert.That(syncBytes).IsGreaterThan(sizing.SyncResultBytes(new(syncResult.BatchId, syncResult.Operations, null, null)));
        await Assert.That(remoteBytes).IsGreaterThan(sizing.RemoteApplyBytes(new(Guid.NewGuid(), Stream, "previous", "next", []), CreateSnapshot()));
        await Assert.That(compactionBytes).IsGreaterThan(unfilteredCompactionBytes);
    }

    /// <summary>Verifies the sizer rejects a non-positive configured capacity.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenCapacityIsNotPositive_ThenSizingRejectsIt()
    {
        var action = static () => _ = new SqliteLocalStoreAdapterSizing(capacityBytes: 0);

        await Assert.That(action).ThrowsExactly<ArgumentOutOfRangeException>();
    }

    /// <summary>Creates a representative local operation.</summary>
    /// <param name="payload">The operation payload.</param>
    /// <param name="metadata">The operation metadata.</param>
    /// <returns>The local operation.</returns>
    private static SyncOperation CreateOperation(PayloadEnvelope payload, IReadOnlyDictionary<string, string> metadata) => new()
    {
        OperationId = OperationId.New(),
        StreamId = Stream,
        ClientSequence = 1,
        TimestampUtc = DateTimeOffset.UnixEpoch,
        Type = SyncOperationType.Update,
        Payload = payload,
        Metadata = metadata,
    };

    /// <summary>Creates a representative payload envelope.</summary>
    /// <param name="content">The payload content.</param>
    /// <returns>The payload envelope.</returns>
    private static PayloadEnvelope CreatePayload(string content) =>
        new("reading", 1, "application/json", System.Text.Encoding.UTF8.GetBytes(content), "hash");

    /// <summary>Creates a representative snapshot mutation.</summary>
    /// <returns>The snapshot mutation.</returns>
    private static SnapshotMutation CreateSnapshot() => new(Stream, CreatePayload("snapshot"), formatVersion: 1);
}
