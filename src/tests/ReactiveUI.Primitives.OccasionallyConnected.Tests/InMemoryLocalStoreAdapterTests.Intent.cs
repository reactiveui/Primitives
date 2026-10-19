// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Verifies persisted canonical intent cannot be replaced by duplicate identifiers.</summary>
public sealed partial class InMemoryLocalStoreAdapterTests
{
    /// <summary>Verifies each canonical operation field participates in duplicate detection.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task DuplicateIdentifierCannotReplaceCanonicalOperation()
    {
        await using var store = await CreateInitializedStoreAsync();
        _ = await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = CreateOperation(1);
        var snapshot = CreateSnapshotMutation(0);
        var receipt = await store.CommitLocalOperationAsync(operation, snapshot, CancellationToken.None);
        SyncOperation[] changed =
        [
            operation with { ClientSequence = SecondClientSequence },
            operation with { TimestampUtc = operation.TimestampUtc.AddTicks(1) },
            operation with { BaseVersion = "different-version" },
            operation with { Type = SyncOperationType.Delete },
            operation with { Policy = operation.Policy with { DeliveryGuarantee = DeliveryGuarantee.AtMostOnce } },
            operation with { Metadata = new Dictionary<string, string>() },
            operation with { Metadata = new Dictionary<string, string> { ["different-key"] = "unit-test" } },
            operation with { Payload = operation.Payload with { SchemaVersion = SecondClientSequence } },
            operation with { Payload = operation.Payload with { ContractId = "other-contract" } },
            operation with { Payload = operation.Payload with { ContentType = "other-content-type" } },
            operation with { Payload = operation.Payload with { PayloadHash = "different-hash" } },
            operation with { Payload = operation.Payload with { Payload = new byte[] { 0 } } },
            operation with { Payload = operation.Payload with { Payload = new byte[operation.Payload.PayloadLength] } },
        ];
        foreach (var replacement in changed)
        {
            Func<Task> replace = async () => _ = await store.CommitLocalOperationAsync(replacement, snapshot, CancellationToken.None);
            await Assert.That(replace).ThrowsExactly<InvalidOperationException>();
        }

        var crossStream = operation with { StreamId = OtherStream };
        Func<Task> move = async () => _ = await store.CommitLocalOperationAsync(crossStream, snapshot with { StreamId = OtherStream }, CancellationToken.None);
        await Assert.That(move).ThrowsExactly<InvalidOperationException>();
        await Assert.That(await store.CommitLocalOperationAsync(operation, snapshot, CancellationToken.None)).IsEqualTo(receipt);
    }

    /// <summary>Verifies duplicate snapshot replacements cannot alter the original transaction.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task DuplicateIdentifierCannotReplaceSnapshotIntent()
    {
        await using var store = await CreateInitializedStoreAsync();
        var subscription = await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = CreateOperation(1);
        var snapshot = CreateSnapshotMutation(0);
        _ = await store.CommitLocalOperationAsync(operation, snapshot, CancellationToken.None);
        SnapshotMutation[] changed =
        [
            snapshot with { FormatVersion = SecondClientSequence },
            snapshot with { ExpectedRevision = 1 },
            snapshot with { State = CreatePayload("replacement") },
        ];
        foreach (var replacement in changed)
        {
            Func<Task> replace = async () => _ = await store.CommitLocalOperationAsync(operation, replacement, CancellationToken.None);
            await Assert.That(replace).ThrowsExactly<InvalidOperationException>();
        }

        await Assert.That((await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None)).Snapshot?.Revision).IsEqualTo(1);
    }

    /// <summary>Verifies malformed payload descriptors never enter the store.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task MalformedPayloadDescriptorsLeaveStreamEmpty()
    {
        await using var store = await CreateInitializedStoreAsync();
        var subscription = await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = CreateOperation(1);
        PayloadEnvelope[] malformed =
        [
            operation.Payload with { ContractId = " " },
            operation.Payload with { ContentType = " " },
            operation.Payload with { PayloadHash = " " },
        ];
        foreach (var payload in malformed)
        {
            Func<Task> commit = async () => _ = await store.CommitLocalOperationAsync(operation with { Payload = payload }, CreateSnapshotMutation(0), CancellationToken.None);
            await Assert.That(commit).ThrowsExactly<ArgumentException>();
        }

        var invalidSchemaOperation = operation with { Payload = operation.Payload with { SchemaVersion = 0 } };
        Func<Task> invalidSchema = async () => _ = await store.CommitLocalOperationAsync(invalidSchemaOperation, CreateSnapshotMutation(0), CancellationToken.None);
        Func<Task> invalidFormat = async () => _ = await store.CommitLocalOperationAsync(operation, CreateSnapshotMutation(0) with { FormatVersion = 0 }, CancellationToken.None);
        Func<Task> wrongStream = async () => _ = await store.CommitLocalOperationAsync(operation, CreateSnapshotMutation(0) with { StreamId = OtherStream }, CancellationToken.None);
        await Assert.That(invalidSchema).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(invalidFormat).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(wrongStream).ThrowsExactly<ArgumentException>();
        await Assert.That((await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None)).PendingOperations.Count).IsEqualTo(0);
    }
}
