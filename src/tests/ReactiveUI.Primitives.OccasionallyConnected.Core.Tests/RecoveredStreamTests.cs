// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="RecoveredStream"/>.</summary>
public sealed class RecoveredStreamTests
{
    /// <summary>The copied collection count.</summary>
    private const int CopiedCount = 1;

    /// <summary>The dead-lettered attempt count.</summary>
    private const int DeadLetterAttempts = 1;

    /// <summary>The next client sequence.</summary>
    private const long NextClientSequence = 2;

    /// <summary>The recovered server cursor.</summary>
    private const string ServerCursor = "cursor";

    /// <summary>Verifies recovered stream values and collections are retained defensively.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorRetainsValuesAndCopiesCollections()
    {
        var operation = CreateOperation();
        var subscriptionId = SubscriptionId.New();
        var snapshot = CreateSnapshot();
        var deadLetter = new DeadLetterRecord(
            operation,
            "OC.Terminal",
            DeadLetterAttempts,
            DateTimeOffset.UnixEpoch);
        var operations = new List<SyncOperation> { operation };
        var deadLetters = new List<DeadLetterRecord> { deadLetter };
        var result = new RecoveredStream(
            subscriptionId,
            ServerCursor,
            snapshot,
            operations,
            deadLetters,
            NextClientSequence);

        operations.Add(CreateOperation());
        deadLetters.Add(new(CreateOperation(), "OC.Other", DeadLetterAttempts, DateTimeOffset.UnixEpoch));

        await Assert.That(result.SubscriptionId).IsEqualTo(subscriptionId);
        await Assert.That(result.ServerCursor).IsEqualTo(ServerCursor);
        await Assert.That(result.Snapshot).IsSameReferenceAs(snapshot);
        await Assert.That(result.NextClientSequence).IsEqualTo(NextClientSequence);
        await Assert.That(result.PendingOperations).Count().IsEqualTo(CopiedCount);
        await Assert.That(result.PendingOperations[0]).IsSameReferenceAs(operation);
        await Assert.That(result.DeadLetters).Count().IsEqualTo(CopiedCount);
        await Assert.That(result.DeadLetters[0]).IsEqualTo(deadLetter);
        await Assert.That(((ICollection<SyncOperation>)result.PendingOperations).IsReadOnly).IsTrue();
        await Assert.That(((ICollection<DeadLetterRecord>)result.DeadLetters).IsReadOnly).IsTrue();
    }

    /// <summary>Verifies null pending operations are rejected.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorRejectsNullPendingOperations() =>
        await Assert.That(static () => new RecoveredStream(SubscriptionId.New(), null, null, null!, [], 1)).ThrowsExactly<ArgumentNullException>();

    /// <summary>Creates a representative synchronization operation.</summary>
    /// <returns>A synchronization operation.</returns>
    private static SyncOperation CreateOperation() => new()
    {
        OperationId = OperationId.New(),
        StreamId = new("sensor/temperature"),
        ClientSequence = 1,
        TimestampUtc = DateTimeOffset.UnixEpoch,
        Type = SyncOperationType.Append,
        Payload = new("reading", 1, "application/json", ReadOnlyMemory<byte>.Empty, "sha256-empty"),
        Policy = OperationPolicy.Default,
        Metadata = new Dictionary<string, string>(),
    };

    /// <summary>Creates a representative local snapshot.</summary>
    /// <returns>A local snapshot.</returns>
    private static LocalSnapshot CreateSnapshot() =>
        new(new("sensor/temperature"), 1, ServerCursor, CreateOperation().Payload, DateTimeOffset.UnixEpoch);
}
