// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reflection;
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
        await Assert.That(result.ReplayOperations).Count().IsEqualTo(CopiedCount);
        await Assert.That(result.ReplayOperations[0]).IsSameReferenceAs(operation);
        await Assert.That(result.DeadLetters).Count().IsEqualTo(CopiedCount);
        await Assert.That(result.DeadLetters[0]).IsEqualTo(deadLetter);
        await Assert.That(((ICollection<SyncOperation>)result.PendingOperations).IsReadOnly).IsTrue();
        await Assert.That(((ICollection<SyncOperation>)result.ReplayOperations).IsReadOnly).IsTrue();
        await Assert.That(((ICollection<DeadLetterRecord>)result.DeadLetters).IsReadOnly).IsTrue();
    }

    /// <summary>Verifies replay operations use init-only copy semantics independent from pending operations.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ReplayOperationsCanBeInitializedIndependentlyAndCopiesCollections()
    {
        var pending = CreateOperation();
        var replay = CreateOperation();
        var replayOperations = new List<SyncOperation> { replay };
        var result = new RecoveredStream(SubscriptionId.New(), ServerCursor, CreateSnapshot(), [pending], [], NextClientSequence) { ReplayOperations = replayOperations };

        replayOperations.Add(CreateOperation());

        await Assert.That(result.PendingOperations).Count().IsEqualTo(CopiedCount);
        await Assert.That(result.PendingOperations[0]).IsSameReferenceAs(pending);
        await Assert.That(result.ReplayOperations).Count().IsEqualTo(CopiedCount);
        await Assert.That(result.ReplayOperations[0]).IsSameReferenceAs(replay);
    }

    /// <summary>Verifies null pending operations are rejected.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorRejectsNullPendingOperations()
    {
        var constructor = typeof(RecoveredStream).GetConstructors().Single();
        var exception = await Assert.That(() => constructor.Invoke([SubscriptionId.New(), null, null, null, Array.Empty<DeadLetterRecord>(), 1L]))
            .ThrowsExactly<TargetInvocationException>();

        await Assert.That(exception?.InnerException).IsTypeOf<ArgumentNullException>();
    }

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
