// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="ConflictContext"/>.</summary>
public sealed class ConflictContextTests
{
    /// <summary>The stream and payload contract used by the counter application.</summary>
    private const string CounterName = "counter";

    /// <summary>Verifies a caller cannot change the transaction input after construction.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorOwnsOrderedIncomingOperations()
    {
        var payload = new PayloadEnvelope(CounterName, 1, "application/json", ReadOnlyMemory<byte>.Empty, "hash");
        var operation = new SyncOperation
        {
            OperationId = OperationId.New(),
            StreamId = new(CounterName),
            ClientSequence = 1,
            TimestampUtc = DateTimeOffset.UnixEpoch,
            Type = SyncOperationType.Append,
            Payload = payload,
        };
        var current = new ServerState(operation.StreamId, "v1", payload);
        var client = new ClientIdentity("device");
        var incoming = new List<SyncOperation> { operation };
        var context = new ConflictContext(current, incoming, client);

        incoming.Clear();

        await Assert.That(context.Incoming).Count().IsEqualTo(1);
        await Assert.That(context.Incoming[0]).IsSameReferenceAs(operation);
        await Assert.That(context.Current).IsSameReferenceAs(current);
        await Assert.That(context.Client).IsSameReferenceAs(client);
    }

    /// <summary>Verifies missing operation collections are rejected at construction.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorRejectsMissingIncomingOperations()
    {
        var state = new ServerState(new(CounterName), "v1", new(CounterName, 1, "application/json", ReadOnlyMemory<byte>.Empty, "hash"));

        await Assert.That(() => new ConflictContext(state, null!, new("device"))).ThrowsExactly<ArgumentNullException>();
    }
}
