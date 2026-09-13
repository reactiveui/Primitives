// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reflection;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="ConflictContext"/>.</summary>
public sealed class ConflictContextTests
{
    /// <summary>The stream and payload contract used by the counter application.</summary>
    private const string CounterName = "counter";

    /// <summary>The content type used by counter payloads.</summary>
    private const string JsonContentType = "application/json";

    /// <summary>The authenticated client used by the test operations.</summary>
    private const string DeviceClientId = "device";

    /// <summary>The first server commit day offset.</summary>
    private const int FirstCommitDayOffset = 1;

    /// <summary>The second server commit day offset.</summary>
    private const int SecondCommitDayOffset = 2;

    /// <summary>Verifies a caller cannot change the transaction input after construction.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorOwnsOrderedIncomingOperations()
    {
        var payload = new PayloadEnvelope(CounterName, 1, JsonContentType, ReadOnlyMemory<byte>.Empty, "hash");
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
        var client = new ClientIdentity(DeviceClientId);
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
        var state = new ServerState(new(CounterName), "v1", new(CounterName, 1, JsonContentType, ReadOnlyMemory<byte>.Empty, "hash"));

        var constructor = typeof(ConflictContext).GetConstructors().Single(static candidate => candidate.GetParameters().Length == 3);
        var exception = await Assert.That(() => constructor.Invoke([state, null, new ClientIdentity(DeviceClientId)])).ThrowsExactly<TargetInvocationException>();

        await Assert.That(exception?.InnerException).IsTypeOf<ArgumentNullException>();
    }

    /// <summary>Verifies the additive constructor retains trusted server write provenance.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorRetainsTrustedServerWriteProvenance()
    {
        var payload = new PayloadEnvelope(CounterName, 1, JsonContentType, ReadOnlyMemory<byte>.Empty, "hash");
        var operation = new SyncOperation
        {
            OperationId = OperationId.New(),
            StreamId = new(CounterName),
            ClientSequence = 1,
            TimestampUtc = DateTimeOffset.UnixEpoch,
            Type = SyncOperationType.Append,
            Payload = payload,
        };
        var candidateWrite = Stamp(DateTimeOffset.UnixEpoch.AddDays(FirstCommitDayOffset), "server-assigned-client", operation.OperationId);
        var currentWrite = Stamp(DateTimeOffset.UnixEpoch.AddDays(SecondCommitDayOffset), "prior-client", OperationId.New());
        var server = new ConflictServerContext { CandidateWrite = candidateWrite, CurrentWrite = currentWrite };

        var context = new ConflictContext(new(operation.StreamId, "v1", payload), [operation], new("device"), server);

        await Assert.That(context.Server).IsSameReferenceAs(server);
        await Assert.That(context.Server?.CandidateWrite).IsSameReferenceAs(candidateWrite);
        await Assert.That(context.Server?.CurrentWrite).IsSameReferenceAs(currentWrite);
        await Assert.That(context.Server?.CandidateWrite.CommittedAtUtc).IsEqualTo(DateTimeOffset.UnixEpoch.AddDays(FirstCommitDayOffset));
        await Assert.That(context.Server?.CandidateWrite.CommittedAtUtc).IsNotEqualTo(operation.TimestampUtc);
    }

    /// <summary>Verifies the original constructor represents unavailable provenance with null.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task OriginalConstructorLeavesServerProvenanceUnavailable()
    {
        var payload = new PayloadEnvelope(CounterName, 1, JsonContentType, ReadOnlyMemory<byte>.Empty, "hash");
        var state = new ServerState(new(CounterName), "v1", payload);

        var context = new ConflictContext(state, [], new(DeviceClientId));

        await Assert.That(context.Server).IsNull();
    }

    /// <summary>Creates a trusted server write stamp for a test.</summary>
    /// <param name="committedAtUtc">The server-assigned commit timestamp.</param>
    /// <param name="clientId">The authenticated client identifier.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <returns>A trusted server write stamp.</returns>
    private static ConflictWriteStamp Stamp(DateTimeOffset committedAtUtc, string clientId, OperationId operationId) =>
        new() { CommittedAtUtc = committedAtUtc, ClientId = clientId, OperationId = operationId };
}
