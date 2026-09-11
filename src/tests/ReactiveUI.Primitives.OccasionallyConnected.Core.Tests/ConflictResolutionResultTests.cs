// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="ConflictResolutionResult"/>.</summary>
public sealed class ConflictResolutionResultTests
{
    /// <summary>Verifies application-owned lists cannot change a prepared canonical decision.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorOwnsAllDecisionCollections()
    {
        var operationId = OperationId.New();
        var payload = new PayloadEnvelope("counter", 1, "application/json", ReadOnlyMemory<byte>.Empty, "hash");
        var remoteEvent = new RemoteEvent(
            Guid.NewGuid(),
            new("counter"),
            "cursor",
            DateTimeOffset.UnixEpoch,
            operationId,
            payload,
            new Dictionary<string, string>());
        var rejected = new RejectedOperation(OperationId.New(), "conflict", false);
        var resolved = new ResolvedConflict(operationId, "merged", payload);
        var accepted = new List<OperationId> { operationId };
        var rejectedOperations = new List<RejectedOperation> { rejected };
        var conflicts = new List<ResolvedConflict> { resolved };
        var events = new List<RemoteEvent> { remoteEvent };
        var result = new ConflictResolutionResult(accepted, rejectedOperations, conflicts, events, "v1");

        accepted.Clear();
        rejectedOperations.Clear();
        conflicts.Clear();
        events.Clear();

        await Assert.That(result.AcceptedOperations).Count().IsEqualTo(1);
        await Assert.That(result.AcceptedOperations[0]).IsEqualTo(operationId);
        await Assert.That(result.RejectedOperations).Count().IsEqualTo(1);
        await Assert.That(result.RejectedOperations[0]).IsSameReferenceAs(rejected);
        await Assert.That(result.Conflicts).Count().IsEqualTo(1);
        await Assert.That(result.Conflicts[0]).IsSameReferenceAs(resolved);
        await Assert.That(result.ProducedEvents).Count().IsEqualTo(1);
        await Assert.That(result.ProducedEvents[0]).IsSameReferenceAs(remoteEvent);
        await Assert.That(result.ServerVersion).IsEqualTo("v1");
    }

    /// <summary>Verifies missing collections cannot silently become empty canonical decisions.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorRejectsMissingDecisionCollections()
    {
        await Assert.That(static () => new ConflictResolutionResult(null!, [], [], [], "v1")).ThrowsExactly<ArgumentNullException>();
        await Assert.That(static () => new ConflictResolutionResult([], null!, [], [], "v1")).ThrowsExactly<ArgumentNullException>();
        await Assert.That(static () => new ConflictResolutionResult([], [], null!, [], "v1")).ThrowsExactly<ArgumentNullException>();
        await Assert.That(static () => new ConflictResolutionResult([], [], [], null!, "v1")).ThrowsExactly<ArgumentNullException>();
    }
}
