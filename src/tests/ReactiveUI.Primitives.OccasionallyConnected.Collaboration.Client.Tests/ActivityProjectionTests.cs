// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Client;

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Client.Tests;

/// <summary>Tests canonical activity projection from remote and resolved effects.</summary>
public sealed class ActivityProjectionTests
{
    /// <summary>The shared accepted status.</summary>
    private const string ReadyStatus = "ready";

    /// <summary>The shared accepted client.</summary>
    private const string AcceptedClient = "client-b";

    /// <summary>The local status before server acceptance.</summary>
    private const string PendingStatus = "pending";

    /// <summary>Verifies a server event replaces optimistic metadata with canonical values.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ApplyRemotePreservesUnpatchedFieldsAndAcceptsServerMetadata()
    {
        var operationId = OperationId.New();
        var oldView = ActivityView.Empty() with { Title = "Existing title", Details = "Old details" };
        var update = new ActivityUpdate
        {
            Status = ReadyStatus,
            Details = "New details",
            DetailsSpecified = true,
            AcceptedClientId = AcceptedClient,
            AcceptedOperationId = operationId.Value.ToString("N"),
            AcceptedVersion = "activity-v2",
        };
        var remoteEvent = CreateEvent(operationId, ActivityPayloadSerializer.Instance.Capture(update));

        var result = ActivityProjection.Instance.ApplyRemote(oldView, update, remoteEvent);

        await Assert.That(result.Status).IsEqualTo(ReadyStatus);
        await Assert.That(result.Title).IsEqualTo("Existing title");
        await Assert.That(result.Details).IsEqualTo("New details");
        await Assert.That(result.AcceptedClientId).IsEqualTo(AcceptedClient);
        await Assert.That(result.AcceptedOperationId).IsEqualTo(operationId.Value.ToString("N"));
        await Assert.That(result.AcceptedVersion).IsEqualTo("activity-v2");
    }

    /// <summary>Verifies a resolved canonical payload restores the merged title and details.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ReconcileUsesResolvedCanonicalPayloadAfterInvalidProducedEvent()
    {
        var operationId = OperationId.New();
        var current = ActivityView.Empty() with { Status = PendingStatus, Title = "Old title" };
        var canonical = ActivityView.Empty() with
        {
            Status = ReadyStatus,
            Title = "Merged title",
            Details = "Merged details",
            AcceptedClientId = AcceptedClient,
            AcceptedOperationId = operationId.Value.ToString("N"),
            AcceptedVersion = "activity-v3",
        };
        var canonicalPayload = ActivityPayloadSerializer.CreateEnvelope(canonical);
        var invalidEvent = CreateEvent(operationId, canonicalPayload with { PayloadHash = "sha256-invalid" });
        var resolved = new ResolvedConflict(operationId, "activity-merged", canonicalPayload);
        var decision = new ConflictResolutionResult(
            [operationId],
            [],
            [resolved],
            [invalidEvent],
            canonical.AcceptedVersion);

        var result = ActivityProjection.Instance.Reconcile(current, decision);

        await Assert.That(result).IsEqualTo(canonical);
    }

    /// <summary>Verifies a produced canonical event takes priority over resolved metadata.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ReconcileUsesProducedCanonicalEvent()
    {
        var operationId = OperationId.New();
        var current = ActivityView.Empty() with { Status = PendingStatus };
        var canonical = current with
        {
            Status = ReadyStatus,
            AcceptedClientId = AcceptedClient,
            AcceptedOperationId = operationId.Value.ToString("N"),
            AcceptedVersion = "activity-v5",
        };
        var canonicalEvent = CreateEvent(operationId, ActivityPayloadSerializer.CreateEnvelope(canonical));
        var decision = new ConflictResolutionResult([operationId], [], [], [canonicalEvent], canonical.AcceptedVersion);

        var result = ActivityProjection.Instance.Reconcile(current, decision);

        await Assert.That(result).IsEqualTo(canonical);
    }

    /// <summary>Verifies an empty decision does not replace current state.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ReconcileWithoutCanonicalPayloadRetainsCurrentView()
    {
        var current = ActivityView.Empty() with { Status = PendingStatus };
        var decision = new ConflictResolutionResult([], [], [], [], current.AcceptedVersion);

        var result = ActivityProjection.Instance.Reconcile(current, decision);

        await Assert.That(result).IsEqualTo(current);
    }

    /// <summary>Verifies invalid and absent resolved payloads cannot replace local state.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ReconcileIgnoresAbsentAndInvalidResolvedPayloads()
    {
        var operationId = OperationId.New();
        var current = ActivityView.Empty() with { Status = PendingStatus };
        var valid = ActivityPayloadSerializer.Instance.Capture(new ActivityUpdate { Status = ReadyStatus });
        var invalid = valid with { PayloadHash = "sha256-invalid" };
        var decision = new ConflictResolutionResult(
            [operationId],
            [],
            [new ResolvedConflict(operationId, "absent", null), new ResolvedConflict(operationId, "invalid", invalid)],
            [],
            current.AcceptedVersion);

        var result = ActivityProjection.Instance.Reconcile(current, decision);

        await Assert.That(result).IsEqualTo(current);
    }

    /// <summary>Verifies missing remote metadata falls back to the last accepted view.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ApplyRemoteUsesFallbackMetadataWhenPatchOmitsIt()
    {
        var operationId = OperationId.New();
        var current = ActivityView.Empty() with { Status = PendingStatus, Title = "Previous title", Details = "Previous details" };
        var update = new ActivityUpdate { Status = ReadyStatus };
        var remoteEvent = CreateEvent(operationId, ActivityPayloadSerializer.Instance.Capture(update));

        var result = ActivityProjection.Instance.ApplyRemote(current, update, remoteEvent);

        await Assert.That(result.Status).IsEqualTo(ReadyStatus);
        await Assert.That(result.Title).IsEqualTo(current.Title);
        await Assert.That(result.Details).IsEqualTo(current.Details);
        await Assert.That(result.AcceptedClientId).IsEqualTo(current.AcceptedClientId);
        await Assert.That(result.AcceptedOperationId).IsEqualTo(current.AcceptedOperationId);
        await Assert.That(result.AcceptedVersion).IsEqualTo(current.AcceptedVersion);
        await Assert.That(result.ServerAcceptedUtc).IsEqualTo(current.ServerAcceptedUtc);
    }

    /// <summary>Creates a remote event carrying one test envelope.</summary>
    /// <param name="operationId">The causing operation.</param>
    /// <param name="payload">The event payload.</param>
    /// <returns>The remote event.</returns>
    private static RemoteEvent CreateEvent(OperationId operationId, PayloadEnvelope payload) =>
        new(
            Guid.NewGuid(),
            ActivityContracts.StreamId,
            "cursor-1",
            DateTimeOffset.UnixEpoch,
            operationId,
            payload,
            new Dictionary<string, string>(StringComparer.Ordinal));
}
