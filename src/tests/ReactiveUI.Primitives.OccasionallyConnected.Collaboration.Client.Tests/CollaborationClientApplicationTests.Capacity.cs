// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Client;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Client.Tests;

/// <content>Offline queue capacity tests for <see cref="CollaborationClientApplication"/>.</content>
public sealed partial class CollaborationClientApplicationTests
{
    /// <summary>The status used by the publish canceled at capacity.</summary>
    private const string CapacityCanceledStatus = "capacity-canceled";

    /// <summary>The prefix used by capacity-fill statuses.</summary>
    private const string CapacityStatusPrefix = "capacity-";

    /// <summary>The bounded delay used to prove an at-capacity publish is blocked before cancellation.</summary>
    private const int CapacityPendingProbeMilliseconds = 100;

    /// <summary>Verifies canceled publish-at-capacity leaves SQLite state unchanged.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task OfflinePublishCanceledAtDurableCapacityDoesNotAddDurableOrOptimisticEntry()
    {
        using var lease = new CollaborationClientDatabaseLease();
        var serverUri = new Uri("http://127.0.0.1:0");
        var capacity = GetExpectedDurableOutboxCapacity();
        SubscriptionId subscriptionId;
        await using (var client = await OpenClientAsync(serverUri, lease.ClientAPath, TokenA, ClientA)
                         .ConfigureAwait(false))
        {
            subscriptionId = await FillOfflineOutboxToCapacityAsync(client, capacity).ConfigureAwait(false);
        }

        var before = await ReadActualClientOutboxAsync(lease.ClientAPath, subscriptionId).ConfigureAwait(false);
        await AssertOutboxAtCapacityAsync(before, subscriptionId, capacity).ConfigureAwait(false);

        CapacityCompletionProof? completion = null;
        var localCanceledCount = 0;
        await using (var client = await OpenClientAsync(serverUri, lease.ClientAPath, TokenA, ClientA)
                         .ConfigureAwait(false))
        {
            using var telemetry = new ActivityTelemetry(client.Activity);
            await InitializeRecoveredActivityAsync(client, telemetry, capacity).ConfigureAwait(false);
            using var cancellation = new CancellationTokenSource();
            var publish = client
                .PublishAsync(CreateCapacityUpdate(CapacityCanceledStatus), cancellation.Token)
                .AsTask();
            try
            {
                completion = await AssertPublishRemainsPendingAsync(publish).ConfigureAwait(false);
            }
            finally
            {
                if (!publish.IsCompleted)
                {
                    await cancellation.CancelAsync().ConfigureAwait(false);
                }
            }

            completion ??= await ObservePublishCancellationAsync(publish).ConfigureAwait(false);
            localCanceledCount = telemetry.Local.Count(IsCapacityCanceledView);
            await AssertNoTerminalStreamFailuresAsync(telemetry).ConfigureAwait(false);
        }

        var after = await ReadActualClientOutboxAsync(lease.ClientAPath, subscriptionId).ConfigureAwait(false);
        var completionContext = CreateCompletedCapacityPublishContext(completion, before, after);
        await AssertOutboxUnchangedAsync(before, after, completionContext).ConfigureAwait(false);
        await Assert.That(localCanceledCount).IsEqualTo(0).Because(completionContext);
        await Assert.That(completion).IsNull().Because(completionContext);
    }

    /// <summary>Initializes only the activity stream and waits for recovered local state.</summary>
    /// <param name="client">The client.</param>
    /// <param name="telemetry">The client telemetry.</param>
    /// <param name="capacity">The expected durable capacity.</param>
    /// <returns>The assertion task.</returns>
    private static async Task InitializeRecoveredActivityAsync(
        CollaborationClientSession client,
        ActivityTelemetry telemetry,
        int capacity)
    {
        var started = false;
        using var startCancellation = CreateWaitCancellation();
        try
        {
            await client.Activity.StartAsync(startCancellation.Token).ConfigureAwait(false);
            started = true;
            _ = await telemetry.Local
                .WaitForAsync(value => IsRecoveredCapacityView(value, capacity), WaitTimeout)
                .ConfigureAwait(false);
        }
        finally
        {
            if (started)
            {
                using var stopCancellation = CreateWaitCancellation();
                await client.Activity.StopAsync(stopCancellation.Token).ConfigureAwait(false);
            }
        }
    }

    /// <summary>Determines whether a recovered view contains the last capacity-fill status.</summary>
    /// <param name="value">The recovered view.</param>
    /// <param name="capacity">The expected durable capacity.</param>
    /// <returns>Whether the view is the recovered capacity view.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool IsRecoveredCapacityView(ActivityView value, int capacity) =>
        string.Equals(value.Status, CreateCapacityStatus(capacity - 1), StringComparison.Ordinal);

    /// <summary>Publishes unique offline entries until the configured durable outbox capacity is reached.</summary>
    /// <param name="client">The client.</param>
    /// <param name="capacity">The durable outbox capacity.</param>
    /// <returns>The durable subscription identity.</returns>
    private static async Task<SubscriptionId> FillOfflineOutboxToCapacityAsync(
        CollaborationClientSession client,
        int capacity)
    {
        for (var index = 0; index < capacity; index++)
        {
            using var cancellation = CreateWaitCancellation();
            _ = await client.PublishAsync(CreateCapacityUpdate(index), cancellation.Token).ConfigureAwait(false);
        }

        return client.Activity.SubscriptionId;
    }

    /// <summary>Checks whether the capacity publish remains blocked during the bounded probe.</summary>
    /// <param name="publish">The at-capacity publish task.</param>
    /// <returns>The completion proof when the publish completed during the probe.</returns>
    private static async Task<CapacityCompletionProof?> AssertPublishRemainsPendingAsync(Task<PublishReceipt> publish)
    {
        await Task.Delay(TimeSpan.FromMilliseconds(CapacityPendingProbeMilliseconds)).ConfigureAwait(false);
        return publish.IsCompleted
            ? new(await ReadCompletedPublishResultAsync(publish).ConfigureAwait(false))
            : null;
    }

    /// <summary>Observes the publish after cancellation without throwing before durable proof is captured.</summary>
    /// <param name="publish">The canceled publish task.</param>
    /// <returns>The completion proof when the publish did not observe cancellation.</returns>
    private static async Task<CapacityCompletionProof?> ObservePublishCancellationAsync(Task<PublishReceipt> publish)
    {
        try
        {
            var receipt = await publish.WaitAsync(WaitTimeout).ConfigureAwait(false);
            return new($"receipt:{receipt.OperationId.Value:N}");
        }
        catch (OperationCanceledException)
        {
            return null;
        }
        catch (Exception exception)
        {
            return new($"fault:{exception.GetType().Name}");
        }
    }

    /// <summary>Reads the actual client outbox from the public SQLite adapter.</summary>
    /// <param name="databasePath">The client database path.</param>
    /// <param name="subscriptionId">The expected durable subscription id.</param>
    /// <returns>The recovered outbox proof.</returns>
    private static async Task<OutboxProof> ReadActualClientOutboxAsync(
        string databasePath,
        SubscriptionId subscriptionId)
    {
        await using var store = new SqliteLocalStoreAdapter(databasePath);
        await store.InitializeAsync(CreateStoreInitialization(ClientA), CancellationToken.None).ConfigureAwait(false);
        var recovered = await store
            .RecoverStreamAsync(ActivityContracts.StreamId, subscriptionId, CancellationToken.None)
            .ConfigureAwait(false);
        return new(
            recovered.SubscriptionId,
            recovered.PendingOperations,
            recovered.NextClientSequence,
            recovered.Snapshot?.Revision ?? 0,
            recovered.Snapshot?.State);
    }

    /// <summary>Asserts recovered public SQLite state is filled exactly to durable outbox capacity.</summary>
    /// <param name="proof">The recovered outbox proof.</param>
    /// <param name="subscriptionId">The expected subscription id.</param>
    /// <param name="capacity">The durable outbox capacity.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertOutboxAtCapacityAsync(
        OutboxProof proof,
        SubscriptionId subscriptionId,
        int capacity)
    {
        await Assert.That(proof.SubscriptionId).IsEqualTo(subscriptionId);
        await Assert.That(proof.PendingOperations.Count).IsEqualTo(capacity);
        await Assert.That(proof.NextClientSequence).IsEqualTo(capacity + 1);
        await Assert.That(proof.SnapshotRevision).IsEqualTo(capacity);
        await AssertStoredCapacityOperationsAsync(proof.PendingOperations, capacity).ConfigureAwait(false);
        await AssertSnapshotStatusAsync(proof, capacity - 1).ConfigureAwait(false);
    }

    /// <summary>Asserts recovered public SQLite state was not changed by the canceled publish.</summary>
    /// <param name="before">The state before cancellation.</param>
    /// <param name="after">The state after cancellation.</param>
    /// <param name="context">The redacted completion context.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertOutboxUnchangedAsync(
        OutboxProof before,
        OutboxProof after,
        string context)
    {
        await Assert.That(after.SubscriptionId).IsEqualTo(before.SubscriptionId).Because(context);
        await Assert.That(after.PendingOperations.Count).IsEqualTo(before.PendingOperations.Count).Because(context);
        await Assert.That(after.NextClientSequence).IsEqualTo(before.NextClientSequence).Because(context);
        await Assert.That(after.SnapshotRevision).IsEqualTo(before.SnapshotRevision).Because(context);
        await AssertPayloadEnvelopeUnchangedAsync(before.SnapshotState, after.SnapshotState).ConfigureAwait(false);
        await AssertPendingOperationsUnchangedAsync(
                before.PendingOperations,
                after.PendingOperations)
            .ConfigureAwait(false);
    }

    /// <summary>Asserts the recovered pending operations are the durable capacity-fill operations.</summary>
    /// <param name="operations">The recovered pending operations.</param>
    /// <param name="capacity">The durable outbox capacity.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertStoredCapacityOperationsAsync(
        IReadOnlyList<SyncOperation> operations,
        int capacity)
    {
        for (var index = 0; index < capacity; index++)
        {
            await Assert.That(operations[index].ClientSequence).IsEqualTo(index + 1);
            await AssertOperationStatusAsync(operations[index], CreateCapacityStatus(index)).ConfigureAwait(false);
        }
    }

    /// <summary>Asserts pending operation identity and payloads are unchanged.</summary>
    /// <param name="before">The operations before cancellation.</param>
    /// <param name="after">The operations after cancellation.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertPendingOperationsUnchangedAsync(
        IReadOnlyList<SyncOperation> before,
        IReadOnlyList<SyncOperation> after)
    {
        for (var index = 0; index < before.Count; index++)
        {
            await Assert.That(after[index].OperationId).IsEqualTo(before[index].OperationId);
            await Assert.That(after[index].ClientSequence).IsEqualTo(before[index].ClientSequence);
            await AssertPayloadEnvelopeUnchangedAsync(before[index].Payload, after[index].Payload)
                .ConfigureAwait(false);
        }
    }

    /// <summary>Asserts an operation stores the expected activity status.</summary>
    /// <param name="operation">The recovered operation.</param>
    /// <param name="status">The expected status.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertOperationStatusAsync(SyncOperation operation, string status)
    {
        var update = ActivityPayloadSerializer.ReadUpdate(operation.Payload);
        await Assert.That(update.Status).IsEqualTo(status);
    }

    /// <summary>Asserts the recovered snapshot stores the expected activity status.</summary>
    /// <param name="proof">The recovered outbox proof.</param>
    /// <param name="index">The expected capacity-fill index.</param>
    /// <returns>The assertion task.</returns>
    /// <exception cref="InvalidOperationException">The recovered snapshot state is missing.</exception>
    private static async Task AssertSnapshotStatusAsync(OutboxProof proof, int index)
    {
        var snapshot = proof.SnapshotState
            ?? throw new InvalidOperationException("The recovered snapshot state is missing.");
        var update = ActivityPayloadSerializer.ReadUpdate(snapshot);
        await Assert.That(update.Status).IsEqualTo(CreateCapacityStatus(index));
    }

    /// <summary>Asserts two payload envelopes have identical metadata and bytes.</summary>
    /// <param name="before">The payload before cancellation.</param>
    /// <param name="after">The payload after cancellation.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertPayloadEnvelopeUnchangedAsync(PayloadEnvelope? before, PayloadEnvelope? after)
    {
        await Assert.That(after is null).IsEqualTo(before is null);
        if (before is null || after is null)
        {
            return;
        }

        await Assert.That(after.ContractId).IsEqualTo(before.ContractId);
        await Assert.That(after.SchemaVersion).IsEqualTo(before.SchemaVersion);
        await Assert.That(after.ContentType).IsEqualTo(before.ContentType);
        await Assert.That(after.PayloadHash).IsEqualTo(before.PayloadHash);
        await Assert.That(after.PayloadLength).IsEqualTo(before.PayloadLength);
        await Assert.That(after.Payload.Span.SequenceEqual(before.Payload.Span)).IsTrue();
    }

    /// <summary>Gets the durable outbox capacity used by the collaboration application builder.</summary>
    /// <returns>The public durable outbox capacity.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int GetExpectedDurableOutboxCapacity() =>
        OccasionallyConnectedOptions.Default.Outbox.MaxOperations;

    /// <summary>Creates a capacity-fill update.</summary>
    /// <param name="index">The fill index.</param>
    /// <returns>The update.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ActivityUpdate CreateCapacityUpdate(int index) =>
        CreateCapacityUpdate(CreateCapacityStatus(index));

    /// <summary>Creates a capacity test update.</summary>
    /// <param name="status">The update status.</param>
    /// <returns>The update.</returns>
    private static ActivityUpdate CreateCapacityUpdate(string status) =>
        new() { Status = status };

    /// <summary>Creates the status for a capacity-fill update.</summary>
    /// <param name="index">The fill index.</param>
    /// <returns>The status.</returns>
    private static string CreateCapacityStatus(int index) =>
        CapacityStatusPrefix + index.ToString(CultureInfo.InvariantCulture);

    /// <summary>Determines whether a local view came from the canceled capacity publish.</summary>
    /// <param name="value">The local view.</param>
    /// <returns>Whether the view came from the canceled capacity publish.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool IsCapacityCanceledView(ActivityView value) =>
        string.Equals(value.Status, CapacityCanceledStatus, StringComparison.Ordinal);

    /// <summary>Creates redacted diagnostics when a capacity publish completes unexpectedly.</summary>
    /// <param name="completion">The completed publish proof.</param>
    /// <param name="before">The outbox proof captured before the publish.</param>
    /// <param name="after">The outbox proof captured after disposing the application owner.</param>
    /// <returns>The diagnostic context.</returns>
    private static string CreateCompletedCapacityPublishContext(
        CapacityCompletionProof? completion,
        OutboxProof before,
        OutboxProof after) =>
        completion is null
            ? "Publish remained pending during the capacity probe."
            : string.Concat(
                "Publish completed during capacity probe; result=",
                completion.Result,
                "; beforePending=",
                before.PendingOperations.Count.ToString(CultureInfo.InvariantCulture),
                "; afterPending=",
                after.PendingOperations.Count.ToString(CultureInfo.InvariantCulture),
                "; beforeNextSequence=",
                before.NextClientSequence.ToString(CultureInfo.InvariantCulture),
                "; afterNextSequence=",
                after.NextClientSequence.ToString(CultureInfo.InvariantCulture));

    /// <summary>Reads the result of a completed publish task without exposing exception messages.</summary>
    /// <param name="publish">The completed publish task.</param>
    /// <returns>The redacted completion result.</returns>
    private static async Task<string> ReadCompletedPublishResultAsync(Task<PublishReceipt> publish)
    {
        try
        {
            var receipt = await publish.ConfigureAwait(false);
            return $"receipt:{receipt.OperationId.Value:N}";
        }
        catch (OperationCanceledException)
        {
            return "canceled";
        }
        catch (Exception exception)
        {
            return $"fault:{exception.GetType().Name}";
        }
    }

    /// <summary>Captures a completed capacity publish without exposing payload or exception text.</summary>
    /// <param name="Result">The redacted publish completion result.</param>
    private sealed record CapacityCompletionProof(string Result);

    /// <summary>Describes the actual recovered client outbox state.</summary>
    /// <param name="SubscriptionId">The durable subscription id.</param>
    /// <param name="PendingOperations">The recovered pending operations.</param>
    /// <param name="NextClientSequence">The next client sequence.</param>
    /// <param name="SnapshotRevision">The recovered snapshot revision.</param>
    /// <param name="SnapshotState">The recovered snapshot state payload.</param>
    private sealed record OutboxProof(
        SubscriptionId SubscriptionId,
        IReadOnlyList<SyncOperation> PendingOperations,
        long NextClientSequence,
        long SnapshotRevision,
        PayloadEnvelope? SnapshotState);
}
