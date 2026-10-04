// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Snapshot recovery tests for <see cref="LoopbackTransportAdapter"/>.</summary>
public sealed partial class LoopbackTransportAdapterTests
{
    /// <summary>The feature set that advertises snapshot recovery.</summary>
    private const RemoteTransportCapabilities SnapshotRecoveryFeatures = AllFeatures | RemoteTransportCapabilities.SnapshotRecovery;

    /// <summary>The snapshot response byte bound used by recovery requests.</summary>
    private const long SnapshotResponseBytes = 4096;

    /// <summary>Verifies advertising snapshot recovery without a snapshot recovery hub fails closed at construction.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task ConstructorRejectsSnapshotRecoveryWithoutSnapshotRecoveryHub()
    {
        var options = CreateOptions(new RecordingHub()) with { PeerCapabilities = CreateCapabilities(SnapshotRecoveryFeatures) };

        await Assert.That(() => new LoopbackTransportAdapter(options)).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies snapshot recovery reaches the snapshot hub with the trusted principal and returns its result.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task GetSnapshotAsyncDelegatesToSnapshotRecoveryHubWithTrustedPrincipal()
    {
        var snapshotHub = new RecordingSnapshotHub();
        var options = CreateOptions(new RecordingHub()) with
        {
            PeerCapabilities = CreateCapabilities(SnapshotRecoveryFeatures),
            SnapshotRecoveryHub = snapshotHub,
        };
        await using var adapter = new LoopbackTransportAdapter(options);
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var request = CreateSnapshotRequest();

        var result = await ((IRemoteSnapshotRecoverySession)session).GetSnapshotAsync(request, CancellationToken.None);

        await Assert.That(result.Status).IsEqualTo(RemoteSnapshotRecoveryStatus.RetentionExpired);
        await Assert.That(snapshotHub.Request).IsEqualTo(request);
        await Assert.That(snapshotHub.Client?.TenantId).IsEqualTo(TrustedTenant);
        await Assert.That(snapshotHub.Client?.ClientId).IsEqualTo(TrustedClientId);
    }

    /// <summary>Verifies a session whose peer does not advertise snapshot recovery refuses snapshot requests.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task GetSnapshotAsyncRejectsWhenSnapshotRecoveryIsNotAdvertised()
    {
        var snapshotHub = new RecordingSnapshotHub();
        await using var adapter = new LoopbackTransportAdapter(CreateOptions(new RecordingHub()) with { SnapshotRecoveryHub = snapshotHub });
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);

        await Assert.That(async () => await ((IRemoteSnapshotRecoverySession)session).GetSnapshotAsync(CreateSnapshotRequest(), CancellationToken.None))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(snapshotHub.Request).IsNull();
    }

    /// <summary>Creates a minimal snapshot recovery request.</summary>
    /// <returns>The snapshot recovery request.</returns>
    private static RemoteSnapshotRecoveryRequest CreateSnapshotRequest() =>
        new()
        {
            StreamId = new("orders"),
            SubscriptionId = SubscriptionId.New(),
            ExpiredCursor = "cursor-1",
            ClientStateContractId = "state",
            ClientStateSchemaVersion = 1,
            SnapshotFormatVersion = 1,
            PendingOperations = [],
            MaximumResponseBytes = SnapshotResponseBytes,
        };

    /// <summary>Records snapshot recovery calls and answers with a retention-expired result.</summary>
    private sealed class RecordingSnapshotHub : IServerSnapshotRecoveryHub
    {
        /// <summary>Gets the last snapshot request.</summary>
        public RemoteSnapshotRecoveryRequest? Request { get; private set; }

        /// <summary>Gets the last snapshot client identity.</summary>
        public ServerAuthenticatedClient? Client { get; private set; }

        /// <inheritdoc/>
        public ValueTask<RemoteSnapshotRecoveryResult> GetSnapshotAsync(
            RemoteSnapshotRecoveryRequest request,
            ServerAuthenticatedClient client,
            CancellationToken cancellationToken)
        {
            Request = request;
            Client = client;
            return new(new RemoteSnapshotRecoveryResult { Status = RemoteSnapshotRecoveryStatus.RetentionExpired });
        }
    }
}
