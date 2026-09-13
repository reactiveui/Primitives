// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Trusted-principal tests for <see cref="IServerStreamHubExtensions"/>.</summary>
public sealed partial class IServerStreamHubExtensionsTests
{
    /// <summary>Verifies the convenience overloads pass the trusted server principal through unchanged.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ConvenienceOverloadsForwardTrustedServerAuthenticatedClient()
    {
        var hub = new RecordingHub();
        var batch = new SyncBatch(Guid.NewGuid(), []);
        var request = new RemoteSubscribeRequest(new(StreamName), SubscriptionId.New(), null, StartPosition.Latest);
        var acknowledgement = new ReceiveAcknowledgement(request.SubscriptionId, request.StreamId, Cursor);
        var client = new ServerAuthenticatedClient(TenantId, ClientId);

        _ = await hub.ApplyOperationsAsync(batch, client);
        _ = hub.SubscribeStreamAsync(request, client);
        await hub.AcknowledgeAsync(acknowledgement, client);

        await Assert.That(hub.ApplyClient).IsSameReferenceAs(client);
        await Assert.That(hub.SubscribeClient).IsSameReferenceAs(client);
        await Assert.That(hub.AcknowledgeClient).IsSameReferenceAs(client);
    }
}
