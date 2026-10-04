// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Net;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>Tests the HTTP remote transport adapter.</summary>
public sealed partial class HttpRemoteTransportAdapterTests
{
    /// <summary>The connect response from a peer that atomically records effects and acknowledgements.</summary>
    private const string AtomicConnectResponseJson = """
        {"protocolVersion":"1.0","features":31,"maximumBatchOperations":10,"maximumBatchBytes":1024,
        "serverIdempotencyRetentionMilliseconds":60000,"clientInboxRetentionRequiredMilliseconds":120000}
        """;

    /// <summary>Verifies an exactly-once connection accepts a peer's atomic effect capability.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ConnectAsyncAcceptsAtomicPeerForExactlyOnce()
    {
        using var httpClient = CreateHttpClient(new RecordingHttpHandler(
            static _ => CreateProtocolResponse(HttpStatusCode.OK, AtomicConnectResponseJson)));
        await using var adapter = CreateAdapter(httpClient);
        var request = new TransportConnectRequest(new(new(1, 0), new(1, 0)), new("client-1", "tenant"), [DeliveryGuarantee.ExactlyOnce]);

        await using var session = await adapter.ConnectAsync(request, CancellationToken.None);

        await Assert.That((session.NegotiatedCapabilities.Features & RemoteTransportCapabilities.AtomicApplyAndAcknowledge) != 0).IsTrue();
        await Assert.That((adapter.Capabilities & RemoteTransportCapabilities.AtomicApplyAndAcknowledge) != 0).IsTrue();
    }

    /// <summary>Verifies idempotency alone cannot establish an exactly-once effect connection.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ConnectAsyncRejectsExactlyOnceWithoutAtomicPeer()
    {
        using var httpClient = CreateHttpClient(new RecordingHttpHandler(
            static _ => CreateProtocolResponse(HttpStatusCode.OK, ConnectResponseJson)));
        await using var adapter = CreateAdapter(httpClient);
        var request = new TransportConnectRequest(new(new(1, 0), new(1, 0)), new("client-1", "tenant"), [DeliveryGuarantee.ExactlyOnce]);

        var exception = await CaptureHttpExceptionAsync(async () =>
        {
            await using var session = await adapter.ConnectAsync(request, CancellationToken.None);
        });

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.SchemaIncompatible);
    }
}
