// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Net;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>Tests exact negotiated HTTP batch byte admission.</summary>
public sealed partial class HttpRemoteTransportAdapterTests
{
    /// <summary>Verifies negotiation includes framing and metadata, accepts equality, and rejects one byte less before sending.</summary>
    /// <param name="byteAdjustment">The adjustment from the measured request body size.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments(-1)]
    [Arguments(0)]
    public async Task PreparePushAsyncEnforcesExactNegotiatedEncodedBytes(int byteAdjustment)
    {
        var batch = CreateBatch();
        long negotiatedBytes = MetadataValueBytes;
        var handler = new RecordingHttpHandler(request => CreateProtocolResponse(HttpStatusCode.OK, $$"""
            {"protocolVersion":"1.0","features":15,"maximumBatchOperations":10,"maximumBatchBytes":{{negotiatedBytes}},
            "serverIdempotencyRetentionMilliseconds":60000,"clientInboxRetentionRequiredMilliseconds":120000}
            """));
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(httpClient);
        await using (var initialSession = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None))
        {
            await using var measured = await ((IRemoteTransportBatchPreparer)initialSession).PreparePushAsync(batch, CancellationToken.None);
            negotiatedBytes = measured.EncodedSizeBytes + byteAdjustment;
            await Assert.That(negotiatedBytes).IsGreaterThan(batch.Operations[0].Payload.PayloadLength);
        }

        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var preparer = (IRemoteTransportBatchPreparer)session;
        if (byteAdjustment < 0)
        {
            var failure = await CaptureHttpExceptionAsync(async () =>
            {
                await using var rejected = await preparer.PreparePushAsync(batch, CancellationToken.None);
            });
            await Assert.That(failure.Kind).IsEqualTo(HttpTransportFailureKind.PayloadTooLarge);
        }
        else
        {
            await using var accepted = await preparer.PreparePushAsync(batch, CancellationToken.None);
            await Assert.That(accepted.EncodedSizeBytes).IsEqualTo(negotiatedBytes);
        }

        await Assert.That(handler.Requests.Count).IsEqualTo(SecondSequence);
    }
}
