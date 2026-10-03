// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Net;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>Tests W3C trace context propagation by the HTTP remote transport adapter.</summary>
public sealed partial class HttpRemoteTransportAdapterTests
{
    /// <summary>The W3C trace parent header name.</summary>
    private const string TraceParentHeaderName = "traceparent";

    /// <summary>The W3C trace state header name.</summary>
    private const string TraceStateHeaderName = "tracestate";

    /// <summary>The trace state carried by the ambient test activity.</summary>
    private const string AmbientTraceState = "vendor=value";

    /// <summary>A caller-supplied trace parent that the adapter must not replace.</summary>
    private const string CallerTraceParent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

    /// <summary>The number of requests sent by a connect followed by one push.</summary>
    private const int TracedRequestCount = 2;

    /// <summary>Verifies connect and push requests carry the current W3C trace context and no payload data.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task RequestsCarryCurrentTraceContext()
    {
        var batch = CreateBatch();
        var handler = CreateTraceRecordingHandler(batch);
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(httpClient);
        using var activity = new Activity("trace-context-test") { TraceStateString = AmbientTraceState }
            .SetIdFormat(ActivityIdFormat.W3C)
            .Start();

        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        _ = await session.PushAsync(batch, CancellationToken.None);

        await Assert.That(handler.Requests.Count).IsEqualTo(TracedRequestCount);
        foreach (var request in handler.Requests)
        {
            await Assert.That(GetSingleHeader(request.Headers, TraceParentHeaderName)).IsEqualTo(activity.Id);
            await Assert.That(GetSingleHeader(request.Headers, TraceStateHeaderName)).IsEqualTo(AmbientTraceState);
            await Assert.That(GetSingleHeader(request.Headers, "baggage")).IsNull();
            await Assert.That(GetSingleHeader(request.Headers, "Correlation-Context")).IsNull();
        }
    }

    /// <summary>Verifies requests carry no trace headers when no activity is current.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task RequestsOmitTraceContextWithoutCurrentActivity()
    {
        Activity.Current = null;
        var batch = CreateBatch();
        var handler = CreateTraceRecordingHandler(batch);
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(httpClient);

        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        _ = await session.PushAsync(batch, CancellationToken.None);

        foreach (var request in handler.Requests)
        {
            await Assert.That(GetSingleHeader(request.Headers, TraceParentHeaderName)).IsNull();
            await Assert.That(GetSingleHeader(request.Headers, TraceStateHeaderName)).IsNull();
        }
    }

    /// <summary>Verifies a caller-supplied trace parent is sent unchanged instead of being duplicated.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task RequestsKeepCallerSuppliedTraceParent()
    {
        var batch = CreateBatch();
        var handler = CreateTraceRecordingHandler(batch);
        using var httpClient = CreateHttpClient(handler);
        _ = httpClient.DefaultRequestHeaders.TryAddWithoutValidation(TraceParentHeaderName, CallerTraceParent);
        await using var adapter = CreateAdapter(httpClient);
        using var activity = new Activity("trace-context-test").SetIdFormat(ActivityIdFormat.W3C).Start();

        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        _ = await session.PushAsync(batch, CancellationToken.None);

        foreach (var request in handler.Requests)
        {
            await Assert.That(GetSingleHeader(request.Headers, TraceParentHeaderName)).IsEqualTo(CallerTraceParent);
        }
    }

    /// <summary>Creates a recording handler that answers connect and push requests.</summary>
    /// <param name="batch">The batch that push acknowledges.</param>
    /// <returns>The recording handler.</returns>
    private static RecordingHttpHandler CreateTraceRecordingHandler(SyncBatch batch) =>
        new(request => request.RequestUri?.AbsolutePath switch
        {
            ConnectRoute => CreateProtocolResponse(HttpStatusCode.OK, ConnectResponseJson),
            PushRoute => CreateProtocolResponse(HttpStatusCode.OK, PushResponseJson(batch)),
            _ => CreateProtocolResponse(HttpStatusCode.NotFound, "{}"),
        });
}
