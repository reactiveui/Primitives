// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Http;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>Tests W3C trace context extraction by <see cref="HttpServerEndpoint"/>.</summary>
public sealed partial class HttpServerEndpointTests
{
    /// <summary>The shared diagnostics source name.</summary>
    private const string TraceSourceName = "ReactiveUI.Primitives.OccasionallyConnected";

    /// <summary>The server request activity name.</summary>
    private const string ServerActivityName = "oc.transport.server";

    /// <summary>The W3C trace parent header name.</summary>
    private const string TraceParentHeaderName = "traceparent";

    /// <summary>The W3C trace state header name.</summary>
    private const string TraceStateHeaderName = "tracestate";

    /// <summary>The trace state sent by trace context tests.</summary>
    private const string IncomingTraceState = "vendor=value";

    /// <summary>A route the endpoint does not serve.</summary>
    private const string UnknownRouteUri = "https://example.invalid/unknown";

    /// <summary>The oversized header length used by bounding tests.</summary>
    private const int OversizedHeaderLength = 513;

    /// <summary>Verifies the server activity is parented to a valid incoming trace parent and keeps trace state.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncParentsServerActivityToIncomingTraceParent()
    {
        using var recorder = new ServerActivityRecorder();
        var traceId = ActivityTraceId.CreateRandom();
        var spanId = ActivitySpanId.CreateRandom();
        await using var endpoint = new HttpServerEndpoint(CreateOptions(new RecordingHub()));
        using var request = CreateTracedRequest($"00-{traceId.ToHexString()}-{spanId.ToHexString()}-01", IncomingTraceState);

        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient());

        var activity = recorder.Single(traceId);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.NotFound);
        await Assert.That(activity.ParentSpanId).IsEqualTo(spanId);
        await Assert.That(activity.HasRemoteParent).IsTrue();
        await Assert.That(activity.Kind).IsEqualTo(ActivityKind.Server);
        await Assert.That(activity.TraceStateString).IsEqualTo(IncomingTraceState);
        await Assert.That(activity.TagObjects.Any()).IsFalse();
    }

    /// <summary>Verifies malformed and oversized trace headers are ignored without failing the request.</summary>
    /// <param name="traceParent">The incoming trace parent value, or <see langword="null"/> for an oversized value.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments("not-a-trace-parent")]
    [Arguments("00-00000000000000000000000000000000-0000000000000000-01")]
    [Arguments("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01-")]
    [Arguments("")]
    [Arguments(null)]
    public async Task HandleAsyncIgnoresInvalidTraceParent(string? traceParent)
    {
        using var recorder = new ServerActivityRecorder();
        using var ambient = new Activity("ambient-host-request").SetIdFormat(ActivityIdFormat.W3C).Start();
        await using var endpoint = new HttpServerEndpoint(CreateOptions(new RecordingHub()));
        var oversized = $"00-{ActivityTraceId.CreateRandom().ToHexString()}-{new string('a', OversizedHeaderLength)}-01";
        using var request = CreateTracedRequest(traceParent ?? oversized, traceState: null);

        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient());

        var activity = recorder.Single(ambient.TraceId);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.NotFound);
        await Assert.That(activity.ParentSpanId).IsEqualTo(ambient.SpanId);
        await Assert.That(activity.HasRemoteParent).IsFalse();
    }

    /// <summary>Verifies oversized trace state is dropped while a valid trace parent is kept.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncDropsOversizedTraceState()
    {
        using var recorder = new ServerActivityRecorder();
        var traceId = ActivityTraceId.CreateRandom();
        await using var endpoint = new HttpServerEndpoint(CreateOptions(new RecordingHub()));
        using var request = CreateTracedRequest(
            $"00-{traceId.ToHexString()}-{ActivitySpanId.CreateRandom().ToHexString()}-01",
            $"vendor={new string('a', OversizedHeaderLength)}");

        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient());

        var activity = recorder.Single(traceId);
        await Assert.That(activity.HasRemoteParent).IsTrue();
        await Assert.That(activity.TraceStateString).IsNull();
    }

    /// <summary>Creates a request to an unknown route carrying raw trace headers.</summary>
    /// <param name="traceParent">The raw trace parent value.</param>
    /// <param name="traceState">The optional raw trace state value.</param>
    /// <returns>The request.</returns>
    private static HttpRequestMessage CreateTracedRequest(string traceParent, string? traceState)
    {
        var request = new HttpRequestMessage(HttpMethod.Get, UnknownRouteUri);
        _ = request.Headers.TryAddWithoutValidation(TraceParentHeaderName, traceParent);
        if (traceState is not null)
        {
            _ = request.Headers.TryAddWithoutValidation(TraceStateHeaderName, traceState);
        }

        return request;
    }

    /// <summary>Records stopped server request activities from the shared diagnostics source.</summary>
    private sealed class ServerActivityRecorder : IDisposable
    {
        /// <summary>The recorded activities.</summary>
        private readonly ConcurrentQueue<Activity> _activities = new();

        /// <summary>The registered listener.</summary>
        private readonly ActivityListener _listener;

        /// <summary>Initializes a new instance of the <see cref="ServerActivityRecorder"/> class.</summary>
        internal ServerActivityRecorder()
        {
            _listener = new()
            {
                ShouldListenTo = static source => source.Name == TraceSourceName,
                Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
                ActivityStopped = _activities.Enqueue,
            };
            ActivitySource.AddActivityListener(_listener);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose() => _listener.Dispose();

        /// <summary>Gets the single server activity recorded for a trace.</summary>
        /// <param name="traceId">The trace identifier.</param>
        /// <returns>The recorded activity.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Activity Single(ActivityTraceId traceId) =>
            _activities.Single(activity => activity.TraceId == traceId && activity.OperationName == ServerActivityName);
    }
}
