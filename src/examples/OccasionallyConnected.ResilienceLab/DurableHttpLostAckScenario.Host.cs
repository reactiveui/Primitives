// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Net;
using System.Net.Http;
using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Primitives;
using ReactiveUI.Primitives.OccasionallyConnected.Crdt;
using ReactiveUI.Primitives.OccasionallyConnected.Server;
using ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

/// <summary>Runs the durable HTTP lost acknowledgement recovery scenario.</summary>
internal static partial class DurableHttpLostAckScenario
{
    /// <summary>Hosts the portable HTTP endpoint on real ASP.NET/Kestrel loopback HTTP.</summary>
    internal sealed class DurableHttpLostAckHost : IAsyncDisposable
    {
        /// <summary>The maximum request body captured before endpoint dispatch.</summary>
        private const int HostMaximumRequestBytes = 8192;

        /// <summary>The request body read buffer size.</summary>
        private const int HostBodyReadBufferBytes = 1024;

        /// <summary>The first push committed.</summary>
        private readonly TaskCompletionSource<object?> _firstPushCommitted = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>The first push response aborted signal.</summary>
        private readonly TaskCompletionSource<object?> _firstPushResponseAbortedSignal = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>The release first push response.</summary>
        private readonly TaskCompletionSource<object?> _releaseFirstPushResponse = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>The retry push operation id.</summary>
        private readonly TaskCompletionSource<OperationId> _retryPushOperationId = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>The retry push endpoint response.</summary>
        private readonly TaskCompletionSource<int> _retryPushResponse = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>The release subscribe responses.</summary>
        private readonly TaskCompletionSource<object?> _releaseSubscribeResponses = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>The observer subscribe observed.</summary>
        private readonly TaskCompletionSource<object?> _observerSubscribeObserved = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>The reopened subscribe observed.</summary>
        private readonly TaskCompletionSource<object?> _reopenedSubscribeObserved = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>The application.</summary>
        private readonly WebApplication _application;

        /// <summary>The endpoint.</summary>
        private readonly HttpServerEndpoint _endpoint;

        /// <summary>The hub.</summary>
        private readonly ServerStreamHub _hub;

        /// <summary>The base address.</summary>
        private Uri? _baseAddress;

        /// <summary>The withheld first push.</summary>
        private int _withheldFirstPush;

        /// <summary>The first push response aborted.</summary>
        private int _firstPushResponseAborted;

        /// <summary>The push request count.</summary>
        private int _pushRequestCount;

        /// <summary>Initializes a new instance of the <see cref="DurableHttpLostAckHost"/> class.</summary>
        /// <param name="application">The application.</param>
        /// <param name="endpoint">The endpoint.</param>
        /// <param name="hub">The hub.</param>
        private DurableHttpLostAckHost(WebApplication application, HttpServerEndpoint endpoint, ServerStreamHub hub)
        {
            _application = application;
            _endpoint = endpoint;
            _hub = hub;
        }

        /// <summary>Gets the HTTP base address.</summary>
        internal Uri BaseAddress => _baseAddress ?? throw new InvalidOperationException("The host has not started.");

        /// <summary>Gets whether the first successful push response was aborted.</summary>
        internal bool FirstPushResponseAborted => Volatile.Read(ref _firstPushResponseAborted) != 0;

        /// <summary>Gets the observed push request count.</summary>
        internal int PushRequestCount => Volatile.Read(ref _pushRequestCount);

        /// <summary>Starts the host.</summary>
        /// <param name="databasePath">The database path.</param>
        /// <param name="timeProvider">The time provider.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The result.</returns>
        /// <exception cref="InvalidOperationException">Host startup or startup cleanup fails.</exception>
        internal static async ValueTask<DurableHttpLostAckHost> StartAsync(
            string databasePath,
            TimeProvider timeProvider,
            CancellationToken cancellationToken)
        {
            ServerStreamHub? hub = null;
            HttpServerEndpoint? endpoint = null;
            WebApplication? app = null;
            try
            {
                hub = ServerStreamHub.CreateSqlite(databasePath, CreateHubOptions(timeProvider));
                endpoint = new(CreateEndpointOptions(hub, timeProvider));
                var builder = WebApplication.CreateBuilder(new WebApplicationOptions { Args = [] });
                _ = builder.Logging.ClearProviders();
                _ = builder.WebHost.ConfigureKestrel(static options => options.Listen(IPAddress.Loopback, 0));
                app = builder.Build();
                var host = new DurableHttpLostAckHost(app, endpoint, hub);
                app.Run(host.HandleAsync);
                await app.StartAsync(cancellationToken).ConfigureAwait(false);
                host._baseAddress = ResolveBaseAddress(app);
                return host;
            }
            catch (Exception startFailure)
            {
                try
                {
                    await DisposeStartupResourcesAsync(app, endpoint, hub).ConfigureAwait(false);
                }
                catch (Exception cleanupFailure)
                {
                    throw new InvalidOperationException(
                        "The durable HTTP lost-ACK host failed to start and startup cleanup failed.",
                        new AggregateException(startFailure, cleanupFailure));
                }

                throw;
            }
        }

        /// <summary>Creates endpoint options.</summary>
        /// <param name="hub">The durable server stream hub.</param>
        /// <param name="timeProvider">The shared deterministic clock.</param>
        /// <returns>The endpoint options.</returns>
        internal static HttpServerEndpointOptions CreateEndpointOptions(IServerStreamHub hub, TimeProvider timeProvider) =>
            new()
            {
                Hub = hub,
                DeclaredCapabilities = CreateCapabilities(),
                ReplayAuthorizer = LabReplayAuthorizer.Instance,
                ReplayProtection = CreateReplayProtection(timeProvider),
                MaximumBatchOperations = SmallCapacity,
                MaximumEventsPerBatch = SmallCapacity,
                MaximumCompletedOperationsPerBatch = SmallCapacity,
                MaximumPayloadBytes = PayloadBytes,
                MaximumRequestBytes = PayloadBytes,
                MaximumResponseBytes = PayloadBytes,
                LongPollTimeout = OperationTimeout,
                TimeProvider = timeProvider,
            };

        /// <summary>Creates replay protection settings bound to the shared deterministic clock.</summary>
        /// <param name="timeProvider">The shared deterministic clock.</param>
        /// <returns>The replay protection options.</returns>
        internal static HttpReplayProtectionOptions CreateReplayProtection(TimeProvider timeProvider) =>
            new() { TimeProvider = timeProvider };

        /// <summary>Creates server hub options.</summary>
        /// <param name="timeProvider">The time provider.</param>
        /// <returns>The result.</returns>
        internal static ServerStreamHubOptions CreateHubOptions(TimeProvider timeProvider) =>
            new()
            {
                AuthorizationPolicy = new LabAuthorizationPolicy(TenantId),
                ConflictHandler = new()
                {
                    Streams = [CrdtServerStreamRegistration.Create(new() { StreamId = Stream, Kind = CrdtKind.GCounter, Bounds = CreateBounds() })],
                    MaximumProducedEvents = SmallCapacity,
                },
                TimeProvider = timeProvider,
                MaximumBatchOperations = SmallCapacity,
                MaximumBatchLogicalBytes = PayloadBytes,
                MaximumReceiveGroups = JournalCapacity,
                MaximumReceiveEvents = SmallCapacity,
                MaximumReceiveLogicalBytes = PayloadBytes,
                EmptyPollDelay = OperationTimeout,
                JournalLimits = new()
                {
                    MaximumStreams = JournalStreams,
                    MaximumLedgerEntries = JournalCapacity,
                    MaximumEvents = JournalCapacity,
                    MaximumLogicalBytes = JournalBytes,
                    MaximumOperationCaptureCount = JournalCapacity,
                    MaximumEntryEventCount = SmallCapacity,
                    MaximumSubscriptions = SmallCapacity,
                    MaximumSubscriptionOffers = SmallCapacity,
                    OperationRetention = TimeSpan.FromMinutes(RetentionMinutes),
                    SubscriptionRetention = TimeSpan.FromMinutes(RetentionMinutes),
                },
            };

        /// <summary>Creates negotiated HTTP capabilities.</summary>
        /// <returns>The result.</returns>
        internal static NegotiatedCapabilities CreateCapabilities() =>
            new(
                new(1, 0),
                RemoteTransportCapabilities.BatchPush
                    | RemoteTransportCapabilities.CursorResume
                    | RemoteTransportCapabilities.ReceiveAcknowledgements
                    | RemoteTransportCapabilities.ServerIdempotency
                    | RemoteTransportCapabilities.AtomicApplyAndAcknowledge,
                SmallCapacity,
                PayloadBytes,
                TimeSpan.FromMinutes(RetentionMinutes),
                TimeSpan.FromMinutes(RetentionMinutes));

        /// <summary>Tries to resolve the authenticated server principal for one lab request.</summary>
        /// <param name="request">The request.</param>
        /// <param name="client">The client.</param>
        /// <param name="credential">The credential.</param>
        /// <returns>The result.</returns>
        internal static bool TryResolveAuthenticatedClient(
            HttpRequest request,
            out ServerAuthenticatedClient client,
            out string credential)
        {
            if (request.Headers.TryGetValue(LabCredentialHeader, out var values))
            {
                for (var index = 0; index < values.Count; index++)
                {
                    if (TryMapCredential(values[index], out client, out credential))
                    {
                        return true;
                    }
                }
            }

            client = new(string.Empty, string.Empty);
            credential = string.Empty;
            return false;
        }

        /// <summary>Tries to map a lab credential to an authenticated client.</summary>
        /// <param name="candidate">The candidate.</param>
        /// <param name="client">The client.</param>
        /// <param name="credential">The credential.</param>
        /// <returns>The result.</returns>
        internal static bool TryMapCredential(
            string? candidate,
            out ServerAuthenticatedClient client,
            out string credential)
        {
            if (string.Equals(candidate, FirstCredential, StringComparison.Ordinal)
                || string.Equals(candidate, ReopenedCredential, StringComparison.Ordinal))
            {
                client = new(TenantId, WriterClientId);
                credential = string.Equals(candidate, FirstCredential, StringComparison.Ordinal) ? FirstCredential : ReopenedCredential;
                return true;
            }

            if (string.Equals(candidate, ObserverCredential, StringComparison.Ordinal))
            {
                client = new(TenantId, ObserverClientId);
                credential = ObserverCredential;
                return true;
            }

            client = new(string.Empty, string.Empty);
            credential = string.Empty;
            return false;
        }

        /// <summary>Resolves the bound Kestrel base address.</summary>
        /// <param name="application">The application.</param>
        /// <returns>The result.</returns>
        /// <exception cref="InvalidOperationException">Kestrel does not expose its bound address.</exception>
        internal static Uri ResolveBaseAddress(WebApplication application)
        {
            var feature = application.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>();
            if (feature is not null)
            {
                foreach (var address in feature.Addresses)
                {
                    if (!string.IsNullOrWhiteSpace(address))
                    {
                        return new(EnsureTrailingSlash(address));
                    }
                }
            }

            throw new InvalidOperationException("Kestrel did not publish a bound address.");
        }

        /// <summary>Ensures a URI string ends with a slash.</summary>
        /// <param name="address">The address.</param>
        /// <returns>The result.</returns>
        internal static string EnsureTrailingSlash(string address) =>
            address.EndsWith('/') ? address : $"{address}/";

        /// <summary>Determines whether a request targets push.</summary>
        /// <param name="request">The request.</param>
        /// <returns>The result.</returns>
        internal static bool IsPush(HttpRequest request) =>
            string.Equals(request.Method, HttpMethods.Post, StringComparison.Ordinal)
            && string.Equals(request.Path.Value, "/push", StringComparison.Ordinal);

        /// <summary>Determines whether a request targets subscribe.</summary>
        /// <param name="request">The request.</param>
        /// <returns>The result.</returns>
        internal static bool IsSubscribe(HttpRequest request) =>
            string.Equals(request.Method, HttpMethods.Get, StringComparison.Ordinal)
            && string.Equals(request.Path.Value, "/subscribe", StringComparison.Ordinal);

        /// <summary>Creates a portable request for the endpoint from a bounded in-memory body.</summary>
        /// <param name="request">The ASP.NET request.</param>
        /// <param name="isPush">A value indicating whether the request targets push.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The captured portable request.</returns>
        internal static async ValueTask<CapturedPortableRequest> CreatePortableRequestAsync(
            HttpRequest request,
            bool isPush,
            CancellationToken cancellationToken)
        {
            var target = request.GetEncodedUrl();
            var body = await ReadBoundedRequestBodyAsync(request, cancellationToken).ConfigureAwait(false);
            var portable = new HttpRequestMessage(new HttpMethod(request.Method), target);
            CopyRequestHeaders(request, portable);
            if (!string.Equals(request.Method, HttpMethods.Get, StringComparison.Ordinal))
            {
                portable.Content = new ByteArrayContent(body);
                if (!string.IsNullOrWhiteSpace(request.ContentType))
                {
                    _ = portable.Content.Headers.TryAddWithoutValidation("Content-Type", request.ContentType);
                }

                portable.Content.Headers.ContentLength = body.Length;
            }

            return new(portable, isPush ? TryReadFirstPushOperationId(body) : null);
        }

        /// <summary>Reads one ASP.NET request body while enforcing the host request limit.</summary>
        /// <param name="request">The ASP.NET request.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The bounded request body.</returns>
        /// <exception cref="InvalidOperationException">The body exceeds the lab request limit.</exception>
        internal static async ValueTask<byte[]> ReadBoundedRequestBodyAsync(HttpRequest request, CancellationToken cancellationToken)
        {
            if (request.ContentLength is > HostMaximumRequestBytes)
            {
                throw new InvalidOperationException("The durable HTTP lost-ACK request body exceeds the lab host limit.");
            }

            await using var body = new MemoryStream();
            var buffer = new byte[HostBodyReadBufferBytes];
            while (true)
            {
                var read = await request.Body.ReadAsync(buffer.AsMemory(), cancellationToken).ConfigureAwait(false);
                if (read == 0)
                {
                    return body.ToArray();
                }

                if (body.Length + read > HostMaximumRequestBytes)
                {
                    throw new InvalidOperationException("The durable HTTP lost-ACK request body exceeds the lab host limit.");
                }

                await body.WriteAsync(buffer.AsMemory(0, read), cancellationToken).ConfigureAwait(false);
            }
        }

        /// <summary>Decodes the first operation id from a push request body.</summary>
        /// <param name="body">The captured push request body.</param>
        /// <returns>The first operation id when present.</returns>
        internal static OperationId? TryReadFirstPushOperationId(byte[] body)
        {
            try
            {
                using var document = JsonDocument.Parse(body);
                if (!document.RootElement.TryGetProperty("operations", out var operations)
                    || operations.ValueKind != JsonValueKind.Array
                    || operations.GetArrayLength() == 0)
                {
                    return null;
                }

                var first = operations[0];
                if (first.TryGetProperty("operationId", out var id)
                    && id.ValueKind == JsonValueKind.String
                    && Guid.TryParse(id.GetString(), out var parsed))
                {
                    return new(parsed);
                }
            }
            catch (JsonException)
            {
                return null;
            }

            return null;
        }

        /// <summary>Copies request headers.</summary>
        /// <param name="request">The request.</param>
        /// <param name="portable">The portable.</param>
        internal static void CopyRequestHeaders(HttpRequest request, HttpRequestMessage portable)
        {
            foreach (var header in request.Headers)
            {
                if (header.Key.StartsWith("Content-", StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                AddHeaderValues(portable.Headers.TryAddWithoutValidation, header.Key, header.Value);
            }
        }

        /// <summary>Copies the endpoint response to ASP.NET.</summary>
        /// <param name="source">The source.</param>
        /// <param name="target">The target.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The result.</returns>
        internal static async ValueTask CopyResponseAsync(
            HttpResponseMessage source,
            HttpResponse target,
            CancellationToken cancellationToken)
        {
            target.StatusCode = (int)source.StatusCode;
            CopyResponseHeaders(source, target);
            await source.Content.CopyToAsync(target.Body, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>Copies response headers.</summary>
        /// <param name="source">The source.</param>
        /// <param name="target">The target.</param>
        internal static void CopyResponseHeaders(HttpResponseMessage source, HttpResponse target)
        {
            foreach (var header in source.Headers)
            {
                AppendHeaderValues(target.Headers, header.Key, header.Value);
            }

            foreach (var header in source.Content.Headers)
            {
                AppendHeaderValues(target.Headers, header.Key, header.Value);
            }
        }

        /// <summary>Adds header values to a portable header collection.</summary>
        /// <param name="add">The add.</param>
        /// <param name="name">The name.</param>
        /// <param name="values">The values.</param>
        internal static void AddHeaderValues(Func<string, string?, bool> add, string name, StringValues values)
        {
            for (var index = 0; index < values.Count; index++)
            {
                _ = add(name, values[index]);
            }
        }

        /// <summary>Appends header values to ASP.NET response headers.</summary>
        /// <param name="headers">The headers.</param>
        /// <param name="name">The name.</param>
        /// <param name="values">The values.</param>
        internal static void AppendHeaderValues(IHeaderDictionary headers, string name, IEnumerable<string> values)
        {
            foreach (var value in values)
            {
                headers.Append(name, value);
            }
        }

        /// <summary>Disposes startup resources after a failed start.</summary>
        /// <param name="application">The application.</param>
        /// <param name="endpoint">The endpoint.</param>
        /// <param name="hub">The hub.</param>
        /// <returns>The result.</returns>
        /// <exception cref="InvalidOperationException">Startup cleanup fails.</exception>
        internal static async ValueTask DisposeStartupResourcesAsync(
            WebApplication? application,
            HttpServerEndpoint? endpoint,
            ServerStreamHub? hub)
        {
            Exception? failure = null;
            if (application is not null)
            {
                failure = await CaptureCleanupFailureAsync(failure, () => application.DisposeAsync().AsTask()).ConfigureAwait(false);
            }

            if (endpoint is not null)
            {
                failure = await CaptureCleanupFailureAsync(failure, () => endpoint.DisposeAsync().AsTask()).ConfigureAwait(false);
            }

            if (hub is not null)
            {
                failure = await CaptureCleanupFailureAsync(failure, () => hub.DisposeAsync().AsTask()).ConfigureAwait(false);
            }

            if (failure is not null)
            {
                throw new InvalidOperationException("The durable HTTP lost-ACK host startup cleanup failed.", failure);
            }
        }

        /// <summary>Runs cleanup while retaining every failure.</summary>
        /// <param name="existing">The existing.</param>
        /// <param name="cleanup">The cleanup.</param>
        /// <returns>The result.</returns>
        internal static async ValueTask<Exception?> CaptureCleanupFailureAsync(Exception? existing, Func<Task> cleanup)
        {
            try
            {
                await cleanup().ConfigureAwait(false);
                return existing;
            }
            catch (Exception exception)
            {
                return existing is null ? exception : new AggregateException(existing, exception);
            }
        }

        /// <summary>Waits until the first push has committed on the server.</summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The result.</returns>
        internal async ValueTask WaitForFirstPushCommittedAsync(CancellationToken cancellationToken) =>
            await _firstPushCommitted.Task.WaitAsync(cancellationToken).ConfigureAwait(false);

        /// <summary>Waits until the committed first push response has been aborted on the wire.</summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The result.</returns>
        internal async ValueTask WaitForFirstPushResponseAbortedAsync(CancellationToken cancellationToken) =>
            await _firstPushResponseAbortedSignal.Task.WaitAsync(cancellationToken).ConfigureAwait(false);

        /// <summary>Waits until a retry push reaches the server and returns its wire operation id.</summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The retried operation id decoded from the HTTP push request body.</returns>
        internal async ValueTask<OperationId> WaitForRetryPushOperationIdAsync(CancellationToken cancellationToken) =>
            await _retryPushOperationId.Task.WaitAsync(cancellationToken).ConfigureAwait(false);

        /// <summary>Waits until the durable endpoint handles the retry push.</summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The HTTP response status.</returns>
        internal async ValueTask<int> WaitForRetryPushResponseAsync(CancellationToken cancellationToken) =>
            await _retryPushResponse.Task.WaitAsync(cancellationToken).ConfigureAwait(false);

        /// <summary>Waits until the named credential has started a subscribe request.</summary>
        /// <param name="credential">The credential.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The result.</returns>
        internal async ValueTask WaitForSubscribeObservedAsync(string credential, CancellationToken cancellationToken)
        {
            var task = string.Equals(credential, ObserverCredential, StringComparison.Ordinal)
                ? _observerSubscribeObserved.Task
                : _reopenedSubscribeObserved.Task;
            await task.WaitAsync(cancellationToken).ConfigureAwait(false);
        }

        /// <summary>Releases the first withheld push response.</summary>
        internal void ReleaseFirstPushResponse() => _ = _releaseFirstPushResponse.TrySetResult(null);

        /// <summary>Releases withheld subscribe responses.</summary>
        internal void ReleaseSubscribeResponses() => _ = _releaseSubscribeResponses.TrySetResult(null);

        /// <inheritdoc/>
        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        ValueTask IAsyncDisposable.DisposeAsync() => DisposeHostAsync();

        /// <summary>Stops and disposes the host while retaining any cleanup failure.</summary>
        /// <returns>The asynchronous cleanup operation.</returns>
        /// <exception cref="InvalidOperationException">One or more host cleanup steps fail.</exception>
        private async ValueTask DisposeHostAsync()
        {
            ReleaseFirstPushResponse();
            ReleaseSubscribeResponses();
            Exception? failure = null;
            failure = await CaptureCleanupFailureAsync(failure, StopApplicationAsync).ConfigureAwait(false);
            failure = await CaptureCleanupFailureAsync(failure, () => _endpoint.DisposeAsync().AsTask()).ConfigureAwait(false);
            failure = await CaptureCleanupFailureAsync(failure, () => _hub.DisposeAsync().AsTask()).ConfigureAwait(false);
            failure = await CaptureCleanupFailureAsync(failure, () => _application.DisposeAsync().AsTask()).ConfigureAwait(false);
            if (failure is not null)
            {
                throw new InvalidOperationException("The durable HTTP lost-ACK host cleanup failed.", failure);
            }
        }

        /// <summary>Handles one ASP.NET request.</summary>
        /// <param name="context">The context.</param>
        /// <returns>The result.</returns>
        private async Task HandleAsync(HttpContext context)
        {
            if (!TryResolveAuthenticatedClient(context.Request, out var authenticatedClient, out var credential))
            {
                context.Response.StatusCode = StatusCodes.Status401Unauthorized;
                return;
            }

            var isSubscribe = IsSubscribe(context.Request);
            if (isSubscribe)
            {
                SignalSubscribeObserved(credential);
                await _releaseSubscribeResponses.Task.WaitAsync(context.RequestAborted).ConfigureAwait(false);
            }

            var isPush = IsPush(context.Request);
            using var captured = await CreatePortableRequestAsync(context.Request, isPush, context.RequestAborted).ConfigureAwait(false);
            var pushRequestCount = isPush ? Interlocked.Increment(ref _pushRequestCount) : 0;
            if (pushRequestCount >= MinimumPushRequests && captured.PushOperationId.HasValue)
            {
                _ = _retryPushOperationId.TrySetResult(captured.PushOperationId.Value);
            }

            using var response = await _endpoint.HandleAsync(captured.Message, authenticatedClient, context.RequestAborted).ConfigureAwait(false);
            if (pushRequestCount >= MinimumPushRequests)
            {
                await SignalRetryPushResponseAsync(response, context.RequestAborted).ConfigureAwait(false);
            }

            if (isPush && !response.IsSuccessStatusCode)
            {
                _ = _firstPushCommitted.TrySetException(new InvalidOperationException($"The first push returned HTTP {(int)response.StatusCode}."));
            }

            if (await TryAbortFirstPushAsync(context, response, isPush).ConfigureAwait(false))
            {
                return;
            }

            await CopyResponseAsync(response, context.Response, context.RequestAborted).ConfigureAwait(false);
        }

        /// <summary>Signals the durable endpoint result for a retry push.</summary>
        /// <param name="response">The endpoint response.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The signal task.</returns>
        private async ValueTask SignalRetryPushResponseAsync(HttpResponseMessage response, CancellationToken cancellationToken)
        {
            if (response.IsSuccessStatusCode)
            {
                _ = _retryPushResponse.TrySetResult((int)response.StatusCode);
                return;
            }

            var body = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
            _ = _retryPushResponse.TrySetException(new InvalidOperationException($"Retry push returned HTTP {(int)response.StatusCode}: {body}"));
        }

        /// <summary>Aborts the first successful push response after durable server commit.</summary>
        /// <param name="context">The context.</param>
        /// <param name="response">The response.</param>
        /// <param name="isPush">The is push.</param>
        /// <returns>The result.</returns>
        private async ValueTask<bool> TryAbortFirstPushAsync(HttpContext context, HttpResponseMessage response, bool isPush)
        {
            if (!isPush || !response.IsSuccessStatusCode || Interlocked.Exchange(ref _withheldFirstPush, 1) != 0)
            {
                return false;
            }

            _ = _firstPushCommitted.TrySetResult(null);
            await _releaseFirstPushResponse.Task.WaitAsync(context.RequestAborted).ConfigureAwait(false);
            context.Abort();
            _ = Interlocked.Exchange(ref _firstPushResponseAborted, 1);
            _ = _firstPushResponseAbortedSignal.TrySetResult(null);
            return true;
        }

        /// <summary>Signals a subscribe request for a lab credential.</summary>
        /// <param name="credential">The credential.</param>
        private void SignalSubscribeObserved(string credential)
        {
            if (string.Equals(credential, ObserverCredential, StringComparison.Ordinal))
            {
                _ = _observerSubscribeObserved.TrySetResult(null);
                return;
            }

            if (string.Equals(credential, ReopenedCredential, StringComparison.Ordinal))
            {
                _ = _reopenedSubscribeObserved.TrySetResult(null);
            }
        }

        /// <summary>Stops the ASP.NET application with a bounded timeout.</summary>
        /// <returns>The result.</returns>
        private async Task StopApplicationAsync()
        {
            using var stop = new CancellationTokenSource(OperationTimeout);
            await _application.StopAsync(stop.Token).ConfigureAwait(false);
        }

        /// <summary>Owns a captured portable request and the optional decoded push operation id.</summary>
        internal sealed class CapturedPortableRequest : IDisposable
        {
            /// <summary>Initializes a new instance of the <see cref="CapturedPortableRequest"/> class.</summary>
            /// <param name="message">The portable request message.</param>
            /// <param name="pushOperationId">The optional decoded push operation id.</param>
            /// <returns>The result.</returns>
            internal CapturedPortableRequest(HttpRequestMessage message, OperationId? pushOperationId)
            {
                Message = message;
                PushOperationId = pushOperationId;
            }

            /// <summary>Gets the portable request message.</summary>
            internal HttpRequestMessage Message { get; }

            /// <summary>Gets the decoded push operation id.</summary>
            internal OperationId? PushOperationId { get; }

            /// <inheritdoc/>
            [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
            public void Dispose() => Message.Dispose();
        }

        /// <summary>Authorizes replay admission for the explicitly credential-authenticated lab clients.</summary>
        internal sealed class LabReplayAuthorizer : IHttpReplayAuthorizer
        {
            /// <summary>The replay operation name for connect requests.</summary>
            private const string ConnectOperationName = "Connect";

            /// <summary>The replay operation name for push requests.</summary>
            private const string PushOperationName = "Push";

            /// <summary>The replay operation name for subscribe requests.</summary>
            private const string SubscribeOperationName = "Subscribe";

            /// <summary>The replay operation name for acknowledgement requests.</summary>
            private const string AcknowledgeOperationName = "Acknowledge";

            /// <summary>Initializes a new instance of the <see cref="LabReplayAuthorizer"/> class.</summary>
            private LabReplayAuthorizer()
            {
            }

            /// <summary>Gets the singleton lab authorizer.</summary>
            internal static LabReplayAuthorizer Instance { get; } = new();

            /// <inheritdoc/>
            public ValueTask<bool> AuthorizeReplayAsync(HttpReplayAuthorizationContext context, CancellationToken cancellationToken)
            {
                ArgumentNullException.ThrowIfNull(context);
                cancellationToken.ThrowIfCancellationRequested();
                return new(IsAuthorized(context));
            }

            /// <summary>Determines whether the authenticated principal can admit a replay request.</summary>
            /// <param name="context">The replay authorization context.</param>
            /// <returns><see langword="true"/> when the request matches the lab principal and stream contract.</returns>
            private static bool IsAuthorized(HttpReplayAuthorizationContext context) =>
                string.Equals(context.Client.TenantId, TenantId, StringComparison.Ordinal)
                && IsKnownClient(context.Client.ClientId)
                && IsAuthorizedOperation(context);

            /// <summary>Determines whether a client is one of the two lab principals.</summary>
            /// <param name="clientId">The authenticated client identifier.</param>
            /// <returns><see langword="true"/> when the client is known.</returns>
            private static bool IsKnownClient(string clientId) =>
                string.Equals(clientId, WriterClientId, StringComparison.Ordinal)
                || string.Equals(clientId, ObserverClientId, StringComparison.Ordinal);

            /// <summary>Determines whether a known client can perform the replay operation.</summary>
            /// <param name="context">The replay authorization context.</param>
            /// <returns><see langword="true"/> when the operation is allowed.</returns>
            private static bool IsAuthorizedOperation(HttpReplayAuthorizationContext context)
            {
                if (string.Equals(context.Operation, ConnectOperationName, StringComparison.Ordinal))
                {
                    return context.StreamIds.Count == 0 && !context.SubscriptionId.HasValue;
                }

                return HasOnlyLabStream(context.StreamIds)
                    && (string.Equals(context.Operation, PushOperationName, StringComparison.Ordinal)
                        ? IsWriterPush(context)
                        : IsAuthorizedSubscriptionOperation(context));
            }

            /// <summary>Determines whether all decoded stream identifiers target the lab stream.</summary>
            /// <param name="streamIds">The decoded stream identifiers.</param>
            /// <returns><see langword="true"/> when every stream is the lab stream and at least one stream is present.</returns>
            private static bool HasOnlyLabStream(IReadOnlyList<StreamId> streamIds)
            {
                if (streamIds.Count == 0)
                {
                    return false;
                }

                for (var index = 0; index < streamIds.Count; index++)
                {
                    if (streamIds[index] != Stream)
                    {
                        return false;
                    }
                }

                return true;
            }

            /// <summary>Determines whether the writer client is pushing the lab stream.</summary>
            /// <param name="context">The replay authorization context.</param>
            /// <returns><see langword="true"/> when the request is a writer push.</returns>
            private static bool IsWriterPush(HttpReplayAuthorizationContext context) =>
                string.Equals(context.Client.ClientId, WriterClientId, StringComparison.Ordinal)
                && !context.SubscriptionId.HasValue;

            /// <summary>Determines whether the request is an authorized subscribe or acknowledgement operation.</summary>
            /// <param name="context">The replay authorization context.</param>
            /// <returns><see langword="true"/> when the subscription belongs to the authenticated client.</returns>
            private static bool IsAuthorizedSubscriptionOperation(HttpReplayAuthorizationContext context) =>
                (string.Equals(context.Operation, SubscribeOperationName, StringComparison.Ordinal)
                    || string.Equals(context.Operation, AcknowledgeOperationName, StringComparison.Ordinal))
                && IsExpectedSubscription(context.Client.ClientId, context.SubscriptionId);

            /// <summary>Determines whether a subscription belongs to the authenticated lab client.</summary>
            /// <param name="clientId">The authenticated client identifier.</param>
            /// <param name="subscriptionId">The decoded subscription identifier.</param>
            /// <returns><see langword="true"/> when the subscription is expected for the client.</returns>
            private static bool IsExpectedSubscription(string clientId, SubscriptionId? subscriptionId) =>
                subscriptionId.HasValue
                && ((string.Equals(clientId, WriterClientId, StringComparison.Ordinal) && subscriptionId.Value == WriterSubscription)
                    || (string.Equals(clientId, ObserverClientId, StringComparison.Ordinal) && subscriptionId.Value == ObserverSubscription));
        }
    }
}
