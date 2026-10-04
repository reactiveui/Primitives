// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Net.Http;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected.Crdt;
using ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;
using ReactiveUI.Primitives.OccasionallyConnected.Server;
using ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.Tests;

/// <summary>
/// End-to-end tests for the retention-gap recovery that <see cref="RetentionGapRecoveryScenario"/> demonstrates. They drive
/// the real engine through a public context, so snapshot recovery starts on its own when the persisted cursor expires.
/// </summary>
public sealed class RetentionGapRecoveryScenarioTests
{
    /// <summary>The loopback transport selector.</summary>
    private const int LoopbackTransport = 0;

    /// <summary>The HTTP transport selector.</summary>
    private const int HttpTransport = 1;

    /// <summary>The receiving client identifier.</summary>
    private const string ReaderClientId = "device-reader";

    /// <summary>The client identifier whose counter component the server publishes.</summary>
    private const string WriterClientId = "device-writer";

    /// <summary>The server operation retention in minutes.</summary>
    private const int OperationRetentionMinutes = 1;

    /// <summary>The clock advance that pushes early commits out of retention, in minutes.</summary>
    private const int OfflineMinutes = 2;

    /// <summary>The value the reader receives before it goes offline.</summary>
    private const long FirstValue = 1;

    /// <summary>The value that expires while the reader is offline.</summary>
    private const long ExpiringValue = 2;

    /// <summary>The value the server holds when the reader recovers.</summary>
    private const long SnapshotValue = 3;

    /// <summary>The value published after recovery.</summary>
    private const long ResumedValue = 4;

    /// <summary>The base of the deterministic batch and operation GUID seeds.</summary>
    private const int SeedBase = 900;

    /// <summary>
    /// Verifies a context whose persisted cursor fell out of a SQLite hub's retention recovers through a snapshot on its
    /// own and then keeps receiving new events.
    /// </summary>
    /// <param name="transport">The transport selector.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(HttpTransport)]
    [Arguments(LoopbackTransport)]
    public async Task ContextRecoversExpiredCursorThroughSnapshotAndKeepsReceiving(int transport)
    {
        var stream = new StreamId($"resilience/gap-e2e-{transport}");
        var subscriptionId = SubscriptionId.New();
        var serverDirectory = ResilienceLabContext.CreateTemporaryDirectory("reactiveui-oc-gap-server");
        var clientDirectory = ResilienceLabContext.CreateTemporaryDirectory("reactiveui-oc-gap-client");
        try
        {
            var clock = new ResilienceLabClock(ResilienceLabLoopback.InitialTime);
            await using var hub = ServerStreamHub.CreateSqlite(
                Path.Combine(serverDirectory, "journal.db"),
                CreateHubOptions(clock, stream));

            await PublishAsync(hub, stream, FirstValue);
            await using (var firstRun = new ReaderRun(transport == HttpTransport, hub, clientDirectory, stream, subscriptionId))
            {
                await firstRun.StartAsync();
                await Assert.That(await firstRun.WaitForCounterAsync(FirstValue)).IsEqualTo(FirstValue);
            }

            await PublishAsync(hub, stream, ExpiringValue);
            clock.Advance(TimeSpan.FromMinutes(OfflineMinutes));
            await PublishAsync(hub, stream, SnapshotValue);

            await using var secondRun = new ReaderRun(transport == HttpTransport, hub, clientDirectory, stream, subscriptionId);
            await secondRun.StartAsync();
            await Assert.That(await secondRun.WaitForCounterAsync(SnapshotValue)).IsEqualTo(SnapshotValue);
            await Assert.That(secondRun.ObservedCounters).DoesNotContain(ExpiringValue);

            await PublishAsync(hub, stream, ResumedValue);
            await Assert.That(await secondRun.WaitForCounterAsync(ResumedValue)).IsEqualTo(ResumedValue);
        }
        finally
        {
            DeleteDirectory(clientDirectory);
            DeleteDirectory(serverDirectory);
        }
    }

    /// <summary>Creates SQLite-backed hub options with snapshot recovery for one G-counter stream.</summary>
    /// <param name="clock">The server clock.</param>
    /// <param name="stream">The stream.</param>
    /// <returns>The hub options.</returns>
    private static ServerStreamHubOptions CreateHubOptions(TimeProvider clock, StreamId stream) =>
        ResilienceLabLoopback.CreateHubOptions(
            clock,
            ResilienceLabLoopback.CreateJournalLimits(TimeSpan.FromMinutes(OperationRetentionMinutes)),
            stream) with
        {
            SnapshotRecoveryAuthorizationPolicy = new ResilienceLabAuthorizationPolicy(ResilienceLabLoopback.TenantId),
            SnapshotRecoveryMaterializer = new ResilienceLabSnapshotMaterializer(),
        };

    /// <summary>Publishes the writer's G-counter component straight to the hub.</summary>
    /// <param name="hub">The hub.</param>
    /// <param name="stream">The stream.</param>
    /// <param name="value">The counter value, also used as the client sequence and GUID seed.</param>
    /// <returns>The publish task.</returns>
    private static async Task PublishAsync(ServerStreamHub hub, StreamId stream, long value)
    {
        var seed = SeedBase + (int)value;
        _ = await hub.ApplyOperationsAsync(
            ResilienceLabLoopback.CreateBatch(seed, seed + SeedBase, stream, value, CrdtMutation.GCounterSet(WriterClientId, value)),
            new(ResilienceLabLoopback.TenantId, WriterClientId),
            CancellationToken.None);
    }

    /// <summary>Deletes a temporary directory when it still exists.</summary>
    /// <param name="path">The directory path.</param>
    private static void DeleteDirectory(string path)
    {
        if (Directory.Exists(path))
        {
            Directory.Delete(path, recursive: true);
        }
    }

    /// <summary>Owns one reader context and the transport resources behind it.</summary>
    private sealed class ReaderRun : IAsyncDisposable, IObserver<CrdtState>
    {
        /// <summary>The outbox operation limit of the reader context.</summary>
        private const int OutboxOperations = 16;

        /// <summary>The long-poll timeout of the in-process HTTP endpoint, in milliseconds.</summary>
        private const int LongPollMilliseconds = 500;

        /// <summary>The receive buffer byte bound.</summary>
        private const int ReceiveBufferBytes = 65_536;

        /// <summary>The HTTP base address of the in-process endpoint.</summary>
        private const string HttpBaseAddress = "https://example.invalid/";

        /// <summary>The upper bound for each awaited state.</summary>
        private static readonly TimeSpan StateTimeout = TimeSpan.FromSeconds(30);

        /// <summary>Protects the observed state list and the pending wait.</summary>
#if NET9_0_OR_GREATER
        private readonly Lock _gate = new();
#else
        private readonly object _gate = new();
#endif

        /// <summary>The counters observed on the local projection.</summary>
        private readonly List<long> _counters = [];

        /// <summary>The HTTP endpoint, when HTTP is under test.</summary>
        private readonly HttpServerEndpoint? _endpoint;

        /// <summary>The HTTP client, when HTTP is under test.</summary>
        private readonly HttpClient? _httpClient;

        /// <summary>The reader context.</summary>
        private readonly OccasionallyConnectedContext _context;

        /// <summary>The local projection subscription.</summary>
        private readonly IDisposable _subscription;

        /// <summary>The engine faults observed while the context runs.</summary>
        private readonly FaultLog _faults = new();

        /// <summary>The engine fault subscription.</summary>
        private readonly IDisposable _faultSubscription;

        /// <summary>The pending counter wait.</summary>
        private (long Expected, TaskCompletionSource<long> Completion)? _wait;

        /// <summary>Initializes a new instance of the <see cref="ReaderRun"/> class.</summary>
        /// <param name="useHttp">Whether the reader connects over HTTP instead of loopback.</param>
        /// <param name="hub">The hub.</param>
        /// <param name="directory">The client store directory.</param>
        /// <param name="stream">The stream.</param>
        /// <param name="subscriptionId">The durable subscription identifier shared by every run.</param>
        internal ReaderRun(bool useHttp, ServerStreamHub hub, string directory, StreamId stream, SubscriptionId subscriptionId)
        {
            IRemoteTransportAdapter adapter;
            var loopbackOptions = ResilienceLabLoopback.CreateLoopbackOptions(
                hub,
                ReaderClientId,
                CrdtLoopbackScenarioShape.VolatileLoopbackCapabilities | RemoteTransportCapabilities.SnapshotRecovery);
            if (useHttp)
            {
                _endpoint = new(new()
                {
                    Hub = hub,
                    SnapshotRecoveryHub = hub,
                    DeclaredCapabilities = loopbackOptions.PeerCapabilities with
                    {
                        Features = loopbackOptions.PeerCapabilities.Features & ~RemoteTransportCapabilities.StreamingReceive,
                    },
                    ReplayAuthorizer = AllowReplayAuthorizer.Instance,
                    LongPollTimeout = TimeSpan.FromMilliseconds(LongPollMilliseconds),
                });
                _httpClient = new(new EndpointHandler(_endpoint));
                adapter = new HttpRemoteTransportAdapter(new() { HttpClient = _httpClient, BaseAddress = new(HttpBaseAddress) });
            }
            else
            {
                adapter = new LoopbackTransportAdapter(loopbackOptions with { SnapshotRecoveryHub = hub });
            }

            _context = ResilienceLabContext.Create(new(directory, hub, ReaderClientId, OutboxOperations), adapter);
            var definition = ResilienceLabContext.CreateDefinition(stream, ReaderClientId) with
            {
                SubscriptionId = subscriptionId,
                Subscription = new()
                {
                    StreamId = stream,
                    SubscriptionId = subscriptionId,
                    StartPosition = StartPosition.FromSequence(0),
                    DeliveryGuarantee = DeliveryGuarantee.AtLeastOnce,
                    BufferStrategy = BufferStrategy.Block,
                    BufferCapacity = ResilienceLabLoopback.Capacity,
                    BufferCapacityBytes = ReceiveBufferBytes,
                },
            };
            _subscription = _context.GetOrCreateStream(definition).Local.Subscribe(this);
            _faultSubscription = _context.SyncEngine.Faults.Subscribe(_faults);
        }

        /// <summary>Gets every counter observed on the local projection.</summary>
        internal long[] ObservedCounters
        {
            get
            {
                lock (_gate)
                {
                    return [.. _counters];
                }
            }
        }

        /// <inheritdoc/>
        public void OnNext(CrdtState value)
        {
            TaskCompletionSource<long>? completed = null;
            var counter = value.Value.Counter;
            lock (_gate)
            {
                _counters.Add(counter);
                if (_wait is { } wait && counter >= wait.Expected)
                {
                    completed = wait.Completion;
                    _wait = null;
                }
            }

            _ = completed?.TrySetResult(counter);
        }

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
            TaskCompletionSource<long>? pending;
            lock (_gate)
            {
                pending = _wait?.Completion;
                _wait = null;
            }

            _ = pending?.TrySetException(error);
        }

        /// <inheritdoc/>
        public void OnCompleted()
        {
        }

        /// <inheritdoc/>
        public async ValueTask DisposeAsync()
        {
            _subscription.Dispose();
            _faultSubscription.Dispose();
            await _context.DisposeAsync();
            _httpClient?.Dispose();
            if (_endpoint is not null)
            {
                await _endpoint.DisposeAsync();
            }
        }

        /// <summary>Starts the context.</summary>
        /// <returns>The start task.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Task StartAsync() => _context.StartAsync(CancellationToken.None).AsTask();

        /// <summary>Waits until the local projection shows at least the expected counter.</summary>
        /// <param name="expected">The expected counter.</param>
        /// <returns>The first counter at or above the expected value.</returns>
        internal Task<long> WaitForCounterAsync(long expected)
        {
            TaskCompletionSource<long> completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
            lock (_gate)
            {
                var reached = _counters.Find(counter => counter >= expected);
                if (reached != 0)
                {
                    return Task.FromResult(reached);
                }

                _wait = (expected, completion);
            }

            return WaitWithDiagnosticsAsync(completion.Task, expected);
        }

        /// <summary>Waits for a counter and reports the observed counters and faults on timeout.</summary>
        /// <param name="wait">The counter wait.</param>
        /// <param name="expected">The expected counter.</param>
        /// <returns>The reached counter.</returns>
        /// <exception cref="TimeoutException">The counter was not reached in time.</exception>
        private async Task<long> WaitWithDiagnosticsAsync(Task<long> wait, long expected)
        {
            try
            {
                return await wait.WaitAsync(StateTimeout);
            }
            catch (TimeoutException exception)
            {
                throw new TimeoutException(
                    $"Counter {expected} not reached. Observed [{string.Join(",", ObservedCounters)}]. Faults: {_faults.Describe()}",
                    exception);
            }
        }
    }

    /// <summary>Records engine faults for timeout diagnostics.</summary>
    private sealed class FaultLog : IObserver<OccasionallyConnectedFault>
    {
        /// <summary>The recorded fault descriptions.</summary>
        private readonly System.Collections.Concurrent.ConcurrentQueue<string> _faults = new();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(OccasionallyConnectedFault value) =>
            _faults.Enqueue($"{value.Code}:{value.Message}:{value.Exception?.GetType().Name}:{value.Exception?.Message}");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => _faults.Enqueue($"error:{error.Message}");

        /// <inheritdoc/>
        public void OnCompleted()
        {
        }

        /// <summary>Describes the recorded faults.</summary>
        /// <returns>The joined fault descriptions.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal string Describe() => string.Join(" | ", _faults);
    }

    /// <summary>Routes HTTP client requests into the in-process endpoint for the trusted reader.</summary>
    /// <param name="endpoint">The endpoint.</param>
    private sealed class EndpointHandler(HttpServerEndpoint endpoint) : HttpMessageHandler
    {
        /// <inheritdoc/>
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) =>
            await endpoint.HandleAsync(request, new(ResilienceLabLoopback.TenantId, ReaderClientId), cancellationToken);
    }

    /// <summary>Allows every replay admission.</summary>
    private sealed class AllowReplayAuthorizer : IHttpReplayAuthorizer
    {
        /// <summary>Gets the shared authorizer.</summary>
        internal static AllowReplayAuthorizer Instance { get; } = new();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<bool> AuthorizeReplayAsync(HttpReplayAuthorizationContext context, CancellationToken cancellationToken) =>
            ValueTask.FromResult(true);
    }
}
