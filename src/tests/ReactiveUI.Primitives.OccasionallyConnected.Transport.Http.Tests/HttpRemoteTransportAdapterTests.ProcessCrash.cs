// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Globalization;
using System.Net.Http;
using ReactiveUI.Primitives.OccasionallyConnected.Server;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>Process-crash tests for HTTP pushes against a durable server hub.</summary>
public sealed partial class HttpRemoteTransportAdapterTests
{
    /// <summary>The environment variable carrying a child crash case.</summary>
    private const string TransportCrashCaseVariable = "RXUI_OC_HTTP_TRANSPORT_CRASH_CASE";

    /// <summary>The point before the endpoint receives a push.</summary>
    private const string BeforeServerApplyPoint = "before-server-apply";

    /// <summary>The point after the endpoint commits a push and before its response reaches the client.</summary>
    private const string AfterServerApplyPoint = "after-server-apply-before-ack";

    /// <summary>The point after the client receives the push acknowledgement.</summary>
    private const string AfterClientAckPoint = "after-client-ack";

    /// <summary>The number of fields passed to the child.</summary>
    private const int TransportCrashCaseFields = 3;

    /// <summary>The reopened domain calls when the first push did not reach the server.</summary>
    private const int BeforeApplyDomainCalls = 2;

    /// <summary>The child signal polling interval in milliseconds.</summary>
    private const int TransportCrashPollMilliseconds = 50;

    /// <summary>The maximum child signal wait in seconds.</summary>
    private const int TransportCrashSignalSeconds = 25;

    /// <summary>The maximum child exit and output wait in seconds.</summary>
    private const int TransportCrashExitSeconds = 5;

    /// <summary>The child test node selected by direct Microsoft Testing Platform execution.</summary>
    private const string TransportCrashChildFilter = $"/*/*/*/{nameof(WhenHttpPushChildReachesCrashPoint_ThenSignalsParent)}";

    /// <summary>Verifies a killed HTTP push is retried safely after reopening the SQLite server journal.</summary>
    /// <param name="point">The transport crash point.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [NotInParallel("oc-http-transport-crash")]
    [Arguments(BeforeServerApplyPoint)]
    [Arguments(AfterServerApplyPoint)]
    [Arguments(AfterClientAckPoint)]
    public async Task WhenProcessDiesDuringHttpPush_ThenRestartedTransportPreservesServerEffect(string point)
    {
        var directory = Path.Combine(PhysicalTempDirectory.GetRoot(), $"rxui-http-crash-{Guid.NewGuid():N}");
        _ = Directory.CreateDirectory(directory);
        try
        {
            var databasePath = Path.Combine(directory, "server.db");
            var signalPath = Path.Combine(directory, "crash.signal");
            await RunHttpPushCrashChildAsync(string.Join('\n', point, databasePath, signalPath), signalPath);
            await Assert.That(await File.ReadAllTextAsync(signalPath)).IsEqualTo(point);

            var clock = new ReplayTimeProvider(ReplayObservedUtc);
            var domain = new HubDomainHandler();
            await using var hub = ServerStreamHub.CreateSqlite(databasePath, CreateHubOptions(domain, clock));
            await using var endpoint = new HttpServerEndpoint(CreateReplayEndpointOptions(hub, clock));
            using var handler = new ReplayEndpointHandler(endpoint);
            using var client = CreateHttpClient(handler);
            await using var adapter = CreateResolvedReplayAdapter(client, clock);
            await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
            var operation = CreateOperation(1);
            var retried = await session.PushAsync(new(Guid.NewGuid(), [operation]), CancellationToken.None);
            var next = await session.PushAsync(new(Guid.NewGuid(), [CreateOperation(SecondSequence)]), CancellationToken.None);

            await Assert.That(retried.Operations.Count).IsEqualTo(1);
            await Assert.That(retried.Operations[0].OperationId).IsEqualTo(operation.OperationId);
            await Assert.That(retried.Operations[0].Kind).IsEqualTo(OperationResultKind.Accepted);
            await Assert.That(retried.Operations[0].ServerVersion).IsEqualTo(HubFirstVersion);
            await Assert.That(next.Operations[0].ServerVersion).IsEqualTo("v2");
            await Assert.That(domain.CallCount).IsEqualTo(point == BeforeServerApplyPoint ? BeforeApplyDomainCalls : 1);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    /// <summary>Runs the HTTP push in a child process until its selected crash point.</summary>
    /// <returns>The assertion task.</returns>
    /// <exception cref="InvalidOperationException">The child case is malformed or the push returns unexpectedly.</exception>
    [Test]
    public async Task WhenHttpPushChildReachesCrashPoint_ThenSignalsParent()
    {
        var encoded = Environment.GetEnvironmentVariable(TransportCrashCaseVariable);
        if (encoded is null)
        {
            await Assert.That(encoded).IsNull();
            return;
        }

        var fields = encoded.Split('\n');
        if (fields.Length != TransportCrashCaseFields
            || fields[0] is not (BeforeServerApplyPoint or AfterServerApplyPoint or AfterClientAckPoint))
        {
            throw new InvalidOperationException("The HTTP transport crash case is malformed.");
        }

        var clock = new ReplayTimeProvider(ReplayObservedUtc);
        await using var hub = ServerStreamHub.CreateSqlite(fields[1], CreateHubOptions(new(), clock));
        await using var endpoint = new HttpServerEndpoint(CreateReplayEndpointOptions(hub, clock));
        using var handler = new CrashPointEndpointHandler(endpoint, fields[0], fields[2]);
        using var client = CreateHttpClient(handler);
        await using var adapter = CreateResolvedReplayAdapter(client, clock);
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var acknowledged = await session.PushAsync(new(Guid.NewGuid(), [CreateOperation(1)]), CancellationToken.None);
        if (fields[0] == AfterClientAckPoint)
        {
            await Assert.That(acknowledged.Operations[0].Kind).IsEqualTo(OperationResultKind.Accepted);
            SignalHttpCrashPoint(fields[2], fields[0]);
        }

        throw new InvalidOperationException("The HTTP push completed without stopping at its crash point.");
    }

    /// <summary>Starts the child test, waits for its atomic signal, then kills its process tree.</summary>
    /// <param name="encodedCase">The crash case fields.</param>
    /// <param name="signalPath">The signal file path.</param>
    /// <returns>The child process task.</returns>
    /// <exception cref="InvalidOperationException">The child did not reach the crash point.</exception>
    private static async Task RunHttpPushCrashChildAsync(string encodedCase, string signalPath)
    {
        var assembly = Path.Combine(AppContext.BaseDirectory, "ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests.dll");
        ProcessStartInfo start = new("dotnet") { CreateNoWindow = true, RedirectStandardError = true, RedirectStandardOutput = true, UseShellExecute = false };
        start.ArgumentList.Add(assembly);
        start.ArgumentList.Add("--treenode-filter");
        start.ArgumentList.Add(TransportCrashChildFilter);
        start.Environment[TransportCrashCaseVariable] = encodedCase;
        using var child = Process.Start(start) ?? throw new InvalidOperationException("The HTTP crash child did not start.");
        var stdout = child.StandardOutput.ReadToEndAsync();
        var stderr = child.StandardError.ReadToEndAsync();
        var began = Stopwatch.GetTimestamp();
        while (!File.Exists(signalPath) && !child.HasExited && Stopwatch.GetElapsedTime(began) < TimeSpan.FromSeconds(TransportCrashSignalSeconds))
        {
            await Task.Delay(TimeSpan.FromMilliseconds(TransportCrashPollMilliseconds));
        }

        var signaled = File.Exists(signalPath);
        if (!child.HasExited)
        {
            child.Kill(entireProcessTree: true);
            await child.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(TransportCrashExitSeconds));
        }

        var output = await stdout.WaitAsync(TimeSpan.FromSeconds(TransportCrashExitSeconds));
        var error = await stderr.WaitAsync(TimeSpan.FromSeconds(TransportCrashExitSeconds));
        if (!signaled)
        {
            throw new InvalidOperationException(string.Join(Environment.NewLine, "The HTTP crash child did not signal.", output, error));
        }
    }

    /// <summary>Writes the reached point and blocks until the parent kills this process.</summary>
    /// <param name="signalPath">The atomic signal path.</param>
    /// <param name="point">The reached point.</param>
    private static void SignalHttpCrashPoint(string signalPath, string point)
    {
        var temporary = $"{signalPath}.{Environment.ProcessId.ToString(CultureInfo.InvariantCulture)}.tmp";
        File.WriteAllText(temporary, point);
        File.Move(temporary, signalPath);
        using var never = new ManualResetEventSlim(false);
        never.Wait();
    }

    /// <summary>Forwards HTTP requests to an endpoint and blocks at the configured push boundary.</summary>
    /// <param name="endpoint">The public server endpoint.</param>
    /// <param name="point">The selected crash point.</param>
    /// <param name="signalPath">The atomic signal path.</param>
    private sealed class CrashPointEndpointHandler(HttpServerEndpoint endpoint, string point, string signalPath) : HttpMessageHandler
    {
        /// <inheritdoc/>
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var isPush = request.RequestUri?.AbsolutePath.EndsWith($"/{ReplayEndpointPushPath}", StringComparison.Ordinal) == true;
            if (isPush && point == BeforeServerApplyPoint)
            {
                SignalHttpCrashPoint(signalPath, point);
            }

            var response = await endpoint.HandleAsync(
                request,
                new(ReplayEndpointTenantId, ReplayEndpointClientId),
                cancellationToken).ConfigureAwait(false);
            if (isPush && point == AfterServerApplyPoint)
            {
                if (!response.IsSuccessStatusCode)
                {
                    throw new InvalidOperationException("The HTTP endpoint rejected the push before the crash point.");
                }

                SignalHttpCrashPoint(signalPath, point);
            }

            return response;
        }
    }
}
