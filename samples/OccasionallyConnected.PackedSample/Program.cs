// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.InteropServices;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Crdt;
using ReactiveUI.Primitives.OccasionallyConnected.Hosting;

namespace OccasionallyConnected.PackedSample;

/// <summary>
/// Runs the spec section 16 walkthrough against the packed packages and exits 0 only when every check passes.
/// </summary>
internal static class Program
{
    private static readonly TimeSpan StepTimeout = TimeSpan.FromSeconds(30);
    private static readonly SubscriptionId SubscriptionA = new(new Guid("6C2A31E3-4E0B-4E83-9E0A-6E1C2E4C0A01"));
    private static readonly SubscriptionId SubscriptionB = new(new Guid("6C2A31E3-4E0B-4E83-9E0A-6E1C2E4C0B02"));

    /// <summary>The sample entry point.</summary>
    /// <returns>0 when all checks pass, 1 when a check fails, 2 when the run faults or times out.</returns>
    internal static async Task<int> Main()
    {
        var report = new CheckReport();
        Console.WriteLine($"Runtime: {RuntimeInformation.FrameworkDescription} ({RuntimeInformation.ProcessArchitecture})");
        Console.WriteLine($"Package: {typeof(OccasionallyConnectedBuilder).Assembly.GetName().Name} {typeof(OccasionallyConnectedBuilder).Assembly.GetName().Version}");
        var directory = Path.Combine(Path.GetTempPath(), $"rxui-oc-packed-sample-{Guid.NewGuid():N}");
        _ = Directory.CreateDirectory(directory);
        using var overall = new CancellationTokenSource(TimeSpan.FromMinutes(5));
        try
        {
            var run = RunAsync(directory, report, overall.Token);
            var finished = await Task.WhenAny(run, Task.Delay(Timeout.Infinite, overall.Token).ContinueWith(static _ => { }, TaskScheduler.Default)).ConfigureAwait(false);
            if (finished != run)
            {
                Console.WriteLine("FAULT: the sample did not finish within 5 minutes.");
                return 2;
            }

            await run.ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            Console.WriteLine($"FAULT: {exception}");
            return 2;
        }
        finally
        {
            TryDelete(directory);
        }

        return report.Summarize();
    }

    private static async Task RunAsync(string directory, CheckReport report, CancellationToken cancellationToken)
    {
        await using var server = new SampleServer(Path.Combine(directory, "server.db"));
        var databaseA = Path.Combine(directory, "client-a.db");
        var databaseB = Path.Combine(directory, "client-b.db");

        // 0. Offline startup when the connection is refused outright (no gateway answers).
        report.Check(
            "offline-startup.connection-refused",
            "StartAsync returned",
            await StartWithRefusedConnectionAsync(server, directory, cancellationToken).ConfigureAwait(false));

        // 1. Offline startup: the server is down (the gateway answers 503), yet StartAsync returns and the
        //    context reports Offline.
        var clientA = await SampleClient.StartAsync("client-a", server, databaseA, SubscriptionA, refuseWhenDown: false, cancellationToken).ConfigureAwait(false);
        var clientB = await SampleClient.StartAsync("client-b", server, databaseB, SubscriptionB, refuseWhenDown: false, cancellationToken).ConfigureAwait(false);
        try
        {
            report.Check("offline-startup.start-returned", "True", (!server.IsRunning).ToString());
            var offline = await clientA.Sync.WaitAsync(static state => state.Status == SyncLifecycleStatus.Offline, StepTimeout).ConfigureAwait(false);
            report.Check("offline-startup.status", nameof(SyncLifecycleStatus.Offline), offline.Value?.Status.ToString());
            var offlineHealth = await CheckHealthAsync(clientA.Health).ConfigureAwait(false);
            CheckReport.Info("offline-startup.health", offlineHealth.ToString());

            // 2. Optimistic write with PublishAsync: Local shows the value after the durable local commit.
            var receiptA = await clientA.Stream.PublishAsync(clientA.SetCounter(5), cancellationToken).ConfigureAwait(false);
            var local = await clientA.Local.WaitAsync(static state => state.Value.Counter == 5, StepTimeout).ConfigureAwait(false);
            report.Check("publish-async.optimistic-local", "5", local.Value?.Value.Counter.ToString());
            var status = await clientA.Context.SyncEngine.GetOperationStatusAsync(receiptA.OperationId, cancellationToken).ConfigureAwait(false);
            report.Check("publish-async.not-yet-synchronized", "True", (status is { State: not SyncOperationState.Synchronized }).ToString());

            // 3. Observer input: stream.Input accepts a value without a receipt.
            clientA.Stream.Input.OnNext(clientA.SetCounter(7));
            local = await clientA.Local.WaitAsync(static state => state.Value.Counter == 7, StepTimeout).ConfigureAwait(false);
            report.Check("observer-input.optimistic-local", "7", local.Value?.Value.Counter.ToString());

            // A second device writes concurrently while both are offline.
            var receiptB = await clientB.Stream.PublishAsync(clientB.SetCounter(4), cancellationToken).ConfigureAwait(false);
            var localB = await clientB.Local.WaitAsync(static state => state.Value.Counter == 4, StepTimeout).ConfigureAwait(false);
            report.Check("conflict.client-b-offline-write", "4", localB.Value?.Value.Counter.ToString());

            // 4. Restart recovery: dispose client A while offline, reopen the same database, and find the pending work.
            await clientA.DisposeAsync().ConfigureAwait(false);
            clientA = await SampleClient.StartAsync("client-a", server, databaseA, SubscriptionA, refuseWhenDown: false, cancellationToken).ConfigureAwait(false);
            local = await clientA.Local.WaitAsync(static state => state.Value.Counter == 7, StepTimeout).ConfigureAwait(false);
            report.Check("restart-recovery.local-state", "7", local.Value?.Value.Counter.ToString());
            var pending = await clientA.Sync.WaitAsync(static state => state.PendingOperations >= 2, StepTimeout).ConfigureAwait(false);
            report.Check("restart-recovery.pending-operations", ">=2", pending.Value?.PendingOperations.ToString(), pending.Matched);
            status = await clientA.Context.SyncEngine.GetOperationStatusAsync(receiptA.OperationId, cancellationToken).ConfigureAwait(false);
            report.Check("restart-recovery.receipt-still-pending", "True", (status is { State: not SyncOperationState.Synchronized }).ToString());

            // 5. Reconnect: the server starts; TriggerSyncAsync pushes the outbox without waiting for the next retry.
            server.Start();
            await clientA.Context.SyncEngine.TriggerSyncAsync(cancellationToken).ConfigureAwait(false);
            await clientB.Context.SyncEngine.TriggerSyncAsync(cancellationToken).ConfigureAwait(false);

            // 6. Operation synchronization: await the receipt issued before the restart, and client B's receipt.
            report.Check("await-synchronized.client-a-receipt-from-before-restart", "Synchronized", await AwaitAsync(clientA, receiptA, cancellationToken).ConfigureAwait(false));
            report.Check("await-synchronized.client-b-receipt", "Synchronized", await AwaitAsync(clientB, receiptB, cancellationToken).ConfigureAwait(false));
            var drained = await clientA.Sync.WaitAsync(static state => state.PendingOperations == 0, StepTimeout).ConfigureAwait(false);
            report.Check("reconnect.client-a-outbox-drained", "0", drained.Value?.PendingOperations.ToString());
            var online = await clientA.Sync.WaitAsync(static state => state.Status == SyncLifecycleStatus.Online, StepTimeout).ConfigureAwait(false);
            report.Check("reconnect.status", nameof(SyncLifecycleStatus.Online), online.Value?.Status.ToString());
            var onlineHealth = await CheckHealthAsync(clientA.Health).ConfigureAwait(false);
            report.Check("reconnect.health", nameof(HealthStatus.Healthy), onlineHealth.ToString());

            // 7. Conflict reconciliation: both devices converge on the merged G-counter (A=7 + B=4).
            local = await clientA.Local.WaitAsync(static state => state.Value.Counter == 11, StepTimeout).ConfigureAwait(false);
            report.Check("conflict.client-a-converged", "11", local.Value?.Value.Counter.ToString());
            localB = await clientB.Local.WaitAsync(static state => state.Value.Counter == 11, StepTimeout).ConfigureAwait(false);
            report.Check("conflict.client-b-converged", "11", localB.Value?.Value.Counter.ToString());

            // 8. A write made online is synchronized and reaches the other device.
            var receiptOnline = await clientB.Stream.PublishAsync(clientB.SetCounter(6), cancellationToken).ConfigureAwait(false);
            report.Check("await-synchronized.online-write", "Synchronized", await AwaitAsync(clientB, receiptOnline, cancellationToken).ConfigureAwait(false));
            local = await clientA.Local.WaitAsync(static state => state.Value.Counter == 13, StepTimeout).ConfigureAwait(false);
            report.Check("conflict.client-a-sees-online-write", "13", local.Value?.Value.Counter.ToString());
            CheckReport.Info("faults.client-a", clientA.Faults.Describe());
            CheckReport.Info("faults.client-b", clientB.Faults.Describe());
            CheckReport.Info("server.requests", server.DescribeRequests());
        }
        finally
        {
            await clientA.DisposeAsync().ConfigureAwait(false);
            await clientB.DisposeAsync().ConfigureAwait(false);
        }
    }

    private static async Task<string> StartWithRefusedConnectionAsync(SampleServer server, string directory, CancellationToken cancellationToken)
    {
        SampleClient? probe = null;
        try
        {
            probe = await SampleClient.StartAsync("client-probe", server, Path.Combine(directory, "client-probe.db"), SubscriptionId.New(), refuseWhenDown: true, cancellationToken).ConfigureAwait(false);
            return "StartAsync returned";
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            return $"StartAsync threw {exception.GetType().Name}: {exception.Message}";
        }
        finally
        {
            if (probe is not null)
            {
                await probe.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    private static async Task<string> AwaitAsync(SampleClient client, PublishReceipt receipt, CancellationToken cancellationToken)
    {
        try
        {
            await client.Context.SyncEngine.AwaitSynchronizedAsync(receipt.OperationId, StepTimeout, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is TimeoutException or SyncOperationFailedException)
        {
            var current = await client.Context.SyncEngine.GetOperationStatusAsync(receipt.OperationId, cancellationToken).ConfigureAwait(false);
            return $"{exception.GetType().Name} (state={current?.State.ToString() ?? "missing"}, attempt={current?.Attempt}, reason={current?.ReasonCode ?? "none"})";
        }

        var status = await client.Context.SyncEngine.GetOperationStatusAsync(receipt.OperationId, cancellationToken).ConfigureAwait(false);
        return status?.State.ToString() ?? "missing";
    }

    private static async Task<HealthStatus> CheckHealthAsync(OccasionallyConnectedHealthMonitor monitor)
    {
        var check = new OccasionallyConnectedHealthCheck(monitor);
        var context = new HealthCheckContext { Registration = new HealthCheckRegistration(OccasionallyConnectedHostingExtensions.DefaultHealthCheckName, check, null, null) };
        var result = await check.CheckHealthAsync(context).ConfigureAwait(false);
        return result.Status;
    }

    private static void TryDelete(string directory)
    {
        try
        {
            Directory.Delete(directory, recursive: true);
        }
        catch (IOException)
        {
            // SQLite may keep a pooled handle for a moment; the directory lives under the temp folder.
        }
        catch (UnauthorizedAccessException)
        {
            // Same as above.
        }
    }
}
