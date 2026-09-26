// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Crdt;
using ReactiveUI.Primitives.OccasionallyConnected.Server;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

/// <summary>Demonstrates that blocked state observers do not hold up publication or durable commits.</summary>
internal static class SlowObserversScenario
{
    /// <summary>The command-line scenario name.</summary>
    internal const string ScenarioName = "slow-observers";

    /// <summary>The trusted client identifier.</summary>
    private const string ClientId = "device-a";

    /// <summary>The number of publishes made while the observers are blocked.</summary>
    private const int PublishCount = 3;

    /// <summary>The outbox operation limit, large enough for every publish.</summary>
    private const int OutboxOperations = 16;

    /// <summary>The outcome recorded while an observer is still blocked.</summary>
    private const string Blocked = "blocked";

    /// <summary>The outcome recorded after an observer was released.</summary>
    private const string Released = "released";

    /// <summary>The outcome recorded when the store holds the operation as saved or queued.</summary>
    private const string Persisted = "persisted";

    /// <summary>The stream used by the scenario.</summary>
    private static readonly StreamId Stream = new("resilience/slow-observers");

    /// <summary>Runs the scenario.</summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The scenario invariants.</returns>
    internal static async ValueTask<IReadOnlyList<ResilienceLabCaseResult>> RunAsync(CancellationToken cancellationToken)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(ResilienceLabLoopback.GuardTimeout);
        var directory = ResilienceLabContext.CreateTemporaryDirectory("reactiveui-oc-slow-observers");
        try
        {
            return await RunInDirectoryAsync(directory, timeout.Token).ConfigureAwait(false);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    /// <summary>Runs the scenario against one temporary directory.</summary>
    /// <param name="directory">The temporary directory.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The scenario invariants.</returns>
    private static async ValueTask<IReadOnlyList<ResilienceLabCaseResult>> RunInDirectoryAsync(
        string directory,
        CancellationToken cancellationToken)
    {
        var clock = new ResilienceLabClock(ResilienceLabLoopback.InitialTime);
        await using var hub = ServerStreamHub.CreateInMemory(ResilienceLabLoopback.CreateHubOptions(
            clock,
            ResilienceLabLoopback.CreateJournalLimits(TimeSpan.FromMinutes(ResilienceLabLoopback.GuardTimeoutSeconds)),
            Stream));
        await using var context = ResilienceLabContext.Create(new(directory, hub, ClientId, OutboxOperations));
        var stream = context.GetOrCreateStream(ResilienceLabContext.CreateDefinition(Stream, ClientId));
        using var gate = new ManualResetEventSlim(false);
        var localObserver = new ResilienceLabBlockingObserver<CrdtState>(
            gate,
            ResilienceLabLoopback.GuardTimeout,
            static state => state.Value.Counter == PublishCount);
        var syncObserver = new ResilienceLabBlockingObserver<SyncState>(
            gate,
            ResilienceLabLoopback.GuardTimeout,
            static state => state.PendingOperations == PublishCount);
        using var localSubscription = stream.Local.Subscribe(localObserver);
        using var syncSubscription = context.SyncStates.Subscribe(syncObserver);

        var receipts = await PublishWhileBlockedAsync(stream, localObserver, syncObserver, cancellationToken).ConfigureAwait(false);
        var lastStatus = await context.SyncEngine.GetOperationStatusAsync(receipts[^1].OperationId, cancellationToken)
            .ConfigureAwait(false);
        var observersBeforeRelease = localObserver.Latest.IsCompleted || syncObserver.Latest.IsCompleted ? Released : Blocked;

        gate.Set();
        var latestLocal = await localObserver.Latest.WaitAsync(ResilienceLabLoopback.GuardTimeout, cancellationToken)
            .ConfigureAwait(false);
        var latestSync = await syncObserver.Latest.WaitAsync(ResilienceLabLoopback.GuardTimeout, cancellationToken)
            .ConfigureAwait(false);

        return
        [
            ResilienceLabLoopback.Case($"{ScenarioName}.receipts-while-blocked", PublishCount, receipts.Count),
            ResilienceLabLoopback.Case($"{ScenarioName}.observers-still-blocked", Blocked, observersBeforeRelease),
            ResilienceLabLoopback.Case($"{ScenarioName}.durable-commit-while-blocked", Persisted, DescribePersistence(lastStatus)),
            ResilienceLabLoopback.Case($"{ScenarioName}.local-observer-receives-latest", (long)PublishCount, latestLocal.Value.Counter),
            ResilienceLabLoopback.Case($"{ScenarioName}.sync-observer-receives-latest", PublishCount, latestSync.PendingOperations),
        ];
    }

    /// <summary>Describes whether an operation status shows a durable local commit.</summary>
    /// <param name="status">The operation status, or null when the store has no record.</param>
    /// <returns>The persistence outcome.</returns>
    private static string DescribePersistence(SyncOperationStatus? status) =>
        status?.State is SyncOperationState.SavedLocally or SyncOperationState.QueuedForUpload
            ? Persisted
            : status?.State.ToString() ?? "missing";

    /// <summary>Publishes every counter value after both observers have entered their blocked callbacks.</summary>
    /// <param name="stream">The stream.</param>
    /// <param name="localObserver">The blocked local state observer.</param>
    /// <param name="syncObserver">The blocked sync state observer.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The publish receipts.</returns>
    private static async ValueTask<List<PublishReceipt>> PublishWhileBlockedAsync(
        IOccasionallyConnectedStream<CrdtState, CrdtInput> stream,
        ResilienceLabBlockingObserver<CrdtState> localObserver,
        ResilienceLabBlockingObserver<SyncState> syncObserver,
        CancellationToken cancellationToken)
    {
        List<PublishReceipt> receipts = [];
        for (var value = 1; value <= PublishCount; value++)
        {
            receipts.Add(await stream.PublishAsync(ResilienceLabContext.CreateCounterInput(ClientId, value), cancellationToken)
                .ConfigureAwait(false));
            if (value == 1)
            {
                await Task.WhenAll(localObserver.Entered, syncObserver.Entered)
                    .WaitAsync(ResilienceLabLoopback.GuardTimeout, cancellationToken)
                    .ConfigureAwait(false);
            }
        }

        return receipts;
    }
}
