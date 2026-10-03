// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Crdt;
using ReactiveUI.Primitives.OccasionallyConnected.Server;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

/// <summary>Demonstrates bounded outbox admission with reject and block publishers.</summary>
internal static class BackpressureScenario
{
    /// <summary>The command-line scenario name.</summary>
    internal const string ScenarioName = "backpressure";

    /// <summary>The trusted client identifier.</summary>
    private const string ClientId = "device-a";

    /// <summary>The counter value published first.</summary>
    private const long FirstValue = 1;

    /// <summary>The counter value the rejected publish carries.</summary>
    private const long RejectedValue = 2;

    /// <summary>The counter value the blocked publish carries.</summary>
    private const long BlockedValue = 3;

    /// <summary>The deterministic subscription seed used to read the server state.</summary>
    private const int SubscriptionSeed = 501;

    /// <summary>The outcome recorded when a publish was admitted.</summary>
    private const string Admitted = "admitted";

    /// <summary>The outcome recorded while a publish waits for capacity.</summary>
    private const string Waiting = "waiting";

    /// <summary>The outcome recorded after a waiting publish completes.</summary>
    private const string Completed = "completed";

    /// <summary>The stream used by the scenario.</summary>
    private static readonly StreamId Stream = new("resilience/backpressure");

    /// <summary>Runs the scenario.</summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The scenario invariants.</returns>
    internal static async ValueTask<IReadOnlyList<ResilienceLabCaseResult>> RunAsync(CancellationToken cancellationToken)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(ResilienceLabLoopback.ScenarioTimeout);
        var directory = ResilienceLabContext.CreateTemporaryDirectory("reactiveui-oc-backpressure");
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
        await using var context = ResilienceLabContext.Create(new(directory, hub, ClientId, 1));
        var stream = context.GetOrCreateStream(ResilienceLabContext.CreateDefinition(Stream, ClientId));
        var rejectOptions = new RemotePublishOptions { StreamId = Stream, AdmissionStrategy = BufferStrategy.Reject };
        var blockOptions = rejectOptions with { AdmissionStrategy = BufferStrategy.Block };

        await stream.StartAsync(cancellationToken).ConfigureAwait(false);

        var first = await stream.PublishAsync(
            ResilienceLabContext.CreateCounterInput(ClientId, FirstValue),
            rejectOptions,
            cancellationToken).ConfigureAwait(false);
        var rejected = await TryPublishAsync(stream, RejectedValue, rejectOptions, cancellationToken).ConfigureAwait(false);
        var blocked = stream.PublishAsync(
            ResilienceLabContext.CreateCounterInput(ClientId, BlockedValue),
            blockOptions,
            cancellationToken).AsTask();
        var blockedBeforeSync = blocked.IsCompleted ? Completed : Waiting;

        await context.StartAsync(cancellationToken).ConfigureAwait(false);
        var second = await blocked.WaitAsync(ResilienceLabLoopback.GuardTimeout, cancellationToken).ConfigureAwait(false);
        await context.SyncEngine.AwaitSynchronizedAsync(first.OperationId, ResilienceLabLoopback.GuardTimeout, cancellationToken)
            .ConfigureAwait(false);
        await context.SyncEngine.AwaitSynchronizedAsync(second.OperationId, ResilienceLabLoopback.GuardTimeout, cancellationToken)
            .ConfigureAwait(false);
        var serverValue = await ReadServerCounterAsync(hub, cancellationToken).ConfigureAwait(false);

        return
        [
            ResilienceLabLoopback.Case($"{ScenarioName}.first-publish-admitted", SyncOperationState.SavedLocally, first.State),
            ResilienceLabLoopback.Case($"{ScenarioName}.reject-when-full", nameof(QueueCapacityExceededException), rejected),
            ResilienceLabLoopback.Case($"{ScenarioName}.block-waits-while-full", Waiting, blockedBeforeSync),
            ResilienceLabLoopback.Case($"{ScenarioName}.block-completes-after-sync", Completed, blocked.IsCompletedSuccessfully ? Completed : Waiting),
            ResilienceLabLoopback.Case($"{ScenarioName}.server-counter", BlockedValue, serverValue),
        ];
    }

    /// <summary>Publishes with the reject strategy and reports the typed outcome.</summary>
    /// <param name="stream">The stream.</param>
    /// <param name="value">The counter value.</param>
    /// <param name="options">The reject publish options.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The outcome name.</returns>
    private static async ValueTask<string> TryPublishAsync(
        IOccasionallyConnectedStream<CrdtState, CrdtInput> stream,
        long value,
        RemotePublishOptions options,
        CancellationToken cancellationToken)
    {
        try
        {
            _ = await stream.PublishAsync(ResilienceLabContext.CreateCounterInput(ClientId, value), options, cancellationToken)
                .ConfigureAwait(false);
            return Admitted;
        }
        catch (QueueCapacityExceededException exception)
        {
            return exception.GetType().Name;
        }
    }

    /// <summary>Reads the authoritative server counter from the first receive page.</summary>
    /// <param name="hub">The server hub.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The server counter.</returns>
    private static async ValueTask<long> ReadServerCounterAsync(ServerStreamHub hub, CancellationToken cancellationToken)
    {
        var page = await ResilienceLabLoopback.ReadFirstPageAsync(
            hub.SubscribeStreamAsync(
                new(Stream, new(ResilienceLabLoopback.CreateGuid(SubscriptionSeed)), null, StartPosition.FromSequence(0)),
                new(ResilienceLabLoopback.TenantId, ClientId),
                cancellationToken),
            cancellationToken).ConfigureAwait(false);
        return CrdtLoopbackReceiver.DecodeStates(page, ResilienceLabLoopback.Bounds)[^1].Value.Counter;
    }
}
