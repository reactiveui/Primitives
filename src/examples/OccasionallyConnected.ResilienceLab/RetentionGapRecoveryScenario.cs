// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected.Crdt;
using ReactiveUI.Primitives.OccasionallyConnected.Server;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

/// <summary>Demonstrates that a cursor outside server retention fails explicitly and recovers through a snapshot.</summary>
internal static class RetentionGapRecoveryScenario
{
    /// <summary>The command-line scenario name.</summary>
    internal const string ScenarioName = "retention-gap-recovery";

    /// <summary>The trusted client identifier.</summary>
    private const string ClientId = "device-a";

    /// <summary>The server operation retention in minutes.</summary>
    private const int OperationRetentionMinutes = 1;

    /// <summary>The clock advance that pushes early commits out of retention, in minutes.</summary>
    private const int OfflineMinutes = 2;

    /// <summary>The counter value published before the client goes offline.</summary>
    private const long ExpiringValue = 2;

    /// <summary>The counter value the server holds when the snapshot is taken.</summary>
    private const long SnapshotValue = 3;

    /// <summary>The counter value published after recovery.</summary>
    private const long ResumedValue = 4;

    /// <summary>The largest snapshot response the client accepts.</summary>
    private const int MaximumResponseBytes = 16_384;

    /// <summary>The deterministic subscription seed.</summary>
    private const int SubscriptionSeed = 701;

    /// <summary>The deterministic batch and operation seed base.</summary>
    private const int SeedBase = 710;

    /// <summary>The outcome recorded when resubscribing succeeded.</summary>
    private const string Resumed = "resumed";

    /// <summary>The stream used by the scenario.</summary>
    private static readonly StreamId Stream = new("resilience/retention");

    /// <summary>The subscription whose cursor falls outside retention.</summary>
    private static readonly SubscriptionId Subscription = new(ResilienceLabLoopback.CreateGuid(SubscriptionSeed));

    /// <summary>The trusted client principal.</summary>
    private static readonly ServerAuthenticatedClient Client = new(ResilienceLabLoopback.TenantId, ClientId);

    /// <summary>Runs the scenario.</summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The scenario invariants.</returns>
    internal static async ValueTask<IReadOnlyList<ResilienceLabCaseResult>> RunAsync(CancellationToken cancellationToken)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(ResilienceLabLoopback.GuardTimeout);
        var token = timeout.Token;
        var clock = new ResilienceLabClock(ResilienceLabLoopback.InitialTime);
        var authorization = new ResilienceLabAuthorizationPolicy(ResilienceLabLoopback.TenantId);
        await using var hub = ServerStreamHub.CreateInMemory(ResilienceLabLoopback.CreateHubOptions(
            clock,
            ResilienceLabLoopback.CreateJournalLimits(TimeSpan.FromMinutes(OperationRetentionMinutes)),
            Stream) with
        {
            SnapshotRecoveryAuthorizationPolicy = authorization,
            SnapshotRecoveryMaterializer = new ResilienceLabSnapshotMaterializer(),
        });

        await PublishAsync(hub, 1, token).ConfigureAwait(false);
        var firstPage = await ReadPageAsync(hub, null, token).ConfigureAwait(false);
        var expiredCursor = firstPage.NextCursor;
        await PublishAsync(hub, ExpiringValue, token).ConfigureAwait(false);
        clock.Advance(TimeSpan.FromMinutes(OfflineMinutes));
        await PublishAsync(hub, SnapshotValue, token).ConfigureAwait(false);

        var gap = await TryResumeAsync(hub, expiredCursor, token).ConfigureAwait(false);
        var recovery = await hub.GetSnapshotAsync(CreateRecoveryRequest(expiredCursor), Client, token).ConfigureAwait(false);
        var checkpoint = recovery.Checkpoint;
        var snapshotValue = checkpoint is null
            ? 0
            : CrdtCodec.DecodeState(checkpoint.ClientState.Payload, ResilienceLabLoopback.Bounds).Value.Counter;

        await PublishAsync(hub, ResumedValue, token).ConfigureAwait(false);
        var resumed = await ReadPageAsync(hub, checkpoint?.FrontierCursor, token).ConfigureAwait(false);
        var resumedValue = CrdtLoopbackReceiver.DecodeStates(resumed, ResilienceLabLoopback.Bounds)[^1].Value.Counter;

        return
        [
            ResilienceLabLoopback.Case(
                $"{ScenarioName}.expired-cursor-raises-gap",
                ServerReceiveRetentionGapException.ReceiveRetentionGapReasonCode,
                gap),
            ResilienceLabLoopback.Case($"{ScenarioName}.snapshot-recovered", RemoteSnapshotRecoveryStatus.Recovered, recovery.Status),
            ResilienceLabLoopback.Case($"{ScenarioName}.snapshot-holds-latest-state", SnapshotValue, snapshotValue),
            ResilienceLabLoopback.Case(
                $"{ScenarioName}.frontier-moves-past-gap",
                true,
                checkpoint is not null && !string.Equals(checkpoint.FrontierCursor, expiredCursor, StringComparison.Ordinal)),
            ResilienceLabLoopback.Case($"{ScenarioName}.resume-after-snapshot-events", 1, resumed.Events.Count),
            ResilienceLabLoopback.Case($"{ScenarioName}.resume-after-snapshot-value", ResumedValue, resumedValue),
        ];
    }

    /// <summary>Publishes one G-counter value.</summary>
    /// <param name="hub">The server hub.</param>
    /// <param name="value">The counter value, also used as the client sequence.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The publish task.</returns>
    private static async ValueTask PublishAsync(ServerStreamHub hub, long value, CancellationToken cancellationToken)
    {
        var seed = SeedBase + (int)value;
        _ = await hub.ApplyOperationsAsync(
            ResilienceLabLoopback.CreateBatch(seed, seed + SeedBase, Stream, value, CrdtMutation.GCounterSet(ClientId, value)),
            Client,
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Reads the next page for the lab subscription.</summary>
    /// <param name="hub">The server hub.</param>
    /// <param name="cursor">The resume cursor, or null to start at the beginning.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The next receive page.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ValueTask<RemoteEventBatch> ReadPageAsync(
        ServerStreamHub hub,
        string? cursor,
        CancellationToken cancellationToken) =>
        ResilienceLabLoopback.ReadFirstPageAsync(
            hub.SubscribeStreamAsync(new(Stream, Subscription, cursor, StartPosition.FromSequence(0)), Client, cancellationToken),
            cancellationToken);

    /// <summary>Tries to resume from the expired cursor and reports the typed reason code.</summary>
    /// <param name="hub">The server hub.</param>
    /// <param name="cursor">The expired cursor.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The retention gap reason code, or the resumed outcome.</returns>
    private static async ValueTask<string> TryResumeAsync(ServerStreamHub hub, string cursor, CancellationToken cancellationToken)
    {
        try
        {
            _ = await ReadPageAsync(hub, cursor, cancellationToken).ConfigureAwait(false);
            return Resumed;
        }
        catch (RemoteSubscriptionRetentionGapException exception)
        {
            return exception.ReasonCode ?? string.Empty;
        }
    }

    /// <summary>Creates the snapshot recovery request for the expired cursor.</summary>
    /// <param name="expiredCursor">The expired cursor.</param>
    /// <returns>The recovery request.</returns>
    private static RemoteSnapshotRecoveryRequest CreateRecoveryRequest(string expiredCursor) =>
        new()
        {
            StreamId = Stream,
            SubscriptionId = Subscription,
            ExpiredCursor = expiredCursor,
            ClientStateContractId = CrdtContracts.StateContractId,
            ClientStateSchemaVersion = CrdtContracts.SchemaVersion,
            SnapshotFormatVersion = 1,
            PendingOperations = [],
            MaximumResponseBytes = MaximumResponseBytes,
        };
}
