// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Crdt;
using ReactiveUI.Primitives.OccasionallyConnected.Server;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

/// <summary>Demonstrates that a peer without exactly-once capabilities is rejected instead of silently downgraded.</summary>
internal static class CapabilityDowngradeScenario
{
    /// <summary>The command-line scenario name.</summary>
    internal const string ScenarioName = "capability-downgrade";

    /// <summary>The trusted client identifier.</summary>
    private const string ClientId = "device-a";

    /// <summary>The outcome recorded when no session was created.</summary>
    private const string NoSession = "no-session";

    /// <summary>The outcome recorded when a session was created.</summary>
    private const string SessionCreated = "session";

    /// <summary>The outcome recorded when a feature is absent.</summary>
    private const string Absent = "absent";

    /// <summary>The outcome recorded when a feature is present.</summary>
    private const string Present = "present";

    /// <summary>The deterministic batch seed.</summary>
    private const int BatchSeed = 401;

    /// <summary>The deterministic operation seed.</summary>
    private const int OperationSeed = 402;

    /// <summary>The features a peer needs for exactly-once delivery.</summary>
    private const RemoteTransportCapabilities ExactlyOnceFeatures = RemoteTransportCapabilities.ServerIdempotency
        | RemoteTransportCapabilities.AtomicApplyAndAcknowledge
        | RemoteTransportCapabilities.ReceiveAcknowledgements;

    /// <summary>The stream used by the at-least-once push.</summary>
    private static readonly StreamId Stream = new("resilience/capability");

    /// <summary>Runs the scenario.</summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The scenario invariants.</returns>
    internal static async ValueTask<IReadOnlyList<ResilienceLabCaseResult>> RunAsync(CancellationToken cancellationToken)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(ResilienceLabLoopback.GuardTimeout);
        var token = timeout.Token;
        var clock = new ResilienceLabClock(ResilienceLabLoopback.InitialTime);
        await using var hub = ServerStreamHub.CreateInMemory(ResilienceLabLoopback.CreateHubOptions(
            clock,
            ResilienceLabLoopback.CreateJournalLimits(TimeSpan.FromMinutes(ResilienceLabLoopback.GuardTimeoutSeconds)),
            Stream));
        var limitedFeatures = CrdtLoopbackScenarioShape.VolatileLoopbackCapabilities;

        var exactlyOnce = await TryConnectAsync(hub, limitedFeatures, DeliveryGuarantee.ExactlyOnce, token).ConfigureAwait(false);
        var fullPeer = await TryConnectAsync(hub, limitedFeatures | ExactlyOnceFeatures, DeliveryGuarantee.ExactlyOnce, token)
            .ConfigureAwait(false);

        await using var adapter = new LoopbackTransportAdapter(
            ResilienceLabLoopback.CreateLoopbackOptions(hub, ClientId, limitedFeatures));
        await using var session = await adapter.ConnectAsync(
            ResilienceLabLoopback.CreateConnectRequest(ClientId, DeliveryGuarantee.AtLeastOnce),
            token).ConfigureAwait(false);
        var push = await session.PushAsync(
            ResilienceLabLoopback.CreateBatch(BatchSeed, OperationSeed, Stream, 1, CrdtMutation.GCounterSet(ClientId, 1)),
            token).ConfigureAwait(false);
        var atomicAcknowledge = (session.NegotiatedCapabilities.Features & RemoteTransportCapabilities.AtomicApplyAndAcknowledge) != 0;

        return
        [
            ResilienceLabLoopback.Case($"{ScenarioName}.exactly-once-rejected", nameof(InvalidOperationException), exactlyOnce.Failure),
            ResilienceLabLoopback.Case($"{ScenarioName}.exactly-once-no-session", NoSession, exactlyOnce.Outcome),
            ResilienceLabLoopback.Case($"{ScenarioName}.exactly-once-capable-peer-connects", SessionCreated, fullPeer.Outcome),
            ResilienceLabLoopback.Case(
                $"{ScenarioName}.at-least-once-accepted",
                OperationResultKind.Accepted,
                push.Operations[0].Kind),
            ResilienceLabLoopback.Case(
                $"{ScenarioName}.at-least-once-no-silent-upgrade",
                Absent,
                atomicAcknowledge ? Present : Absent),
        ];
    }

    /// <summary>Tries to connect with one required guarantee and reports the typed outcome.</summary>
    /// <param name="hub">The server hub.</param>
    /// <param name="features">The peer features.</param>
    /// <param name="guarantee">The required delivery guarantee.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The connection outcome.</returns>
    private static async ValueTask<ConnectOutcome> TryConnectAsync(
        IServerStreamHub hub,
        RemoteTransportCapabilities features,
        DeliveryGuarantee guarantee,
        CancellationToken cancellationToken)
    {
        await using var adapter = new LoopbackTransportAdapter(ResilienceLabLoopback.CreateLoopbackOptions(hub, ClientId, features));
        try
        {
            await using var session = await adapter.ConnectAsync(
                ResilienceLabLoopback.CreateConnectRequest(ClientId, guarantee),
                cancellationToken).ConfigureAwait(false);
            return new(SessionCreated, string.Empty);
        }
        catch (InvalidOperationException exception)
        {
            return new(NoSession, exception.GetType().Name);
        }
    }

    /// <summary>Describes one connection attempt.</summary>
    /// <param name="Outcome">Whether a session was created.</param>
    /// <param name="Failure">The typed failure name, or empty when the connection succeeded.</param>
    private readonly record struct ConnectOutcome(string Outcome, string Failure);
}
