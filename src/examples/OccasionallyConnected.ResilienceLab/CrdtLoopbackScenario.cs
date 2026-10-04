// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected.Crdt;
using ReactiveUI.Primitives.OccasionallyConnected.Server;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

/// <summary>Runs the bounded in-memory CRDT loopback demonstration.</summary>
internal static partial class CrdtLoopbackScenario
{
    /// <summary>The finite application receive event bound.</summary>
    internal const int MaximumReceiveEvents = MaximumEvents;

    /// <summary>The finite application completed operation bound for receive pages.</summary>
    internal const int MaximumCompletedOperations = MaximumEvents;

    /// <summary>The trusted tenant identifier used by the loopback host boundary.</summary>
    private const string TenantId = "resilience-lab";

    /// <summary>The first trusted client identifier.</summary>
    private const string ClientAId = "device-a";

    /// <summary>The second trusted client identifier.</summary>
    private const string ClientBId = "device-b";

    /// <summary>The subscription rewind diagnostic fragment.</summary>
    private const string RewindDiagnosticFragment = "rewind";

    /// <summary>The shared receive frontier outcome.</summary>
    private const string SameFrontier = "same";

    /// <summary>The divergent receive frontier outcome.</summary>
    private const string DifferentFrontier = "different";

    /// <summary>The blue OR-set element.</summary>
    private const string Blue = "blue";

    /// <summary>The red OR-set element removed by observed dot.</summary>
    private const string Red = "red";

    /// <summary>The first LWW register value.</summary>
    private const string First = "first";

    /// <summary>The second LWW register value.</summary>
    private const string Second = "second";

    /// <summary>The expected G-counter total.</summary>
    private const int ExpectedGCounter = 8;

    /// <summary>The expected PN-counter total.</summary>
    private const int ExpectedPNCounter = 2;

    /// <summary>The G-counter value for client A.</summary>
    private const int GCounterClientA = 5;

    /// <summary>The G-counter value for client B.</summary>
    private const int GCounterClientB = 3;

    /// <summary>The PN-counter positive component for client A.</summary>
    private const int PNCounterClientAPositive = 7;

    /// <summary>The PN-counter negative component for client A.</summary>
    private const int PNCounterClientANegative = 2;

    /// <summary>The PN-counter positive component for client B.</summary>
    private const int PNCounterClientBPositive = 1;

    /// <summary>The PN-counter negative component for client B.</summary>
    private const int PNCounterClientBNegative = 4;

    /// <summary>The first ACK probe counter value.</summary>
    private const int AckProbeInitialCounter = 1;

    /// <summary>The next ACK probe counter value.</summary>
    private const int AckProbeNextCounter = 2;

    /// <summary>The expected event count for an ACK resume page.</summary>
    private const int ExpectedAckResumeEventCount = 1;

    /// <summary>The expected event delta after replaying a duplicate operation.</summary>
    private const int ExpectedDuplicateEffectDelta = 0;

    /// <summary>The first client sequence on a stream.</summary>
    private const int FirstSequence = 1;

    /// <summary>The second client sequence on a stream.</summary>
    private const int SecondSequence = 2;

    /// <summary>The number of hex digits in the deterministic GUID tail.</summary>
    private const string GuidSeedFormat = "x12";

    /// <summary>The deterministic GUID prefix used by the lab.</summary>
    private const string GuidPrefix = "00000000-0000-0000-0000-";

    /// <summary>The scenario timeout guard in seconds.</summary>
    private const int ScenarioTimeoutSeconds = 5;

    /// <summary>The deterministic server clock tick in milliseconds.</summary>
    private const int ServerTickMilliseconds = 1;

    /// <summary>The finite CRDT counter component bound.</summary>
    private const int MaximumCounterComponents = 4;

    /// <summary>The finite OR-set dot binding bound.</summary>
    private const int MaximumDots = 8;

    /// <summary>The finite CRDT element bound.</summary>
    private const int MaximumElements = 4;

    /// <summary>The finite CRDT element byte bound.</summary>
    private const int MaximumElementBytes = 32;

    /// <summary>The finite CRDT register byte bound.</summary>
    private const int MaximumRegisterBytes = 32;

    /// <summary>The finite CRDT encoded payload byte bound.</summary>
    private const int MaximumEncodedBytes = 1024;

    /// <summary>The finite client id UTF-8 byte bound.</summary>
    private const int MaximumClientIdBytes = 64;

    /// <summary>The finite transport batch operation bound.</summary>
    private const int MaximumBatchOperations = 16;

    /// <summary>The finite transport byte bound.</summary>
    private const int MaximumBatchBytes = 8192;

    /// <summary>The finite retained journal byte bound.</summary>
    private const int MaximumJournalBytes = 32_768;

    /// <summary>The finite retained stream bound.</summary>
    private const int MaximumStreams = 6;

    /// <summary>The finite retained ledger entry bound.</summary>
    private const int MaximumLedgerEntries = 32;

    /// <summary>The finite retained event bound.</summary>
    private const int MaximumEvents = 32;

    /// <summary>The finite retained operation capture bound.</summary>
    private const int MaximumOperationCaptureCount = 32;

    /// <summary>The finite subscription bound.</summary>
    private const int MaximumSubscriptions = 16;

    /// <summary>The finite operation retention in minutes.</summary>
    private const int OperationRetentionMinutes = 30;

    /// <summary>The finite subscription retention in minutes.</summary>
    private const int SubscriptionRetentionMinutes = 30;

    /// <summary>The server idempotency retention in minutes.</summary>
    private const int ServerIdempotencyRetentionMinutes = 30;

    /// <summary>The client inbox retention requirement in minutes.</summary>
    private const int ClientInboxRetentionMinutes = 30;

    /// <summary>The future client diagnostic timestamp day offset.</summary>
    private const int FutureClientTimestampDays = 1;

    /// <summary>The older client diagnostic timestamp day offset.</summary>
    private const int OlderClientTimestampDays = -1;

    /// <summary>The G-counter client A batch seed.</summary>
    private const int GCounterClientABatchSeed = 101;

    /// <summary>The G-counter client B batch seed.</summary>
    private const int GCounterClientBBatchSeed = 102;

    /// <summary>The PN-counter client B batch seed.</summary>
    private const int PNCounterClientBBatchSeed = 103;

    /// <summary>The PN-counter client A batch seed.</summary>
    private const int PNCounterClientABatchSeed = 104;

    /// <summary>The OR-set blue add batch seed.</summary>
    private const int ORSetBlueAddBatchSeed = 105;

    /// <summary>The OR-set red add batch seed.</summary>
    private const int ORSetRedAddBatchSeed = 106;

    /// <summary>The OR-set red remove batch seed.</summary>
    private const int ORSetRedRemoveBatchSeed = 107;

    /// <summary>The LWW first write batch seed.</summary>
    private const int LwwFirstBatchSeed = 108;

    /// <summary>The LWW second write batch seed.</summary>
    private const int LwwSecondBatchSeed = 109;

    /// <summary>The client A ACK probe first batch seed.</summary>
    private const int AckProbeClientAFirstBatchSeed = 110;

    /// <summary>The client A ACK probe second batch seed.</summary>
    private const int AckProbeClientASecondBatchSeed = 111;

    /// <summary>The client B ACK probe first batch seed.</summary>
    private const int AckProbeClientBFirstBatchSeed = 112;

    /// <summary>The client B ACK probe second batch seed.</summary>
    private const int AckProbeClientBSecondBatchSeed = 113;

    /// <summary>The G-counter client A operation seed.</summary>
    private const int GCounterClientAOperationSeed = 201;

    /// <summary>The G-counter client B operation seed.</summary>
    private const int GCounterClientBOperationSeed = 202;

    /// <summary>The PN-counter client B operation seed.</summary>
    private const int PNCounterClientBOperationSeed = 203;

    /// <summary>The PN-counter client A operation seed.</summary>
    private const int PNCounterClientAOperationSeed = 204;

    /// <summary>The OR-set blue add operation seed.</summary>
    private const int ORSetBlueAddOperationSeed = 205;

    /// <summary>The OR-set red add operation seed.</summary>
    private const int ORSetRedAddOperationSeed = 206;

    /// <summary>The OR-set red remove operation seed.</summary>
    private const int ORSetRedRemoveOperationSeed = 207;

    /// <summary>The LWW first write operation seed.</summary>
    private const int LwwFirstOperationSeed = 208;

    /// <summary>The LWW second write operation seed.</summary>
    private const int LwwSecondOperationSeed = 209;

    /// <summary>The client A ACK probe first operation seed.</summary>
    private const int AckProbeClientAFirstOperationSeed = 210;

    /// <summary>The client A ACK probe second operation seed.</summary>
    private const int AckProbeClientASecondOperationSeed = 211;

    /// <summary>The client B ACK probe first operation seed.</summary>
    private const int AckProbeClientBFirstOperationSeed = 212;

    /// <summary>The client B ACK probe second operation seed.</summary>
    private const int AckProbeClientBSecondOperationSeed = 213;

    /// <summary>The OR-set observed-dot subscription seed.</summary>
    private const int ORSetObservedSubscriptionSeed = 901;

    /// <summary>The OR-set before-duplicate subscription seed.</summary>
    private const int ORSetBeforeDuplicateSubscriptionSeed = 902;

    /// <summary>The OR-set after-duplicate subscription seed.</summary>
    private const int ORSetAfterDuplicateSubscriptionSeed = 903;

    /// <summary>The client A ACK probe subscription seed.</summary>
    private const int AckProbeClientASubscriptionSeed = 904;

    /// <summary>The client B ACK probe subscription seed.</summary>
    private const int AckProbeClientBSubscriptionSeed = 905;

    /// <summary>The final client A subscription seed base.</summary>
    private const int ClientAReceiveSeedBase = 910;

    /// <summary>The final client B subscription seed base.</summary>
    private const int ClientBReceiveSeedBase = 950;

    /// <summary>The PN-counter receive subscription offset.</summary>
    private const int PNCounterReceiveSeedOffset = 1;

    /// <summary>The OR-set receive subscription offset.</summary>
    private const int ORSetReceiveSeedOffset = 2;

    /// <summary>The LWW receive subscription offset.</summary>
    private const int LwwReceiveSeedOffset = 3;

    /// <summary>The G-counter stream identifier.</summary>
    private static readonly StreamId GCounterStream = new("resilience/gcounter");

    /// <summary>The PN-counter stream identifier.</summary>
    private static readonly StreamId PNCounterStream = new("resilience/pncounter");

    /// <summary>The OR-set stream identifier.</summary>
    private static readonly StreamId ORSetStream = new("resilience/orset");

    /// <summary>The LWW register stream identifier.</summary>
    private static readonly StreamId LwwStream = new("resilience/lww");

    /// <summary>The client A ACK probe stream identifier.</summary>
    private static readonly StreamId AckProbeClientAStream = new("resilience/ack/device-a");

    /// <summary>The client B ACK probe stream identifier.</summary>
    private static readonly StreamId AckProbeClientBStream = new("resilience/ack/device-b");

    /// <summary>The trusted client A principal.</summary>
    private static readonly ServerAuthenticatedClient ClientA = new(TenantId, ClientAId);

    /// <summary>The trusted client B principal.</summary>
    private static readonly ServerAuthenticatedClient ClientB = new(TenantId, ClientBId);

    /// <summary>The deterministic initial server time.</summary>
    private static readonly DateTimeOffset InitialServerTime = DateTimeOffset.Parse(
        "2026-09-13T00:00:00Z",
        CultureInfo.InvariantCulture,
        DateTimeStyles.AssumeUniversal);

    /// <summary>The future client diagnostic timestamp that the server must not trust for LWW ordering.</summary>
    private static readonly DateTimeOffset FutureClientTimestamp = InitialServerTime.AddDays(FutureClientTimestampDays);

    /// <summary>The older client diagnostic timestamp that still wins when the server commit stamp is later.</summary>
    private static readonly DateTimeOffset OlderClientTimestamp = InitialServerTime.AddDays(OlderClientTimestampDays);

    /// <summary>Runs the scenario and returns typed invariant results.</summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The invariant results.</returns>
    internal static async ValueTask<IReadOnlyList<ResilienceLabCaseResult>> RunAsync(
        CancellationToken cancellationToken)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(ScenarioTimeoutSeconds));
        var token = timeout.Token;
        var bounds = CreateBounds();
        var clock = new DeterministicTimeProvider(InitialServerTime);
        await using var hub = ServerStreamHub.CreateInMemory(CreateHubOptions(clock, bounds));
        await using var adapterA = new LoopbackTransportAdapter(CreateLoopbackOptions(hub, ClientA));
        await using var adapterB = new LoopbackTransportAdapter(CreateLoopbackOptions(hub, ClientB));
        await using var sessionA = await adapterA.ConnectAsync(
            CreateConnectRequest(ClientAId),
            token).ConfigureAwait(false);
        await using var sessionB = await adapterB.ConnectAsync(
            CreateConnectRequest(ClientBId),
            token).ConfigureAwait(false);

        await PushCounterOperationsAsync(sessionA, sessionB, bounds, token).ConfigureAwait(false);
        await PushPNCounterOperationsAsync(sessionA, sessionB, bounds, token).ConfigureAwait(false);
        var duplicateDelta = await PushORSetOperationsAsync(sessionA, sessionB, bounds, token).ConfigureAwait(false);
        await PushLwwOperationsAsync(sessionA, sessionB, clock, bounds, token).ConfigureAwait(false);
        var clientAAcknowledged = await ProveResumeAfterAckAsync(
            sessionA,
            CreateClientAAckProbe(),
            bounds,
            token).ConfigureAwait(false);
        var clientBAcknowledged = await ProveResumeAfterAckAsync(
            sessionB,
            CreateClientBAckProbe(),
            bounds,
            token).ConfigureAwait(false);

        var clientAStates = await ReceiveAllStreamsAsync(
            sessionA,
            ClientAReceiveSeedBase,
            bounds,
            token).ConfigureAwait(false);
        var clientBStates = await ReceiveAllStreamsAsync(
            sessionB,
            ClientBReceiveSeedBase,
            bounds,
            token).ConfigureAwait(false);
        return CreateCaseResults(new(
            clientAStates,
            clientBStates,
            CreateValueSnapshot(clientAStates),
            CreateValueSnapshot(clientBStates),
            clientAAcknowledged,
            clientBAcknowledged,
            duplicateDelta,
            bounds));
    }

    /// <summary>Creates a value snapshot from received states.</summary>
    /// <param name="states">The received states.</param>
    /// <returns>The value snapshot.</returns>
    private static ReceivedValueSnapshot CreateValueSnapshot(CrdtLoopbackReceivedStates states) =>
        new(
            GetCounterValue(states.GCounter.States[^1]),
            GetCounterValue(states.PNCounter.States[^1]),
            CrdtLoopbackORSetProjection.GetElementDisplay(states.ORSet.States[^1]),
            GetRegisterString(states.Lww.States[^1]));

    /// <summary>Creates expected-versus-actual case results.</summary>
    /// <param name="context">The case build context.</param>
    /// <returns>The case results.</returns>
    private static List<ResilienceLabCaseResult> CreateCaseResults(CaseBuildContext context)
    {
        List<ResilienceLabCaseResult> cases = [];
        AppendGCounterCases(cases, context);
        AppendPNCounterCases(cases, context);
        AppendORSetCases(cases, context);
        AppendLwwCases(cases, context);
        AppendReceiveCases(cases, context);
        AppendCase(
            cases,
            "journal.duplicate-operation-adds-no-effect-group",
            ExpectedDuplicateEffectDelta,
            context.DuplicateDelta);
        return cases;
    }

    /// <summary>Appends G-counter case results.</summary>
    /// <param name="cases">The case list.</param>
    /// <param name="context">The case build context.</param>
    private static void AppendGCounterCases(List<ResilienceLabCaseResult> cases, CaseBuildContext context)
    {
        AppendCase(
            cases,
            "gcounter.client-a-receives-authoritative-value",
            ExpectedGCounter,
            context.ClientAValues.GCounter);
        AppendCase(
            cases,
            "gcounter.client-b-receives-authoritative-value",
            ExpectedGCounter,
            context.ClientBValues.GCounter);
        AppendCase(
            cases,
            "gcounter.clients-share-authoritative-frontier",
            SameFrontier,
            CrdtLoopbackConvergenceEvaluator.EvaluateFrontier<int>(
                new(0, context.ClientAStates.GCounter.Cursor),
                new(0, context.ClientBStates.GCounter.Cursor),
                SameFrontier,
                DifferentFrontier));
        AppendCase(
            cases,
            "gcounter.converges-and-replay-is-idempotent",
            ExpectedGCounter,
            CrdtLoopbackConvergenceEvaluator.EvaluateCounter(
                new(context.ClientAValues.GCounter, context.ClientAStates.GCounter.Cursor),
                new(context.ClientBValues.GCounter, context.ClientBStates.GCounter.Cursor),
                ExpectedGCounter));
        AppendCase(
            cases,
            "gcounter.received-history-forward-reverse-commutes",
            ExpectedGCounter,
            CrdtLoopbackHistoryEvaluator.EvaluateCounter(
                context.ClientAStates.GCounter,
                context.ClientBStates.GCounter,
                CrdtKind.GCounter,
                ExpectedGCounter,
                context.Bounds));
    }

    /// <summary>Appends PN-counter case results.</summary>
    /// <param name="cases">The case list.</param>
    /// <param name="context">The case build context.</param>
    private static void AppendPNCounterCases(List<ResilienceLabCaseResult> cases, CaseBuildContext context)
    {
        AppendCase(
            cases,
            "pncounter.client-a-receives-authoritative-value",
            ExpectedPNCounter,
            context.ClientAValues.PNCounter);
        AppendCase(
            cases,
            "pncounter.client-b-receives-authoritative-value",
            ExpectedPNCounter,
            context.ClientBValues.PNCounter);
        AppendCase(
            cases,
            "pncounter.clients-share-authoritative-frontier",
            SameFrontier,
            CrdtLoopbackConvergenceEvaluator.EvaluateFrontier<int>(
                new(0, context.ClientAStates.PNCounter.Cursor),
                new(0, context.ClientBStates.PNCounter.Cursor),
                SameFrontier,
                DifferentFrontier));
        AppendCase(
            cases,
            "pncounter.converges-after-different-receive-order",
            ExpectedPNCounter,
            CrdtLoopbackConvergenceEvaluator.EvaluateCounter(
                new(context.ClientAValues.PNCounter, context.ClientAStates.PNCounter.Cursor),
                new(context.ClientBValues.PNCounter, context.ClientBStates.PNCounter.Cursor),
                ExpectedPNCounter));
        AppendCase(
            cases,
            "pncounter.received-history-forward-reverse-commutes",
            ExpectedPNCounter,
            CrdtLoopbackHistoryEvaluator.EvaluateCounter(
                context.ClientAStates.PNCounter,
                context.ClientBStates.PNCounter,
                CrdtKind.PNCounter,
                ExpectedPNCounter,
                context.Bounds));
    }

    /// <summary>Appends OR-set case results.</summary>
    /// <param name="cases">The case list.</param>
    /// <param name="context">The case build context.</param>
    private static void AppendORSetCases(List<ResilienceLabCaseResult> cases, CaseBuildContext context)
    {
        AppendCase(cases, "orset.client-a-receives-authoritative-value", Blue, context.ClientAValues.ORSet);
        AppendCase(cases, "orset.client-b-receives-authoritative-value", Blue, context.ClientBValues.ORSet);
        AppendCase(
            cases,
            "orset.clients-share-authoritative-frontier",
            SameFrontier,
            CrdtLoopbackConvergenceEvaluator.EvaluateFrontier<string>(
                new(string.Empty, context.ClientAStates.ORSet.Cursor),
                new(string.Empty, context.ClientBStates.ORSet.Cursor),
                SameFrontier,
                DifferentFrontier));
        AppendCase(
            cases,
            "orset.observed-remove-and-duplicate-add-do-not-resurrect",
            Blue,
            CrdtLoopbackConvergenceEvaluator.EvaluateString(
                new(context.ClientAValues.ORSet, context.ClientAStates.ORSet.Cursor),
                new(context.ClientBValues.ORSet, context.ClientBStates.ORSet.Cursor),
                Blue));
        AppendCase(
            cases,
            "orset.received-history-forward-reverse-commutes",
            Blue,
            CrdtLoopbackHistoryEvaluator.EvaluateString(
                context.ClientAStates.ORSet,
                context.ClientBStates.ORSet,
                CrdtKind.ORSet,
                CrdtLoopbackORSetProjection.GetElementDisplay,
                Blue,
                context.Bounds));
    }

    /// <summary>Appends LWW register case results.</summary>
    /// <param name="cases">The case list.</param>
    /// <param name="context">The case build context.</param>
    private static void AppendLwwCases(List<ResilienceLabCaseResult> cases, CaseBuildContext context)
    {
        AppendCase(cases, "lww.client-a-receives-authoritative-value", Second, context.ClientAValues.Lww);
        AppendCase(cases, "lww.client-b-receives-authoritative-value", Second, context.ClientBValues.Lww);
        AppendCase(
            cases,
            "lww.clients-share-authoritative-frontier",
            SameFrontier,
            CrdtLoopbackConvergenceEvaluator.EvaluateFrontier<string>(
                new(string.Empty, context.ClientAStates.Lww.Cursor),
                new(string.Empty, context.ClientBStates.Lww.Cursor),
                SameFrontier,
                DifferentFrontier));
        AppendCase(
            cases,
            "lww.later-server-stamp-wins-after-older-exact-retry",
            Second,
            CrdtLoopbackConvergenceEvaluator.EvaluateString(
                new(context.ClientAValues.Lww, context.ClientAStates.Lww.Cursor),
                new(context.ClientBValues.Lww, context.ClientBStates.Lww.Cursor),
                Second));
        AppendCase(
            cases,
            "lww.received-history-forward-reverse-commutes",
            Second,
            CrdtLoopbackHistoryEvaluator.EvaluateString(
                context.ClientAStates.Lww,
                context.ClientBStates.Lww,
                CrdtKind.LwwRegister,
                GetRegisterString,
                Second,
                context.Bounds));
    }

    /// <summary>Appends receive acknowledgement case results.</summary>
    /// <param name="cases">The case list.</param>
    /// <param name="context">The case build context.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void AppendReceiveCases(List<ResilienceLabCaseResult> cases, CaseBuildContext context) =>
        cases.AddRange(CrdtLoopbackAckReportBuilder.BuildCases(context.ClientAAcknowledged, context.ClientBAcknowledged));
}
