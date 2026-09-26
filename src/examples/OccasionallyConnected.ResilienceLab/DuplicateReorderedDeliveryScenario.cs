// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Text;
using ReactiveUI.Primitives.OccasionallyConnected.Crdt;
using ReactiveUI.Primitives.OccasionallyConnected.Server;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

/// <summary>Demonstrates that duplicate batches keep one canonical effect and that reordered CRDT delivery converges.</summary>
internal static class DuplicateReorderedDeliveryScenario
{
    /// <summary>The command-line scenario name.</summary>
    internal const string ScenarioName = "duplicate-reordered-delivery";

    /// <summary>The writing client identifier.</summary>
    private const string ClientAId = "device-a";

    /// <summary>The second OR-set replica identifier.</summary>
    private const string ClientBId = "device-b";

    /// <summary>The third OR-set replica identifier.</summary>
    private const string ClientCId = "device-c";

    /// <summary>The G-counter component pushed twice.</summary>
    private const int CounterValue = 5;

    /// <summary>The client sequence of the observed remove.</summary>
    private const int RemoveSequence = 2;

    /// <summary>The deterministic batch seed.</summary>
    private const int BatchSeed = 301;

    /// <summary>The deterministic operation seed.</summary>
    private const int OperationSeed = 302;

    /// <summary>The deterministic subscription seed.</summary>
    private const int SubscriptionSeed = 303;

    /// <summary>The blue OR-set element.</summary>
    private const string Blue = "blue";

    /// <summary>The red OR-set element that is removed after it is observed.</summary>
    private const string Red = "red";

    /// <summary>The green OR-set element.</summary>
    private const string Green = "green";

    /// <summary>The expected converged OR-set display.</summary>
    private const string ExpectedElements = "blue,green";

    /// <summary>The stream that receives the duplicated batch.</summary>
    private static readonly StreamId Stream = new("resilience/duplicate");

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
        var client = new ServerAuthenticatedClient(ResilienceLabLoopback.TenantId, ClientAId);
        var batch = ResilienceLabLoopback.CreateBatch(
            BatchSeed,
            OperationSeed,
            Stream,
            1,
            CrdtMutation.GCounterSet(ClientAId, CounterValue));

        var original = await hub.ApplyOperationsAsync(batch, client, token).ConfigureAwait(false);
        var duplicate = await hub.ApplyOperationsAsync(batch, client, token).ConfigureAwait(false);
        var page = await ResilienceLabLoopback.ReadFirstPageAsync(
            hub.SubscribeStreamAsync(
                new(Stream, new(ResilienceLabLoopback.CreateGuid(SubscriptionSeed)), null, StartPosition.FromSequence(0)),
                client,
                token),
            token).ConfigureAwait(false);
        var canonicalState = CrdtLoopbackReceiver.DecodeStates(page, ResilienceLabLoopback.Bounds)[^1];

        List<ResilienceLabCaseResult> cases =
        [
            ResilienceLabLoopback.Case($"{ScenarioName}.duplicate-returns-original-result", Describe(original), Describe(duplicate)),
            ResilienceLabLoopback.Case($"{ScenarioName}.canonical-event-count", 1, page.Events.Count),
            ResilienceLabLoopback.Case(
                $"{ScenarioName}.canonical-event-caused-by-operation",
                batch.Operations[0].OperationId.Value,
                page.Events[0].CausedByOperationId?.Value ?? Guid.Empty),
            ResilienceLabLoopback.Case($"{ScenarioName}.canonical-counter-value", (long)CounterValue, canonicalState.Value.Counter),
        ];
        cases.AddRange(CreateReorderCases());
        return cases;
    }

    /// <summary>Creates the CRDT reorder invariants.</summary>
    /// <returns>The reorder invariants.</returns>
    private static List<ResilienceLabCaseResult> CreateReorderCases()
    {
        var bounds = ResilienceLabLoopback.Bounds;
        var empty = CrdtFunctions.Empty(CrdtKind.ORSet);
        var blue = CrdtFunctions.ApplyLocal(empty, AddInput(Blue), ClientAId, 1, bounds);
        var red = CrdtFunctions.ApplyLocal(empty, AddInput(Red), ClientBId, 1, bounds);
        var green = CrdtFunctions.ApplyLocal(empty, AddInput(Green), ClientCId, 1, bounds);
        var observed = CrdtFunctions.Merge(blue, red, bounds);
        var removeRed = CrdtFunctions.ApplyLocal(
            observed,
            CrdtInput.ForMutation(CrdtMutation.ORSetRemove(ToBytes(Red), [CrdtLoopbackORSetProjection.FindObservedDot(observed, Red)])),
            ClientAId,
            RemoveSequence,
            bounds);

        var forward = Merge(bounds, blue, red, green, removeRed);
        var reverse = Merge(bounds, removeRed, green, red, blue);
        var duplicated = Merge(bounds, reverse, red, red, removeRed);

        return
        [
            ResilienceLabLoopback.Case($"{ScenarioName}.orset-forward-order", ExpectedElements, Display(forward)),
            ResilienceLabLoopback.Case($"{ScenarioName}.orset-reverse-order", ExpectedElements, Display(reverse)),
            ResilienceLabLoopback.Case($"{ScenarioName}.orset-duplicate-delivery", ExpectedElements, Display(duplicated)),
            ResilienceLabLoopback.Case($"{ScenarioName}.orset-orders-equal", true, forward.Value.Elements.Count == reverse.Value.Elements.Count
                && string.Equals(Display(forward), Display(reverse), StringComparison.Ordinal)),
        ];
    }

    /// <summary>Merges replica states from left to right.</summary>
    /// <param name="bounds">The CRDT bounds.</param>
    /// <param name="states">The replica states in delivery order.</param>
    /// <returns>The merged state.</returns>
    private static CrdtState Merge(CrdtBounds bounds, params CrdtState[] states)
    {
        var merged = states[0];
        for (var index = 1; index < states.Length; index++)
        {
            merged = CrdtFunctions.Merge(merged, states[index], bounds);
        }

        return merged;
    }

    /// <summary>Describes a server result by its first operation outcome and version.</summary>
    /// <param name="result">The server result.</param>
    /// <returns>The stable description.</returns>
    private static string Describe(ServerSyncResult result) =>
        $"{result.Result.Operations[0].Kind}@{result.Result.Operations[0].ServerVersion}";

    /// <summary>Creates an OR-set add input.</summary>
    /// <param name="element">The element text.</param>
    /// <returns>The CRDT input.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static CrdtInput AddInput(string element) =>
        CrdtInput.ForMutation(CrdtMutation.ORSetAdd(ToBytes(element)));

    /// <summary>Gets the sorted element display of an OR-set state.</summary>
    /// <param name="state">The OR-set state.</param>
    /// <returns>The sorted element display.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string Display(CrdtState state) =>
        CrdtLoopbackORSetProjection.GetElementDisplay(state);

    /// <summary>Converts text to UTF-8 bytes.</summary>
    /// <param name="value">The text.</param>
    /// <returns>The UTF-8 bytes.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static byte[] ToBytes(string value) => Encoding.UTF8.GetBytes(value);
}
