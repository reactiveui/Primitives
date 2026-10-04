// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected.Crdt;
using ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;
using ReactiveUI.Primitives.OccasionallyConnected.Server;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.Tests;

/// <summary>Tests for <see cref="CrdtLoopbackAckProbeWorkflow"/>.</summary>
public sealed class CrdtLoopbackAckProbeWorkflowTests
{
    /// <summary>The trusted client identifier.</summary>
    private const string ClientId = "device-a";

    /// <summary>The first cursor.</summary>
    private const string FirstCursor = "cursor-1";

    /// <summary>The second cursor.</summary>
    private const string SecondCursor = "cursor-2";

    /// <summary>The first expected counter value.</summary>
    private const int FirstValue = 1;

    /// <summary>The second expected counter value.</summary>
    private const int SecondValue = 2;

    /// <summary>The wrong counter value.</summary>
    private const int WrongValue = 3;

    /// <summary>The expected event count.</summary>
    private const int ExpectedEventCount = 1;

    /// <summary>The rewind diagnostic fragment.</summary>
    private const string RewindDiagnostic = "rewind";

    /// <summary>The expected single push count.</summary>
    private const int SinglePushCount = 1;

    /// <summary>The expected resumed push count.</summary>
    private const int ResumedPushCount = 2;

    /// <summary>The expected acknowledged first and stale page count.</summary>
    private const int StaleReadAcknowledgementCount = 2;

    /// <summary>The first event seed.</summary>
    private const int FirstEventSeed = 701;

    /// <summary>The stale retry event seed.</summary>
    private const int StaleEventSeed = 702;

    /// <summary>The second event seed.</summary>
    private const int SecondEventSeed = 703;

    /// <summary>The first operation seed.</summary>
    private const int FirstOperationSeed = 601;

    /// <summary>The second operation seed.</summary>
    private const int SecondOperationSeed = 602;

    /// <summary>The first batch seed.</summary>
    private const int FirstBatchSeed = 801;

    /// <summary>The second batch seed.</summary>
    private const int SecondBatchSeed = 802;

    /// <summary>The offset used to derive event-causing operation identifiers.</summary>
    private const int EventOperationSeedOffset = 100;

    /// <summary>The offset used to derive remote event batch identifiers.</summary>
    private const int EventBatchSeedOffset = 200;

    /// <summary>The deterministic GUID prefix.</summary>
    private const string GuidPrefix = "00000000-0000-0000-0000-";

    /// <summary>The deterministic GUID tail format.</summary>
    private const string GuidSeedFormat = "x12";

    /// <summary>The test stream identifier.</summary>
    private static readonly StreamId Stream = new("resilience/ack-workflow");

    /// <summary>The test subscription identifier.</summary>
    private static readonly SubscriptionId Subscription = new(Guid.Parse("00000000-0000-0000-0000-000000000501"));

    /// <summary>Verifies an unexpected first page shape fails the ACK probe workflow before the second push.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ProveResumeAfterAckReportsFalseWhenFirstPageIsMalformed()
    {
        await using var session = new ScriptedSession(
            [SubscribeOutcome.Batch(CreateEventBatch(null, FirstCursor, WrongValue, FirstEventSeed))]);
        var plan = CreatePlan();

        var actual = await CrdtLoopbackAckProbeWorkflow.ProveResumeAfterAckAsync(
            session,
            plan,
            CrdtBounds.Default,
            CancellationToken.None);

        await Assert.That(actual).IsFalse();
        await Assert.That(session.Pushes.Count).IsEqualTo(SinglePushCount);
        await Assert.That(session.Acknowledgements.Count).IsEqualTo(ExpectedEventCount);
    }

    /// <summary>Verifies a same-subscription stale initial read must be rejected before the second push.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ProveResumeAfterAckReportsFalseWhenStaleInitialReadIsNotRejected()
    {
        await using var session = new ScriptedSession(
            [
                SubscribeOutcome.Batch(CreateEventBatch(null, FirstCursor, FirstValue, FirstEventSeed)),
                SubscribeOutcome.Batch(CreateEventBatch(null, FirstCursor, FirstValue, StaleEventSeed)),
            ]);
        var plan = CreatePlan();

        var actual = await CrdtLoopbackAckProbeWorkflow.ProveResumeAfterAckAsync(
            session,
            plan,
            CrdtBounds.Default,
            CancellationToken.None);

        await Assert.That(actual).IsFalse();
        await Assert.That(session.Pushes.Count).IsEqualTo(SinglePushCount);
        await Assert.That(session.Acknowledgements.Count).IsEqualTo(StaleReadAcknowledgementCount);
    }

    /// <summary>Verifies an unexpected resumed page shape fails the ACK probe workflow after the second push.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ProveResumeAfterAckReportsFalseWhenSecondPageIsMalformed()
    {
        await using var session = new ScriptedSession(
            [
                SubscribeOutcome.Batch(CreateEventBatch(null, FirstCursor, FirstValue, FirstEventSeed)),
                SubscribeOutcome.Failure("rewind rejected"),
                SubscribeOutcome.Batch(CreateEventBatch(FirstCursor, SecondCursor, WrongValue, SecondEventSeed)),
            ]);
        var plan = CreatePlan();

        var actual = await CrdtLoopbackAckProbeWorkflow.ProveResumeAfterAckAsync(
            session,
            plan,
            CrdtBounds.Default,
            CancellationToken.None);

        await Assert.That(actual).IsFalse();
        await Assert.That(session.Pushes.Count).IsEqualTo(ResumedPushCount);
        await Assert.That(session.Acknowledgements.Count).IsEqualTo(StaleReadAcknowledgementCount);
    }

    /// <summary>Creates the ACK probe plan.</summary>
    /// <returns>The ACK probe plan.</returns>
    private static CrdtLoopbackAckProbePlan CreatePlan() =>
        new()
        {
            StreamId = Stream,
            SubscriptionId = Subscription,
            FirstBatch = CreateSyncBatch(FirstOperationSeed, FirstValue, FirstBatchSeed),
            SecondBatch = CreateSyncBatch(SecondOperationSeed, SecondValue, SecondBatchSeed),
            FirstValue = FirstValue,
            SecondValue = SecondValue,
            ExpectedEventCount = ExpectedEventCount,
            RewindDiagnosticFragment = RewindDiagnostic,
        };

    /// <summary>Creates a public sync batch for the ACK probe workflow.</summary>
    /// <param name="operationSeed">The deterministic operation id seed.</param>
    /// <param name="value">The counter value.</param>
    /// <param name="batchSeed">The deterministic batch id seed.</param>
    /// <returns>The sync batch.</returns>
    private static SyncBatch CreateSyncBatch(int operationSeed, int value, int batchSeed)
    {
        var operationId = new OperationId(CreateGuid(operationSeed));
        var payload = CrdtServerPayloads.CreateInput(
            CrdtInput.ForMutation(CrdtMutation.GCounterSet(ClientId, value)),
            CrdtBounds.Default);
        var operation = new SyncOperation
        {
            OperationId = operationId,
            StreamId = Stream,
            ClientSequence = value,
            TimestampUtc = DateTimeOffset.UnixEpoch,
            Type = SyncOperationType.Update,
            Payload = payload,
            Policy = OperationPolicy.Default,
        };
        return new(CreateGuid(batchSeed), [operation]);
    }

    /// <summary>Creates a public event batch carrying an authoritative counter state.</summary>
    /// <param name="previousCursor">The previous cursor.</param>
    /// <param name="nextCursor">The next cursor.</param>
    /// <param name="value">The authoritative counter value.</param>
    /// <param name="eventSeed">The deterministic event id seed.</param>
    /// <returns>The remote event batch.</returns>
    private static RemoteEventBatch CreateEventBatch(string? previousCursor, string nextCursor, int value, int eventSeed)
    {
        var operationId = new OperationId(CreateGuid(eventSeed + EventOperationSeedOffset));
        var eventId = CreateGuid(eventSeed);
        var payload = CrdtServerPayloads.CreateInput(CrdtInput.ForAuthoritativeState(CreateCounterState(value)), CrdtBounds.Default);
        var remoteEvent = new RemoteEvent(
            eventId,
            Stream,
            nextCursor,
            DateTimeOffset.UnixEpoch,
            operationId,
            payload,
            new Dictionary<string, string>()) { Origin = new(ClientId, operationId) };
        return new(
            CreateGuid(eventSeed + EventBatchSeedOffset),
            Stream,
            previousCursor,
            nextCursor,
            [remoteEvent]) { CompletedOperations = [new(new(ClientId, operationId), [eventId])] };
    }

    /// <summary>Creates a G-counter state with one component.</summary>
    /// <param name="value">The counter value.</param>
    /// <returns>The CRDT state.</returns>
    private static CrdtState CreateCounterState(int value) =>
        new() { Kind = CrdtKind.GCounter, GCounterComponents = new Dictionary<string, long> { [ClientId] = value } };

    /// <summary>Creates a deterministic GUID from a small positive seed.</summary>
    /// <param name="seed">The seed.</param>
    /// <returns>The deterministic GUID.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Guid CreateGuid(int seed) =>
        new($"{GuidPrefix}{seed.ToString(GuidSeedFormat, CultureInfo.InvariantCulture)}");

    /// <summary>Scripted public transport session for ACK workflow behavior.</summary>
    /// <param name="subscribeOutcomes">The subscribe outcomes to emit in order.</param>
    private sealed class ScriptedSession(IReadOnlyList<SubscribeOutcome> subscribeOutcomes) : IRemoteTransportSession
    {
        /// <summary>The test server version.</summary>
        private const string ServerVersion = "server-v1";

        /// <summary>The maximum negotiated payload size.</summary>
        private const int MaximumPayloadBytes = 1024;

        /// <summary>The next subscribe outcome index.</summary>
        private int _nextSubscribeIndex;

        /// <summary>Gets acknowledgements sent through the session.</summary>
        public List<ReceiveAcknowledgement> Acknowledgements { get; } = [];

        /// <summary>Gets batches pushed through the session.</summary>
        public List<SyncBatch> Pushes { get; } = [];

        /// <summary>Gets subscribe requests sent through the session.</summary>
        public List<RemoteSubscribeRequest> Requests { get; } = [];

        /// <inheritdoc/>
        public NegotiatedCapabilities NegotiatedCapabilities { get; } = new(
            new(1, 0),
            RemoteTransportCapabilities.None,
            1,
            MaximumPayloadBytes,
            null,
            null);

        /// <inheritdoc/>
        public ValueTask<RemoteSyncResult> PushAsync(SyncBatch batch, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Pushes.Add(batch);
            return new(CreateAcceptedResult(batch));
        }

        /// <inheritdoc/>
        public async IAsyncEnumerable<RemoteEventBatch> SubscribeAsync(
            RemoteSubscribeRequest request,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            Requests.Add(request);
            await Task.Yield();
            cancellationToken.ThrowIfCancellationRequested();
            if (_nextSubscribeIndex >= subscribeOutcomes.Count)
            {
                throw new InvalidOperationException("The scripted ACK workflow session has no subscribe outcome.");
            }

            var outcome = subscribeOutcomes[_nextSubscribeIndex];
            _nextSubscribeIndex++;
            if (outcome.FailureMessage is not null)
            {
                throw new InvalidOperationException(outcome.FailureMessage);
            }

            if (outcome.BatchValue is { } batch)
            {
                yield return batch;
            }
        }

        /// <inheritdoc/>
        public ValueTask AcknowledgeAsync(ReceiveAcknowledgement acknowledgement, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Acknowledgements.Add(acknowledgement);
            return ValueTask.CompletedTask;
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        /// <summary>Creates a successful public sync result for a pushed batch.</summary>
        /// <param name="batch">The pushed batch.</param>
        /// <returns>The remote sync result.</returns>
        private static RemoteSyncResult CreateAcceptedResult(SyncBatch batch)
        {
            List<OperationSyncResult> results = [];
            for (var index = 0; index < batch.Operations.Count; index++)
            {
                results.Add(new(batch.Operations[index].OperationId, OperationResultKind.Accepted, null, ServerVersion));
            }

            return new(batch.BatchId, results, ServerVersion, null);
        }
    }

    /// <summary>Scripted public subscribe outcome for the ACK workflow.</summary>
    /// <param name="BatchValue">The batch to emit.</param>
    /// <param name="FailureMessage">The failure message to throw before emitting.</param>
    private sealed record SubscribeOutcome(RemoteEventBatch? BatchValue, string? FailureMessage)
    {
        /// <summary>Creates a batch outcome.</summary>
        /// <param name="batch">The batch to emit.</param>
        /// <returns>The subscribe outcome.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static SubscribeOutcome Batch(RemoteEventBatch batch) =>
            new(batch, null);

        /// <summary>Creates a failure outcome.</summary>
        /// <param name="message">The failure message.</param>
        /// <returns>The subscribe outcome.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static SubscribeOutcome Failure(string message) =>
            new(null, message);
    }
}
