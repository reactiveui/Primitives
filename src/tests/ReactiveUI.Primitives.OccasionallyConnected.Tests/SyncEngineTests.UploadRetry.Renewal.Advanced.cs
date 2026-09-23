// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Advanced stale-session renewal tests for <see cref="SyncEngine"/>.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>The fault code emitted when retiring a shared session fails.</summary>
    private const string RetiredSessionDisposalFaultCode = "OC.Engine.RetiredSessionDisposal";

    /// <summary>Verifies a retired session disposal failure is reported after its last upload lease releases.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task UploadAttemptPublishesFaultWhenRetiredSessionDisposalFailsAfterRenewal()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var operation = CreateOperation();
        var store = CreateUploadStore([operation], timeProvider: clock);
        store.RequeueReleasedLeases = true;
        var releaseDispose = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var disposeEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var expired = CreateExpiredDisposedSession(releaseDispose, disposeEntered);
        var renewed = CreateBatchSession(ExpectedSingleOperation);
        var transport = CreateRenewalTransport(expired, renewed);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        await using var engine = CreateEngine(store, transport, CreateDiagnosticsBatchOptions(ExpectedSingleOperation, PreparedUploadBytes), timeProvider: clock);
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));
        using var faultSubscription = engine.Faults.Subscribe(faults);

        try
        {
            await engine.StartAsync(CancellationToken.None);
            engine.NotifyLocalCommitReady(Stream, operation);
            await DriveUploadDwellWithTraceAsync(clock, store, expired, faults, operationStates: null);
            await disposeEntered.Task.WaitAsync(GuardTimeout);
            await WaitForConditionAsync(() => store.Statuses[operation.OperationId].State == SyncOperationState.Synchronized);
            await Assert.That(renewed.SentBatches.Count).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(faults.Values.Exists(static fault => fault.Code == RetiredSessionDisposalFaultCode)).IsFalse();
            releaseDispose.SetResult();
        }
        finally
        {
            _ = releaseDispose.TrySetResult();
            await engine.StopAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        }

        await Assert.That(expired.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(renewed.SentBatches.Count).IsEqualTo(ExpectedSingleOperation);
        await WaitForConditionAsync(() => faults.Values.Exists(static fault => fault.Code == RetiredSessionDisposalFaultCode));
        await Assert.That(faults.Values.Exists(static fault => fault.Code == RetiredSessionDisposalFaultCode)).IsTrue();
    }

    /// <summary>Verifies renewal rejects a BatchPush session that lacks the prepared-upload interface.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task UploadAttemptRejectsExpiredSessionRenewalWithoutPreparedUploadInterface()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var operation = CreateOperation();
        var store = CreateUploadStore([operation], timeProvider: clock);
        store.RequeueReleasedLeases = true;
        var expired = CreateExpiredUploadSession();
        var unprepared = new RecordingSession { NegotiatedCapabilities = CreateBatchPushCapabilities(ExpectedSingleOperation, PreparedUploadBytes) };
        var transport = CreateRenewalTransport(expired, unprepared);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        await using var engine = CreateEngine(store, transport, CreateDiagnosticsBatchOptions(ExpectedSingleOperation, PreparedUploadBytes), timeProvider: clock);
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));
        using var faultSubscription = engine.Faults.Subscribe(faults);

        await engine.StartAsync(CancellationToken.None);
        engine.NotifyLocalCommitReady(Stream, operation);
        await DriveUploadDwellWithTraceAsync(clock, store, expired, faults, operationStates: null);
        await WaitForUploadConditionWithTraceAsync(
            () => faults.Values.Exists(static fault => fault.Code == UploadAttemptFaultCode),
            store,
            expired,
            faults,
            operationStates: null);

        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedCapacityCommitAttempts);
        await Assert.That(unprepared.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
        await engine.StopAsync(CancellationToken.None);
        await Assert.That(store.Statuses[operation.OperationId].State).IsEqualTo(SyncOperationState.QueuedForUpload);
        await Assert.That(store.Leases.Count).IsEqualTo(ExpectedSingleOperation);
    }

    /// <summary>Verifies an old stale receive joins an in-flight newer renewal after current progress restored the budget.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task OldGenerationExpiryJoinsNewerRenewalAfterDurableProgress()
    {
        var directory = Directory.CreateTempSubdirectory("oc-engine-renewal-join-current-");
        try
        {
            await AssertOldGenerationExpiryJoinsNewerRenewalAsync(Path.Combine(directory.FullName, "local.db"));
        }
        finally
        {
            directory.Delete(recursive: true);
        }
    }

    /// <summary>Creates an expired session whose retired disposal is test-controlled.</summary>
    /// <param name="releaseDispose">The gate releasing disposal.</param>
    /// <param name="disposeEntered">The signal set when disposal starts.</param>
    /// <returns>The expired session.</returns>
    private static PreparedSession CreateExpiredDisposedSession(TaskCompletionSource releaseDispose, TaskCompletionSource disposeEntered) => new(
        ExpectedSingleOperation,
        PreparedUploadBytes)
    {
        DisposeEntered = disposeEntered,
        DisposeException = new InvalidOperationException("retired session dispose failed"),
        NegotiatedCapabilities = CreateBatchPushCapabilities(ExpectedSingleOperation, PreparedUploadBytes),
        ReleaseDispose = releaseDispose,
        SendException = CreateTransportFailure(RetryFailureKind.RemoteSessionExpired),
    };

    /// <summary>Runs the old-generation join scenario against a stream-aware SQLite store.</summary>
    /// <param name="databasePath">The physical SQLite database path.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertOldGenerationExpiryJoinsNewerRenewalAsync(string databasePath)
    {
        var context = await CreateOldGenerationJoinContextAsync(databasePath);
        await using var store = context.Store;
        await using var engine = context.Engine;
        using var uploadRegistration = context.Engine.RegisterParticipant(CreateSqliteUploadParticipant(context.Store, Stream));
        using var receiveRegistration = context.Engine.RegisterParticipant(context.ReceiveParticipant);
        IDisposable? otherRegistration = null;

        try
        {
            await StartAndReachCurrentGenerationAsync(context);
            var otherOperation = await SeedOtherStreamOperationAsync(context.Store);
            otherRegistration = context.Engine.RegisterParticipant(CreateSqliteUploadParticipant(context.Store, otherOperation.StreamId));
            var trigger = TriggerSecondExpiryAndHoldRenewal(context, otherOperation);
            await JoinOldGenerationFailureToCurrentRenewalAsync(context, trigger);
            await AssertOldGenerationJoinResultsAsync(context, otherOperation);
        }
        finally
        {
            _ = context.ReleaseReceiveApply.TrySetResult();
            _ = context.Transport.ReleaseGatedConnect.TrySetResult();
            otherRegistration?.Dispose();
            await context.Engine.StopAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        }
    }

    /// <summary>Creates the old-generation join context.</summary>
    /// <param name="databasePath">The physical SQLite database path.</param>
    /// <returns>The context.</returns>
    private static async Task<OldGenerationJoinContext> CreateOldGenerationJoinContextAsync(string databasePath)
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var store = await CreateEngineSqliteStoreAsync(databasePath, clock);
        var initialOperation = CreateOperation(operationId: OperationId.New());
        await SeedRestartUploadOperationAsync(store, initialOperation);
        var receiveStream = new StreamId("sync/engine/renewal-join-receive");
        var releaseReceiveApply = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var receiveApplyEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var first = CreateOldGenerationReceiveSession(receiveStream);
        var second = CreateExpiringSecondGenerationUploadSession();
        var third = new ReceiveSession { NegotiatedCapabilities = CreateBatchPushCapabilities(ExpectedSingleOperation, PreparedUploadBytes) };
        var transport = CreateGatedThirdConnectTransport(first, second, third, new PreparedSession(ExpectedSingleOperation, PreparedUploadBytes));
        var options = CreateDiagnosticsBatchOptions(ExpectedSingleOperation, PreparedUploadBytes);
        var engine = CreateBorrowedStoreEngine(store, transport, options, clock);
        var participant = CreateReceiveParticipant(receiveStream, receiveApplyEntered, releaseReceiveApply);
        return new()
        {
            Engine = engine,
            Store = store,
            Clock = clock,
            Options = options,
            InitialOperation = initialOperation,
            ReceiveParticipant = participant,
            ReleaseReceiveApply = releaseReceiveApply,
            ReceiveApplyEntered = receiveApplyEntered,
            First = first,
            Second = second,
            Third = third,
            Transport = transport,
        };
    }

    /// <summary>Starts the engine and performs a successful upload on generation two.</summary>
    /// <param name="context">The old-generation context.</param>
    /// <returns>The assertion task.</returns>
    private static async Task StartAndReachCurrentGenerationAsync(OldGenerationJoinContext context)
    {
        await context.Engine.StartAsync(CancellationToken.None);
        await context.First.SubscribeEntered.Task.WaitAsync(GuardTimeout);
        await context.ReceiveApplyEntered.Task.WaitAsync(GuardTimeout);
        context.Engine.NotifyLocalCommitReady(Stream, context.InitialOperation);
        await TriggerAndDrainUploadWithTraceAsync(context.Engine, context.Clock, context.Second, faults: null, operationStates: null);
        await WaitForSqliteOperationStateAsync(context.Store, context.InitialOperation.OperationId, SyncOperationState.Synchronized);
        await Assert.That(context.Transport.ConnectCalls).IsEqualTo(ExpectedCapacityCommitAttempts);
    }

    /// <summary>Triggers a second upload that blocks before its renewal connect returns.</summary>
    /// <param name="context">The old-generation context.</param>
    /// <param name="operation">The second operation requiring renewal.</param>
    /// <returns>The trigger task.</returns>
    private static Task TriggerSecondExpiryAndHoldRenewal(
        OldGenerationJoinContext context,
        SyncOperation operation)
    {
        context.Engine.NotifyLocalCommitReady(operation.StreamId, operation);
        return context.Engine.TriggerSyncAsync(CancellationToken.None).AsTask();
    }

    /// <summary>Releases old receive apply so its stale ACK joins the in-flight newer renewal.</summary>
    /// <param name="context">The old-generation context.</param>
    /// <param name="trigger">The upload trigger to observe.</param>
    /// <returns>The assertion task.</returns>
    private static async Task JoinOldGenerationFailureToCurrentRenewalAsync(OldGenerationJoinContext context, Task trigger)
    {
        await WaitForConditionAsync(
            () => context.Second.SentBatches.Count == ExpectedCapacityCommitAttempts
                || context.Clock.HasTimerDueIn(context.Options.Batching.MaximumDwellTime));
        if (context.Second.SentBatches.Count != ExpectedCapacityCommitAttempts)
        {
            context.Clock.Advance(context.Options.Batching.MaximumDwellTime);
        }

        await context.Transport.GatedConnectEntered.Task.WaitAsync(GuardTimeout);
        context.ReleaseReceiveApply.SetResult();
        await WaitForConditionAsync(() => context.First.AcknowledgeCalls == ExpectedSingleOperation);
        context.Transport.ReleaseGatedConnect.SetResult();
        await trigger.WaitAsync(GuardTimeout);
    }

    /// <summary>Asserts the stale old generation did not start a separate renewal.</summary>
    /// <param name="context">The old-generation context.</param>
    /// <param name="operation">The second operation requiring renewal.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertOldGenerationJoinResultsAsync(
        OldGenerationJoinContext context,
        SyncOperation operation)
    {
        await WaitForSqliteOperationStateAsync(context.Store, operation.OperationId, SyncOperationState.Synchronized);
        await Assert.That(context.Transport.ConnectCalls).IsEqualTo(ExpectedCapacityCommitAttempts + ExpectedSingleOperation);
        await Assert.That(context.First.AcknowledgeCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(context.Second.SentBatches.Count).IsEqualTo(ExpectedCapacityCommitAttempts);
        await Assert.That(context.Third.SentBatches.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(context.Third.SubscribeRequests.Count).IsGreaterThanOrEqualTo(ExpectedSingleOperation);
    }

    /// <summary>Creates the first old-generation receive session.</summary>
    /// <param name="receiveStream">The receive stream.</param>
    /// <returns>The receive session.</returns>
    private static ReceiveSession CreateOldGenerationReceiveSession(StreamId receiveStream) => new()
    {
        AcknowledgeException = CreateTransportFailure(RetryFailureKind.RemoteSessionExpired),
        Batches = { CreateReceiveBatch(receiveStream, previousCursor: null, nextCursor: "old-progress") },
        NegotiatedCapabilities = CreateBatchPushCapabilities(ExpectedSingleOperation, PreparedUploadBytes),
        PreparedSendException = CreateTransportFailure(RetryFailureKind.RemoteSessionExpired),
    };

    /// <summary>Creates the second-generation upload session that expires on its second send.</summary>
    /// <returns>The prepared session.</returns>
    private static PreparedSession CreateExpiringSecondGenerationUploadSession()
    {
        var session = CreateBatchSession(ExpectedSingleOperation);
        session.SendFailures.Enqueue(null);
        session.SendFailures.Enqueue(CreateTransportFailure(RetryFailureKind.RemoteSessionExpired));
        return session;
    }

    /// <summary>Creates a transport that gates the third shared connection.</summary>
    /// <param name="sessions">The queued sessions.</param>
    /// <returns>The gated transport.</returns>
    private static GatedNthConnectTransport CreateGatedThirdConnectTransport(params IRemoteTransportSession[] sessions)
    {
        var inner = CreateQueuedReceiveTransport(sessions);
        return new(inner, ExpectedCapacityCommitAttempts + ExpectedSingleOperation);
    }

    /// <summary>Delegates to a recording transport and gates a specific connect call.</summary>
    /// <param name="inner">The wrapped transport.</param>
    /// <param name="gatedCall">The one-based connect call to gate.</param>
    private sealed class GatedNthConnectTransport(RecordingTransport inner, int gatedCall) : IRemoteTransportAdapter
    {
        /// <summary>Gets the signal raised when the gated connect is reached.</summary>
        public TaskCompletionSource GatedConnectEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets the signal that releases the gated connect.</summary>
        public TaskCompletionSource ReleaseGatedConnect { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets the number of delegated connect calls.</summary>
        public int ConnectCalls => inner.ConnectCalls;

        /// <inheritdoc/>
        public RemoteTransportCapabilities Capabilities => inner.Capabilities;

        /// <inheritdoc/>
        public async ValueTask<IRemoteTransportSession> ConnectAsync(TransportConnectRequest request, CancellationToken cancellationToken)
        {
            var session = await inner.ConnectAsync(request, cancellationToken).ConfigureAwait(false);
            if (inner.ConnectCalls == gatedCall)
            {
                _ = GatedConnectEntered.TrySetResult();
                await ReleaseGatedConnect.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            }

            return session;
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => inner.DisposeAsync();
    }

    /// <summary>Stores the fixtures for old-generation renewal join tests.</summary>
    private sealed record OldGenerationJoinContext
    {
        /// <summary>Gets the Engine fixture.</summary>
        public required SyncEngine Engine { get; init; }

        /// <summary>Gets the Store fixture.</summary>
        public required SqliteLocalStoreAdapter Store { get; init; }

        /// <summary>Gets the Clock fixture.</summary>
        public required ManualTimerTimeProvider Clock { get; init; }

        /// <summary>Gets the Options fixture.</summary>
        public required OccasionallyConnectedOptions Options { get; init; }

        /// <summary>Gets the InitialOperation fixture.</summary>
        public required SyncOperation InitialOperation { get; init; }

        /// <summary>Gets the ReceiveParticipant fixture.</summary>
        public required RecordingParticipant ReceiveParticipant { get; init; }

        /// <summary>Gets the ReleaseReceiveApply fixture.</summary>
        public required TaskCompletionSource ReleaseReceiveApply { get; init; }

        /// <summary>Gets the ReceiveApplyEntered fixture.</summary>
        public required TaskCompletionSource ReceiveApplyEntered { get; init; }

        /// <summary>Gets the First fixture.</summary>
        public required ReceiveSession First { get; init; }

        /// <summary>Gets the Second fixture.</summary>
        public required PreparedSession Second { get; init; }

        /// <summary>Gets the Third fixture.</summary>
        public required ReceiveSession Third { get; init; }

        /// <summary>Gets the Transport fixture.</summary>
        public required GatedNthConnectTransport Transport { get; init; }
    }
}
