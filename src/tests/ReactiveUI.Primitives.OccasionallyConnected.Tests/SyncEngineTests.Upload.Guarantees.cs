// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <content>Durable delivery-guarantee retry tests for <see cref="SyncEngine"/>.</content>
public sealed partial class SyncEngineTests
{
    /// <summary>Verifies a downgraded exactly-once operation keeps retrying within the full at-least-once age.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DowngradedExactlyOnceOperationRetriesTransientFailureAfterServerWindow()
    {
        var directory = SqliteTestDirectory.Create("oc-engine-upload-downgraded-retry-");
        try
        {
            var databasePath = Path.Combine(directory.FullName, "local.db");
            var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
            var serverRetention = TimeSpan.FromSeconds(UploadShortRetentionSeconds);
            var operation = CreateOperation() with
            {
                Policy = OperationPolicy.Default with { DeliveryGuarantee = DeliveryGuarantee.ExactlyOnce },
            };
            await RunFirstExactlyOnceSqliteUploadAttemptAsync(databasePath, clock, operation, serverRetention);
            clock.Advance(TimeSpan.FromSeconds(UploadExpiredRetentionAdvanceSeconds));
            var retryDueUtc = await FailDowngradedSqliteUploadAsync(databasePath, clock, operation.OperationId, serverRetention);
            clock.Advance(retryDueUtc - clock.GetUtcNow());
            await AssertDowngradedSqliteUploadResendsAsync(databasePath, clock, operation.OperationId, serverRetention);
        }
        finally
        {
            directory.Delete(recursive: true);
        }
    }

    /// <summary>Verifies a downgraded operation stops before another send after its full retry age expires.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DowngradedExactlyOnceOperationStopsAfterFullRetryAge()
    {
        var directory = SqliteTestDirectory.Create("oc-engine-upload-downgraded-expired-");
        try
        {
            var databasePath = Path.Combine(directory.FullName, "local.db");
            var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
            var serverRetention = TimeSpan.FromSeconds(UploadShortRetentionSeconds);
            var operation = CreateOperation() with
            {
                Policy = OperationPolicy.Default with { DeliveryGuarantee = DeliveryGuarantee.ExactlyOnce },
            };
            await RunFirstExactlyOnceSqliteUploadAttemptAsync(databasePath, clock, operation, serverRetention);
            clock.Advance(TimeSpan.FromSeconds(UploadExpiredRetentionAdvanceSeconds));
            var retryDueUtc = await FailDowngradedSqliteUploadAsync(databasePath, clock, operation.OperationId, serverRetention);
            clock.Advance(retryDueUtc - clock.GetUtcNow() + TimeSpan.FromMinutes(ExpectedSingleOperation));
            await AssertExpiredDowngradedSqliteUploadDoesNotResendAsync(databasePath, clock, operation.OperationId, serverRetention);
        }
        finally
        {
            directory.Delete(recursive: true);
        }
    }

    /// <summary>Fails a downgraded resend and returns its persisted retry due time.</summary>
    /// <param name="databasePath">The durable store path.</param>
    /// <param name="clock">The manual clock.</param>
    /// <param name="operationId">The pending operation.</param>
    /// <param name="serverRetention">The expired server window.</param>
    /// <returns>The retry due time.</returns>
    private static async Task<DateTimeOffset> FailDowngradedSqliteUploadAsync(
        string databasePath,
        ManualTimerTimeProvider clock,
        OperationId operationId,
        TimeSpan serverRetention)
    {
        await using var store = await CreateEngineSqliteStoreAsync(databasePath, clock);
        var session = new PreparedSession(ExpectedSingleOperation, PreparedUploadBytes)
        {
            NegotiatedCapabilities = CreateExactlyOnceCapabilities(serverRetention),
            SendException = CreateTransportFailure(RetryFailureKind.Transient, TimeSpan.FromMilliseconds(UploadShortRetryMilliseconds)),
        };
        var options = CreateDowngradedRetryOptions();
        await using var engine = CreateBorrowedStoreEngine(store, new RecordingTransport { SessionOverride = session }, options, clock);
        using var registration = engine.RegisterParticipant(new RecordingParticipant());
        await engine.StartAsync(CancellationToken.None);
        var sync = engine.TriggerSyncAsync(CancellationToken.None).AsTask();
        await WaitForConditionAsync(() => HasUploadDwellProgress(clock, session, faults: null));
        AdvanceUploadDwellIfStillPending(clock, session, faults: null);
        await WaitForConditionAsync(() => session.SentBatches.Count == ExpectedSingleOperation);
        var retryState = await WaitForRetryStateWithTraceAsync(
            store,
            operationId,
            session,
            faults: null,
            operationStates: null,
            state => state.DueUtc > clock.GetUtcNow());
        var status = await store.GetOperationStatusAsync(operationId, CancellationToken.None);
        await Assert.That(status?.ReasonCode).IsEqualTo(SyncReasonCodes.GuaranteeDowngraded);
        await Assert.That(session.SentBatches.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(retryState.DueUtc.HasValue).IsTrue();
        await engine.StopAsync(CancellationToken.None);
        await AwaitSyncWithUploadTraceAsync(sync, session, faults: null);
        return retryState.DueUtc!.Value;
    }

    /// <summary>Asserts a durable downgraded operation resumes after retry backoff.</summary>
    /// <param name="databasePath">The durable store path.</param>
    /// <param name="clock">The manual clock.</param>
    /// <param name="operationId">The pending operation.</param>
    /// <param name="serverRetention">The expired server window.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertDowngradedSqliteUploadResendsAsync(
        string databasePath,
        ManualTimerTimeProvider clock,
        OperationId operationId,
        TimeSpan serverRetention)
    {
        await using var store = await CreateEngineSqliteStoreAsync(databasePath, clock);
        var session = new PreparedSession(ExpectedSingleOperation, PreparedUploadBytes)
            { NegotiatedCapabilities = CreateExactlyOnceCapabilities(serverRetention) };
        await using var engine = CreateBorrowedStoreEngine(
            store,
            new RecordingTransport { SessionOverride = session },
            CreateDowngradedRetryOptions(),
            clock);
        using var registration = engine.RegisterParticipant(new RecordingParticipant());
        await engine.StartAsync(CancellationToken.None);
        await TriggerAndDrainUploadWithTraceAsync(engine, clock, session, faults: null, operationStates: null);
        await Assert.That(session.SentBatches.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(session.SentBatches[0].Operations[0].OperationId).IsEqualTo(operationId);
        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Checks that the durable fallback marker remains while an expired retry is rejected before prepare.</summary>
    /// <param name="databasePath">The durable store path.</param>
    /// <param name="clock">The manual clock.</param>
    /// <param name="operationId">The pending operation.</param>
    /// <param name="serverRetention">The expired server window.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertExpiredDowngradedSqliteUploadDoesNotResendAsync(
        string databasePath,
        ManualTimerTimeProvider clock,
        OperationId operationId,
        TimeSpan serverRetention)
    {
        await using var store = await CreateEngineSqliteStoreAsync(databasePath, clock);
        var session = new PreparedSession(ExpectedSingleOperation, PreparedUploadBytes)
            { NegotiatedCapabilities = CreateExactlyOnceCapabilities(serverRetention) };
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        await using var engine = CreateBorrowedStoreEngine(
            store,
            new RecordingTransport { SessionOverride = session },
            CreateDowngradedRetryOptions(),
            clock);
        using var registration = engine.RegisterParticipant(new RecordingParticipant());
        using var faultSubscription = engine.Faults.Subscribe(faults);
        await engine.StartAsync(CancellationToken.None);
        var sync = engine.TriggerSyncAsync(CancellationToken.None).AsTask();
        await WaitForConditionAsync(() => HasUploadDwellProgress(clock, session, faults));
        AdvanceUploadDwellIfStillPending(clock, session, faults);
        await WaitForConditionAsync(() => faults.Values.Exists(static fault => fault.Code == UploadAttemptFaultCode));
        var status = await store.GetOperationStatusAsync(operationId, CancellationToken.None);
        await Assert.That(status?.ReasonCode).IsEqualTo(SyncReasonCodes.GuaranteeDowngraded);
        await Assert.That(session.PrepareCalls).IsEqualTo(0);
        await Assert.That(session.SentBatches.Count).IsEqualTo(0);
        await engine.StopAsync(CancellationToken.None);
        await AwaitSyncWithUploadTraceAsync(sync, session, faults);
    }

    /// <summary>Uses a retry age longer than the server's short exactly-once window.</summary>
    /// <returns>The fallback options.</returns>
    private static OccasionallyConnectedOptions CreateDowngradedRetryOptions() =>
        CreateDiagnosticsBatchOptions(ExpectedSingleOperation, PreparedUploadBytes) with
        {
            ExactlyOnceExpiryBehavior = ExactlyOnceExpiryBehavior.FallbackToAtLeastOnce,
            Retry = OccasionallyConnectedOptions.Default.Retry with
            {
                MinimumDelay = TimeSpan.FromMilliseconds(UploadShortRetryMilliseconds),
                MaximumDelay = TimeSpan.FromMilliseconds(UploadShortRetryMilliseconds),
                MaximumRetryAge = TimeSpan.FromMinutes(ExpectedSingleOperation),
            },
        };
}
