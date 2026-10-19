// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using Microsoft.Extensions.Time.Testing;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Fail-closed tests for PreparedUploadAttemptCoordinator.</summary>
public sealed partial class PreparedUploadAttemptCoordinatorTests
{
    /// <summary>The renewal failure message used by precedence tests.</summary>
    private const string RenewFailedMessage = "renew failed";

    /// <summary>The probe delay used to detect abandoned renewal disposal.</summary>
    private const int RenewalDrainProbeMilliseconds = 200;

    /// <summary>Verifies malformed leased batches fail before preparation.</summary>
    /// <param name="scenario">The malformed lease scenario.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments("empty-lease-id")]
    [Arguments("empty-operations")]
    [Arguments("too-many-operations")]
    [Arguments("null-first-operation")]
    [Arguments("null-later-operation")]
    [Arguments("empty-operation-id")]
    [Arguments("wrong-stream")]
    [Arguments("non-increasing-sequence")]
    public async Task ExecuteAsyncRejectsMalformedLeaseBeforePrepare(string scenario)
    {
        var lease = CreateMalformedLease(scenario);
        var preparer = new RecordingPreparer();
        var store = new RecordingStore { LeaseExpiresAtUtc = lease.ExpiresAtUtc };

        _ = await Assert.ThrowsAsync<Exception>(
            () => PreparedUploadAttemptCoordinator.ExecuteAsync(CreateRequest(lease, preparer, store), CancellationToken.None).AsTask());

        await Assert.That(preparer.PrepareCount).IsEqualTo(0);
        await Assert.That(store.Barriers.Count).IsEqualTo(0);
    }

    /// <summary>Verifies option bounds are finite and positive before preparation.</summary>
    /// <param name="scenario">The invalid option scenario.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments("operation-limit")]
    [Arguments("size-limit")]
    [Arguments("interval-zero")]
    [Arguments("interval-infinite")]
    [Arguments("duration-zero")]
    [Arguments("duration-infinite")]
    [Arguments("duration-max")]
    [Arguments("interval-equals-duration")]
    public async Task ExecuteAsyncRejectsInvalidOptionsBeforePrepare(string scenario)
    {
        var lease = CreateLease(CreateOperation(FirstSequence));
        var store = CreateStore(lease);
        var preparer = new RecordingPreparer();

        _ = await Assert.ThrowsExactlyAsync<ArgumentOutOfRangeException>(
            () => PreparedUploadAttemptCoordinator.ExecuteAsync(
                CreateRequest(lease, preparer, store) with { Options = CreateInvalidOptions(scenario) },
                CancellationToken.None).AsTask());

        await Assert.That(preparer.PrepareCount).IsEqualTo(0);
        await Assert.That(store.Barriers.Count).IsEqualTo(0);
    }

    /// <summary>Verifies nullable operation metadata remains valid and bounded.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ExecuteAsyncAcceptsNullBaseVersionAndEmptyMetadata()
    {
        var operation = CreateOperation(FirstSequence) with
        {
            BaseVersion = null,
            Metadata = new Dictionary<string, string>(),
        };
        var lease = CreateLease(operation);
        var store = CreateStore(lease);

        var result = await PreparedUploadAttemptCoordinator.ExecuteAsync(CreateRequest(lease, new RecordingPreparer(), store), CancellationToken.None);

        await Assert.That(result.Sent).IsTrue();
        await Assert.That(result.Reconciled).IsTrue();
    }

    /// <summary>Verifies zero encoded prepared handles fail before durable barriers.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ExecuteAsyncRejectsZeroEncodedPreparedBodyBeforeBarrier()
    {
        var lease = CreateLease(CreateOperation(FirstSequence));
        var store = CreateStore(lease);
        var preparer = new RecordingPreparer { EncodedSizeBytes = 0 };

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => PreparedUploadAttemptCoordinator.ExecuteAsync(CreateRequest(lease, preparer, store), CancellationToken.None).AsTask());

        await Assert.That(store.Barriers.Count).IsEqualTo(0);
        await Assert.That(preparer.Prepared?.SendCount ?? 0).IsEqualTo(0);
    }

    /// <summary>Verifies every allowed non-terminal durable state may enter a send barrier.</summary>
    /// <param name="state">The durable state.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(SyncOperationState.SavedLocally)]
    [Arguments(SyncOperationState.QueuedForUpload)]
    [Arguments(SyncOperationState.Uploading)]
    [Arguments(SyncOperationState.Conflict)]
    [Arguments(SyncOperationState.Ambiguous)]
    public async Task ExecuteAsyncAllowsEveryNonTerminalSendState(SyncOperationState state)
    {
        var operation = CreateOperation(FirstSequence);
        var lease = CreateLease(operation);
        var store = CreateStore(lease);
        store.Statuses[operation.OperationId] = CreateStatus(operation, 0, state);

        var result = await PreparedUploadAttemptCoordinator.ExecuteAsync(CreateRequest(lease, new RecordingPreparer(), store), CancellationToken.None);

        await Assert.That(result.Sent).IsTrue();
        await Assert.That(result.Barriers.Count).IsEqualTo(FirstSequence);
    }

    /// <summary>Verifies malformed durable status fails closed before sending.</summary>
    /// <param name="scenario">The status scenario.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments("missing")]
    [Arguments("operation")]
    [Arguments("stream")]
    [Arguments("terminal")]
    [Arguments("negative-attempt")]
    [Arguments("max-attempt")]
    public async Task ExecuteAsyncRejectsMalformedDurableStatusBeforeSend(string scenario)
    {
        var operation = CreateOperation(FirstSequence);
        var lease = CreateLease(operation);
        var store = CreateStore(lease);
        ApplyMalformedStatus(store, operation, scenario);
        var preparer = new RecordingPreparer();

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => PreparedUploadAttemptCoordinator.ExecuteAsync(CreateRequest(lease, preparer, store), CancellationToken.None).AsTask());

        await Assert.That(store.Barriers.Count).IsEqualTo(0);
        await Assert.That(preparer.Prepared?.SendCount ?? 0).IsEqualTo(0);
    }

    /// <summary>Verifies malformed barrier receipts fail before sending.</summary>
    /// <param name="scenario">The barrier scenario.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments("operation")]
    [Arguments("attempt")]
    public async Task ExecuteAsyncRejectsMalformedBarrierBeforeSend(string scenario)
    {
        var lease = CreateLease(CreateOperation(FirstSequence));
        var store = CreateStore(lease);
        store.BarrierOverride = (operationId, attempt) => scenario == "operation"
            ? new(OperationId.New(), attempt, true, null)
            : new(operationId, attempt + FirstSequence, true, null);
        var preparer = new RecordingPreparer();

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => PreparedUploadAttemptCoordinator.ExecuteAsync(CreateRequest(lease, preparer, store), CancellationToken.None).AsTask());

        await Assert.That(preparer.Prepared?.SendCount ?? 0).IsEqualTo(0);
    }

    /// <summary>Verifies cleanup-only failure is surfaced when no primary failure occurred.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ExecuteAsyncCleanupFailureAfterDeniedBarrierIsReported()
    {
        var lease = CreateLease(CreateOperation(FirstSequence));
        var store = CreateStore(lease);
        store.DeniedOperation = lease.Operations[0].OperationId;
        store.ReleaseException = new InvalidOperationException(ReleaseFailedMessage);

        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => PreparedUploadAttemptCoordinator.ExecuteAsync(CreateRequest(lease, new RecordingPreparer(), store), CancellationToken.None).AsTask());

        await Assert.That(exception?.Message).Contains(ReleaseFailedMessage);
        await Assert.That(store.ReleaseCount).IsEqualTo(FirstSequence);
    }

    /// <summary>Verifies successful reconciliation transfers lease ownership and skips release.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ExecuteAsyncTransfersLeaseOwnershipAfterReconciliation()
    {
        var lease = CreateLease(CreateOperation(FirstSequence));
        var store = CreateStore(lease);
        store.ReleaseException = new InvalidOperationException("lease is missing but unrelated");

        var result = await PreparedUploadAttemptCoordinator.ExecuteAsync(CreateRequest(lease, new RecordingPreparer(), store), CancellationToken.None);

        await Assert.That(result.Reconciled).IsTrue();
        await Assert.That(store.ReleaseCount).IsEqualTo(0);
    }

    /// <summary>Verifies release failures are not swallowed by message text.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ExecuteAsyncDoesNotSwallowReleaseFailureByMessageText()
    {
        var lease = CreateLease(CreateOperation(FirstSequence));
        var store = CreateStore(lease);
        store.DeniedOperation = lease.Operations[0].OperationId;
        store.ReleaseException = new InvalidOperationException("lease is missing but unrelated");

        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => PreparedUploadAttemptCoordinator.ExecuteAsync(CreateRequest(lease, new RecordingPreparer(), store), CancellationToken.None).AsTask());

        await Assert.That(exception?.Message).Contains("lease is missing");
        await Assert.That(store.ReleaseCount).IsEqualTo(FirstSequence);
    }

    /// <summary>Verifies renewal failure cancels preparation and reports the renewal failure.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ExecuteAsyncRenewalFailureCancelsPreparationAndReportsRenewalFailure()
    {
        var clock = new FakeTimeProvider(TimestampUtc);
        var lease = CreateLease(clock.GetUtcNow().AddSeconds(SecondSequence), CreateOperation(FirstSequence));
        var store = CreateStore(lease);
        store.RenewException = new InvalidOperationException(RenewFailedMessage);
        TaskCompletionSource prepareStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var preparer = new RecordingPreparer { BeforePrepareCompletes = token => SignalAndDelayPrepareAsync(prepareStarted, token) };
        var attempt = PreparedUploadAttemptCoordinator.ExecuteAsync(
            CreateRequest(lease, preparer, store) with
            {
                Options = CreateOptions() with
                {
                    TimeProvider = clock,
                    LeaseRenewalInterval = TimeSpan.FromSeconds(RenewalIntervalSeconds),
                    LeaseRenewalDuration = TimeSpan.FromSeconds(RenewalDurationSeconds),
                },
            },
            CancellationToken.None).AsTask();

        await prepareStarted.Task.WaitAsync(GateTimeout, CancellationToken.None).ConfigureAwait(false);
        AdvanceClock(clock, TimeSpan.FromSeconds(RenewalIntervalSeconds));
        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => attempt);

        await Assert.That(exception?.Message).Contains(RenewFailedMessage);
        await Assert.That(store.RenewCount).IsEqualTo(0);
        await Assert.That(store.Barriers.Count).IsEqualTo(0);
    }

    /// <summary>Verifies caller cancellation keeps precedence when it coincides with renewal failure.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ExecuteAsyncCallerCancellationPreservesCancellationWhenRenewalFailed()
    {
        using var callerCancellation = new CancellationTokenSource();
        var clock = new FakeTimeProvider(TimestampUtc);
        var lease = CreateLease(clock.GetUtcNow().AddSeconds(SecondSequence), CreateOperation(FirstSequence));
        var store = CreateStore(lease);
        store.RenewException = new InvalidOperationException(RenewFailedMessage);
        TaskCompletionSource prepareStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var preparer = new RecordingPreparer { BeforePrepareCompletes = token => CancelCallerWhenAttemptCancelsAsync(prepareStarted, callerCancellation, token) };
        var attempt = PreparedUploadAttemptCoordinator.ExecuteAsync(
            CreateRequest(lease, preparer, store) with
            {
                Options = CreateOptions() with
                {
                    TimeProvider = clock,
                    LeaseRenewalInterval = TimeSpan.FromSeconds(RenewalIntervalSeconds),
                    LeaseRenewalDuration = TimeSpan.FromSeconds(RenewalDurationSeconds),
                },
            },
            callerCancellation.Token).AsTask();

        await prepareStarted.Task.WaitAsync(GateTimeout, CancellationToken.None).ConfigureAwait(false);
        AdvanceClock(clock, TimeSpan.FromSeconds(RenewalIntervalSeconds));
        _ = await Assert.ThrowsExactlyAsync<OperationCanceledException>(() => attempt);

        await Assert.That(callerCancellation.IsCancellationRequested).IsTrue();
        await Assert.That(store.Barriers.Count).IsEqualTo(0);
    }

    /// <summary>Verifies renewal failure does not replace a material non-cancellation send failure.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ExecuteAsyncRenewalFailureDoesNotReplaceSendFailure()
    {
        var clock = new FakeTimeProvider(TimestampUtc);
        var lease = CreateLease(clock.GetUtcNow().AddSeconds(SecondSequence), CreateOperation(FirstSequence));
        var store = CreateStore(lease);
        store.RenewException = new InvalidOperationException(RenewFailedMessage);
        TaskCompletionSource sendStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var preparer = new RecordingPreparer { BeforeSendCompletes = token => ThrowSendAfterAttemptCancelsAsync(sendStarted, token) };
        var attempt = PreparedUploadAttemptCoordinator.ExecuteAsync(
            CreateRequest(lease, preparer, store) with
            {
                Options = CreateOptions() with
                {
                    TimeProvider = clock,
                    LeaseRenewalInterval = TimeSpan.FromSeconds(RenewalIntervalSeconds),
                    LeaseRenewalDuration = TimeSpan.FromSeconds(RenewalDurationSeconds),
                },
            },
            CancellationToken.None).AsTask();

        await sendStarted.Task.WaitAsync(GateTimeout, CancellationToken.None).ConfigureAwait(false);
        AdvanceClock(clock, TimeSpan.FromSeconds(RenewalIntervalSeconds));
        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => attempt);

        await Assert.That(exception?.Message).Contains(SendFailedMessage);
        await Assert.That(store.ReleaseCount).IsEqualTo(FirstSequence);
    }

    /// <summary>Verifies leases that expire before a scheduled renewal fail closed.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ExecuteAsyncExpiredLeaseBeforeScheduledRenewalCancelsPreparation()
    {
        var clock = new FakeTimeProvider(TimestampUtc);
        var lease = CreateLease(clock.GetUtcNow().AddSeconds(SecondSequence), CreateOperation(FirstSequence));
        var store = CreateStore(lease);
        TaskCompletionSource prepareStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var preparer = new RecordingPreparer { BeforePrepareCompletes = token => SignalAndDelayPrepareAsync(prepareStarted, token) };
        var attempt = PreparedUploadAttemptCoordinator.ExecuteAsync(
            CreateRequest(lease, preparer, store) with
            {
                Options = CreateOptions() with
                {
                    TimeProvider = clock,
                    LeaseRenewalInterval = TimeSpan.FromSeconds(RenewalIntervalSeconds),
                    LeaseRenewalDuration = TimeSpan.FromSeconds(RenewalDurationSeconds),
                },
            },
            CancellationToken.None).AsTask();

        await prepareStarted.Task.WaitAsync(GateTimeout, CancellationToken.None).ConfigureAwait(false);
        AdvanceClock(clock, TimeSpan.FromSeconds(RenewalDurationSeconds));
        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => attempt);

        await Assert.That(exception?.Message).Contains("expired before renewal");
        await Assert.That(store.Barriers.Count).IsEqualTo(0);
    }

    /// <summary>Verifies cleanup cancellation of an in-flight renewal is not reported as an attempt failure.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ExecuteAsyncDisposeCancelsInFlightRenewalWithoutFailingAttempt()
    {
        var clock = new FakeTimeProvider(TimestampUtc);
        var lease = CreateLease(clock.GetUtcNow().AddSeconds(SecondSequence), CreateOperation(FirstSequence));
        var store = CreateStore(lease);
        store.BeforeRenewCompletes = static token => new(Task.Delay(GateTimeout, token));
        TaskCompletionSource entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var request = CreateRequest(
            lease,
            new RecordingPreparer(),
            store,
            async (_, _) =>
            {
                _ = entered.TrySetResult();
                await release.Task.WaitAsync(GateTimeout, CancellationToken.None).ConfigureAwait(false);
            }) with
        {
            Options = CreateOptions() with
            {
                TimeProvider = clock,
                LeaseRenewalInterval = TimeSpan.FromSeconds(RenewalIntervalSeconds),
                LeaseRenewalDuration = TimeSpan.FromSeconds(RenewalDurationSeconds),
            },
        };
        var attempt = PreparedUploadAttemptCoordinator.ExecuteAsync(request, CancellationToken.None).AsTask();

        await entered.Task.WaitAsync(GateTimeout, CancellationToken.None).ConfigureAwait(false);
        AdvanceClock(clock, TimeSpan.FromSeconds(RenewalIntervalSeconds));
        await store.RenewStarted.Task.WaitAsync(GateTimeout, CancellationToken.None).ConfigureAwait(false);
        _ = release.TrySetResult();
        var result = await attempt.ConfigureAwait(false);

        await Assert.That(result.Sent).IsTrue();
        await Assert.That(result.Reconciled).IsTrue();
        await Assert.That(store.ReleaseCount).IsEqualTo(0);
    }

    /// <summary>Verifies an expired target after renewal schedules an immediate next tick.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ExecuteAsyncRenewalSchedulesImmediateTickWhenTargetAlreadyExpired()
    {
        var clock = new FakeTimeProvider(TimestampUtc);
        var lease = CreateLease(clock.GetUtcNow().AddSeconds(SecondSequence), CreateOperation(FirstSequence));
        var store = CreateStore(lease);
        TaskCompletionSource finishPrepare = new(TaskCreationOptions.RunContinuationsAsynchronously);
        store.AfterRenewAddsExtension = () => AdvanceClock(clock, TimeSpan.FromSeconds(RenewalDurationSeconds + FirstSequence));
        TaskCompletionSource prepareStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var preparer = new RecordingPreparer { BeforePrepareCompletes = token => SignalAndWaitPrepareAsync(prepareStarted, finishPrepare, token) };
        var attempt = PreparedUploadAttemptCoordinator.ExecuteAsync(
            CreateRequest(lease, preparer, store) with
            {
                Options = CreateOptions() with
                {
                    TimeProvider = clock,
                    LeaseRenewalInterval = TimeSpan.FromSeconds(RenewalIntervalSeconds),
                    LeaseRenewalDuration = TimeSpan.FromSeconds(RenewalDurationSeconds),
                },
            },
            CancellationToken.None).AsTask();

        await prepareStarted.Task.WaitAsync(GateTimeout, CancellationToken.None).ConfigureAwait(false);
        AdvanceClock(clock, TimeSpan.FromSeconds(RenewalIntervalSeconds));
        await store.Renewed.Task.WaitAsync(GateTimeout, CancellationToken.None).ConfigureAwait(false);
        _ = finishPrepare.TrySetResult();
        var result = await attempt.ConfigureAwait(false);

        await Assert.That(result.Sent).IsTrue();
        await Assert.That(store.RenewCount).IsEqualTo(FirstSequence);
    }

    /// <summary>Verifies renewal target overflow fails before preparation.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ExecuteAsyncRenewalTargetOverflowFailsBeforePrepare()
    {
        var now = DateTimeOffset.MaxValue.AddSeconds(-SecondSequence);
        var clock = new FakeTimeProvider(now);
        var lease = CreateLease(now.AddMilliseconds(ShortInitialLeaseMilliseconds), CreateOperation(FirstSequence));
        var store = CreateStore(lease);
        var preparer = new RecordingPreparer();

        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => PreparedUploadAttemptCoordinator.ExecuteAsync(
                CreateRequest(lease, preparer, store) with
                {
                    Options = CreateOptions() with
                    {
                        TimeProvider = clock,
                        LeaseRenewalInterval = TimeSpan.FromSeconds(RenewalIntervalSeconds),
                        LeaseRenewalDuration = TimeSpan.FromSeconds(RenewalDurationSeconds),
                    },
                },
                CancellationToken.None).AsTask());

        await Assert.That(exception?.Message).Contains("LeaseRenewalDuration");
        await Assert.That(preparer.PrepareCount).IsEqualTo(0);
    }

    /// <summary>Verifies invalid timer callback state fails closed through the attempt.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ExecuteAsyncInvalidTimerStateFailsBeforePrepare()
    {
        var lease = CreateLease(CreateOperation(FirstSequence));
        var store = CreateStore(lease);
        var preparer = new RecordingPreparer();

        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => PreparedUploadAttemptCoordinator.ExecuteAsync(
                CreateRequest(lease, preparer, store) with
                {
                    Options = CreateOptions() with { TimeProvider = new InvalidTimerStateTimeProvider() },
                },
                CancellationToken.None).AsTask());

        await Assert.That(exception?.Message).Contains("invalid state");
        await Assert.That(preparer.PrepareCount).IsEqualTo(0);
    }

    /// <summary>Verifies timer creation failures still release the lease.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ExecuteAsyncTimerCreationFailureStillReleasesLease()
    {
        var lease = CreateLease(CreateOperation(FirstSequence));
        var store = CreateStore(lease);
        var preparer = new RecordingPreparer();

        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => PreparedUploadAttemptCoordinator.ExecuteAsync(
                CreateRequest(lease, preparer, store) with
                {
                    Options = CreateOptions() with { TimeProvider = new ThrowingTimerTimeProvider() },
                },
                CancellationToken.None).AsTask());

        await Assert.That(exception?.Message).Contains("timer unavailable");
        await Assert.That(preparer.PrepareCount).IsEqualTo(0);
        await Assert.That(store.ReleaseCount).IsEqualTo(FirstSequence);
    }

    /// <summary>Verifies timer disposal failures cannot skip owned lease release.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ExecuteAsyncTimerDisposeFailureStillReleasesOwnedLease()
    {
        var lease = CreateLease(CreateOperation(FirstSequence));
        var store = CreateStore(lease);
        store.DeniedOperation = lease.Operations[0].OperationId;
        var preparer = new RecordingPreparer();

        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => PreparedUploadAttemptCoordinator.ExecuteAsync(
                CreateRequest(lease, preparer, store) with
                {
                    Options = CreateOptions() with { TimeProvider = new ThrowingTimerDisposeTimeProvider() },
                },
                CancellationToken.None).AsTask());

        await Assert.That(exception?.Message).Contains("timer dispose failed");
        await Assert.That(preparer.Prepared?.DisposeCount).IsEqualTo(FirstSequence);
        await Assert.That(store.ReleaseCount).IsEqualTo(FirstSequence);
    }

    /// <summary>Verifies cancellation callback failures cannot let disposal return before the renewal task drains.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ExecuteAsyncCancelCallbackFailureWaitsForInFlightRenewalToDrain()
    {
        var clock = new FakeTimeProvider(TimestampUtc);
        var lease = CreateLease(clock.GetUtcNow().AddSeconds(SecondSequence), CreateOperation(FirstSequence));
        var store = CreateStore(lease);
        var signals = new RenewalCancellationSignals();
        store.BeforeRenewCompletes = token => HoldRenewalUntilDrainAllowedAsync(signals, token);
        TaskCompletionSource entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource finishReconciliation = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var preparer = new RecordingPreparer();
        var attempt = PreparedUploadAttemptCoordinator.ExecuteAsync(
            CreateRequest(
                lease,
                preparer,
                store,
                async (_, _) =>
                {
                    _ = entered.TrySetResult();
                    await finishReconciliation.Task.WaitAsync(GateTimeout, CancellationToken.None).ConfigureAwait(false);
                }) with
            {
                Options = CreateOptions() with
                {
                    TimeProvider = clock,
                    LeaseRenewalInterval = TimeSpan.FromSeconds(RenewalIntervalSeconds),
                    LeaseRenewalDuration = TimeSpan.FromSeconds(RenewalDurationSeconds),
                },
            },
            CancellationToken.None).AsTask();

        await entered.Task.WaitAsync(GateTimeout, CancellationToken.None).ConfigureAwait(false);
        AdvanceClock(clock, TimeSpan.FromSeconds(RenewalIntervalSeconds));
        await signals.Holding.Task.WaitAsync(GateTimeout, CancellationToken.None).ConfigureAwait(false);
        _ = finishReconciliation.TrySetResult();
        await signals.CancelInvoked.Task.WaitAsync(GateTimeout, CancellationToken.None).ConfigureAwait(false);
        var earlyCompletion = await Task.WhenAny(attempt, Task.Delay(TimeSpan.FromMilliseconds(RenewalDrainProbeMilliseconds), CancellationToken.None)).ConfigureAwait(false);
        await Assert.That(ReferenceEquals(earlyCompletion, attempt)).IsFalse();

        _ = signals.AllowDrain.TrySetResult();
        var exception = await Assert.ThrowsAsync<Exception>(() => attempt);

        await Assert.That(exception?.ToString()).Contains("cancel callback failed");
        await Assert.That(signals.Drained.Task.IsCompletedSuccessfully).IsTrue();
        await Assert.That(preparer.Prepared?.DisposeCount).IsEqualTo(FirstSequence);
        await Assert.That(store.ReleaseCount).IsEqualTo(0);
    }

    /// <summary>Signals that preparation entered and waits for cancellation.</summary>
    /// <param name="prepareStarted">The preparation start signal.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The delayed task.</returns>
    private static ValueTask SignalAndDelayPrepareAsync(TaskCompletionSource prepareStarted, CancellationToken cancellationToken)
    {
        _ = prepareStarted.TrySetResult();
        return new(Task.Delay(GateTimeout, cancellationToken));
    }

    /// <summary>Cancels the caller token when attempt cancellation reaches preparation.</summary>
    /// <param name="prepareStarted">The preparation start signal.</param>
    /// <param name="callerCancellation">The caller cancellation source.</param>
    /// <param name="cancellationToken">The attempt cancellation token.</param>
    /// <returns>The wait task.</returns>
    /// <exception cref="OperationCanceledException">The attempt cancellation reached preparation.</exception>
    private static async ValueTask CancelCallerWhenAttemptCancelsAsync(
        TaskCompletionSource prepareStarted,
        CancellationTokenSource callerCancellation,
        CancellationToken cancellationToken)
    {
        _ = prepareStarted.TrySetResult();
        try
        {
            await Task.Delay(GateTimeout, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            await callerCancellation.CancelAsync().ConfigureAwait(false);
            throw new OperationCanceledException(cancellationToken);
        }
    }

    /// <summary>Throws a send failure after attempt cancellation is observed.</summary>
    /// <param name="sendStarted">The send start signal.</param>
    /// <param name="cancellationToken">The attempt cancellation token.</param>
    /// <returns>The wait task.</returns>
    /// <exception cref="InvalidOperationException">The material send failure.</exception>
    private static async ValueTask ThrowSendAfterAttemptCancelsAsync(TaskCompletionSource sendStarted, CancellationToken cancellationToken)
    {
        _ = sendStarted.TrySetResult();
        TaskCompletionSource attemptCancelled = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var registration = cancellationToken.UnsafeRegister(
            static state =>
            {
                if (state is not TaskCompletionSource completion)
                {
                    throw new InvalidOperationException("The cancellation callback received an invalid test context.");
                }

                _ = completion.TrySetResult();
            },
            attemptCancelled);
        await attemptCancelled.Task.WaitAsync(GateTimeout, CancellationToken.None).ConfigureAwait(false);
        throw new InvalidOperationException(SendFailedMessage);
    }

    /// <summary>Signals that preparation entered and waits for an external release.</summary>
    /// <param name="prepareStarted">The preparation start signal.</param>
    /// <param name="finishPrepare">The preparation release signal.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The wait task.</returns>
    private static ValueTask SignalAndWaitPrepareAsync(
        TaskCompletionSource prepareStarted,
        TaskCompletionSource finishPrepare,
        CancellationToken cancellationToken)
    {
        _ = prepareStarted.TrySetResult();
        return new(finishPrepare.Task.WaitAsync(GateTimeout, cancellationToken));
    }

    /// <summary>Advances a fake clock.</summary>
    /// <param name="clock">The fake clock.</param>
    /// <param name="duration">The duration.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void AdvanceClock(FakeTimeProvider clock, TimeSpan duration) => clock.Advance(duration);

    /// <summary>Holds an in-flight renewal until disposal has observed a throwing cancellation callback.</summary>
    /// <param name="signals">The cancellation signals.</param>
    /// <param name="cancellationToken">The renewal cancellation token.</param>
    /// <returns>The wait task.</returns>
    private static async ValueTask HoldRenewalUntilDrainAllowedAsync(
        RenewalCancellationSignals signals,
        CancellationToken cancellationToken)
    {
        await using var registration = cancellationToken.UnsafeRegister(ThrowCancelCallback, signals);
        _ = signals.Holding.TrySetResult();
        try
        {
            await signals.AllowDrain.Task.WaitAsync(GateTimeout, CancellationToken.None).ConfigureAwait(false);
        }
        finally
        {
            _ = signals.Drained.TrySetResult();
        }
    }

    /// <summary>Signals and throws from a cancellation callback.</summary>
    /// <param name="state">The callback state.</param>
    /// <exception cref="InvalidOperationException">The callback is intentionally failed.</exception>
    private static void ThrowCancelCallback(object? state)
    {
        if (state is not RenewalCancellationSignals signals)
        {
            throw new InvalidOperationException("The cancellation callback received an invalid test context.");
        }

        _ = signals.CancelInvoked.TrySetResult();
        throw new InvalidOperationException("cancel callback failed");
    }

    /// <summary>Signals used by in-flight renewal cancellation tests.</summary>
    private sealed class RenewalCancellationSignals
    {
        /// <summary>Gets a signal set when the cancellation callback runs.</summary>
        public TaskCompletionSource CancelInvoked { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets a signal set when the renewal is held after callback registration.</summary>
        public TaskCompletionSource Holding { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets a signal that releases the held renewal.</summary>
        public TaskCompletionSource AllowDrain { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets a signal set when the held renewal drains.</summary>
        public TaskCompletionSource Drained { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
}
