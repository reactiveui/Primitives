// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Offline startup, background reconnection, and circuit breaker tests for <see cref="SyncEngine"/>.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>The transient connection failure message used by offline startup tests.</summary>
    private const string UnreachableMessage = "endpoint unreachable";

    /// <summary>The number of connection attempts made before a circuit breaker opens in these tests.</summary>
    private const int BreakerFailureThreshold = 2;

    /// <summary>The retry delay used by offline startup tests.</summary>
    private static readonly TimeSpan OfflineRetryDelay = TimeSpan.FromSeconds(1);

    /// <summary>The circuit breaker open duration used by offline startup tests.</summary>
    private static readonly TimeSpan BreakerOpenDuration = TimeSpan.FromSeconds(10);

    /// <summary>Verifies a transient first connection failure completes startup offline and later reconnects.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task StartAsyncCompletesOfflineAfterTransientConnectFailureAndReconnects()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var transport = new RecordingTransport();
        transport.ConnectFailures.Enqueue(new IOException(UnreachableMessage));
        var states = new RecordingObserver<SyncState>();
        await using var engine = CreateEngine(transport: transport, options: CreateOfflineOptions(), timeProvider: clock);
        using var stateSubscription = engine.SyncStates.Subscribe(states);

        await engine.StartAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);

        await WaitForConditionAsync(() => states.Values.Exists(static state => state.Status == SyncLifecycleStatus.Offline));
        var offline = states.Values.Find(static state => state.Status == SyncLifecycleStatus.Offline)!;
        await Assert.That(offline.NetworkAvailable).IsFalse();
        await Assert.That(offline.RetryAfter).IsEqualTo(OfflineRetryDelay);
        await Assert.That(offline.ReasonCode).IsEqualTo(SyncEngine.TransportUnavailableReasonCode);
        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedSingleOperation);

        await WaitForConditionAsync(() => clock.HasTimerDueIn(OfflineRetryDelay));
        clock.Advance(OfflineRetryDelay);

        await WaitForConditionAsync(() => states.Values.Exists(static state => state.Status == SyncLifecycleStatus.Online));
        var online = states.Values.FindLast(static state => state.Status == SyncLifecycleStatus.Online)!;
        await Assert.That(online.NetworkAvailable).IsTrue();
        await Assert.That(online.RetryAfter).IsNull();
        await Assert.That(online.ReasonCode).IsNull();
        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedCapacityCommitAttempts);
    }

    /// <summary>Verifies a permanent first connection failure still fails startup.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task StartAsyncFailsWhenFirstConnectFailureIsPermanent()
    {
        var transport = new RecordingTransport();
        transport.ConnectFailures.Enqueue(new UnauthorizedAccessException("denied"));
        await using var engine = CreateEngine(transport: transport, options: CreateOfflineOptions());

        await Assert.That(async () => await engine.StartAsync(CancellationToken.None).ConfigureAwait(false))
            .ThrowsExactly<UnauthorizedAccessException>();
        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedSingleOperation);
    }

    /// <summary>Verifies consecutive transient failures open the circuit breaker, which rejects attempts until it probes.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CircuitBreakerRejectsReconnectUntilOpenDurationElapses()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var transport = new RecordingTransport();
        transport.ConnectFailures.Enqueue(new IOException("first"));
        transport.ConnectFailures.Enqueue(new IOException("second"));
        var states = new RecordingObserver<SyncState>();
        await using var engine = CreateEngine(transport: transport, options: CreateOfflineOptions(), timeProvider: clock);
        using var stateSubscription = engine.SyncStates.Subscribe(states);

        await engine.StartAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        await WaitForConditionAsync(() => clock.HasTimerDueIn(OfflineRetryDelay));
        clock.Advance(OfflineRetryDelay);
        await WaitForConditionAsync(() => transport.ConnectCalls == BreakerFailureThreshold);
        await WaitForConditionAsync(() => clock.HasTimerDueIn(OfflineRetryDelay));
        clock.Advance(OfflineRetryDelay);

        var remainingOpen = BreakerOpenDuration - OfflineRetryDelay;
        await WaitForConditionAsync(() => states.Values.Exists(static state => state.ReasonCode == SyncEngine.CircuitOpenReasonCode));
        var open = states.Values.Find(static state => state.ReasonCode == SyncEngine.CircuitOpenReasonCode)!;
        await Assert.That(open.Status).IsEqualTo(SyncLifecycleStatus.Offline);
        await Assert.That(open.RetryAfter).IsEqualTo(remainingOpen);
        await Assert.That(transport.ConnectCalls).IsEqualTo(BreakerFailureThreshold);

        await WaitForConditionAsync(() => clock.HasTimerDueIn(remainingOpen));
        clock.Advance(remainingOpen);

        await WaitForConditionAsync(() => states.Values.Exists(static state => state.Status == SyncLifecycleStatus.Online));
        await Assert.That(transport.ConnectCalls).IsEqualTo(BreakerFailureThreshold + ExpectedSingleOperation);
    }

    /// <summary>Verifies triggering synchronization while offline reconnects without waiting for the retry delay.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task TriggerSyncAsyncWhileOfflineReconnectsImmediately()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var transport = new RecordingTransport();
        transport.ConnectFailures.Enqueue(new IOException(UnreachableMessage));
        var states = new RecordingObserver<SyncState>();
        await using var engine = CreateEngine(transport: transport, options: CreateOfflineOptions(), timeProvider: clock);
        using var stateSubscription = engine.SyncStates.Subscribe(states);

        await engine.StartAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        await WaitForConditionAsync(() => clock.HasTimerDueIn(OfflineRetryDelay));

        await engine.TriggerSyncAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);

        await WaitForConditionAsync(() => states.Values.Exists(static state => state.Status == SyncLifecycleStatus.Online));
        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedCapacityCommitAttempts);
    }

    /// <summary>Verifies stopping an offline engine cancels reconnection, and restarting connects again.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task StopAsyncWhileOfflineCancelsReconnectAndRestartConnects()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var transport = new RecordingTransport();
        transport.ConnectFailures.Enqueue(new IOException(UnreachableMessage));
        var states = new RecordingObserver<SyncState>();
        await using var engine = CreateEngine(transport: transport, options: CreateOfflineOptions(), timeProvider: clock);
        using var stateSubscription = engine.SyncStates.Subscribe(states);

        await engine.StartAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        await WaitForConditionAsync(() => clock.HasTimerDueIn(OfflineRetryDelay));

        await engine.StopAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        clock.Advance(OfflineRetryDelay);

        await WaitForConditionAsync(() => states.Values.Exists(static state => state.Status == SyncLifecycleStatus.Stopped));
        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(clock.TimerCount).IsEqualTo(0);
        await Assert.That(async () => await engine.TriggerSyncAsync(CancellationToken.None).ConfigureAwait(false))
            .ThrowsExactly<InvalidOperationException>();

        await engine.StartAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);

        await WaitForConditionAsync(() => states.Values.Exists(static state => state.Status == SyncLifecycleStatus.Online));
        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedRestartConnectCalls);
    }

    /// <summary>Verifies a permanent failure during background reconnection reports a fault and the faulted state.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task BackgroundReconnectPermanentFailurePublishesFaultAndFaultedState()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var transport = new RecordingTransport();
        transport.ConnectFailures.Enqueue(new IOException(UnreachableMessage));
        transport.ConnectFailures.Enqueue(new UnauthorizedAccessException("denied"));
        var states = new RecordingObserver<SyncState>();
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        await using var engine = CreateEngine(transport: transport, options: CreateOfflineOptions(), timeProvider: clock);
        using var stateSubscription = engine.SyncStates.Subscribe(states);
        using var faultSubscription = engine.Faults.Subscribe(faults);

        await engine.StartAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        await WaitForConditionAsync(() => clock.HasTimerDueIn(OfflineRetryDelay));
        clock.Advance(OfflineRetryDelay);

        await WaitForConditionAsync(() => states.Values.Exists(static state => state.Status == SyncLifecycleStatus.Faulted));
        await WaitForConditionAsync(() => faults.Values.Count == ExpectedSingleOperation);
        var faulted = states.Values.Find(static state => state.Status == SyncLifecycleStatus.Faulted)!;
        await Assert.That(faulted.ReasonCode).IsEqualTo(SyncEngine.ConnectFaultCode);
        await Assert.That(faults.Values[0].Code).IsEqualTo(SyncEngine.ConnectFaultCode);
        await Assert.That(clock.TimerCount).IsEqualTo(0);

        await engine.StopAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedCapacityCommitAttempts);
    }

    /// <summary>Verifies an offline engine keeps admitting durable local commits before it reconnects.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task OfflineLocalCommitIsAdmittedBeforeReconnect()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var transport = new RecordingTransport();
        transport.ConnectFailures.Enqueue(new IOException(UnreachableMessage));
        await using var engine = CreateEngine(transport: transport, options: CreateOfflineOptions(), timeProvider: clock);
        var participant = new RecordingParticipant();
        using var registration = engine.RegisterParticipant(participant);

        await engine.StartAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        var receipt = await engine.EnqueueOperationAsync(CreateOperation(), CancellationToken.None).AsTask().WaitAsync(GuardTimeout);

        await Assert.That(receipt.State).IsEqualTo(SyncOperationState.SavedLocally);
        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedSingleOperation);
    }

    /// <summary>Creates options with deterministic retry and circuit breaker timing.</summary>
    /// <returns>The options.</returns>
    private static OccasionallyConnectedOptions CreateOfflineOptions() =>
        OccasionallyConnectedOptions.Default with
        {
            Retry = OccasionallyConnectedOptions.Default.Retry with
            {
                MinimumDelay = OfflineRetryDelay,
                MaximumDelay = OfflineRetryDelay,
            },
            CircuitBreaker = new() { FailureThreshold = BreakerFailureThreshold, OpenDuration = BreakerOpenDuration },
        };
}
