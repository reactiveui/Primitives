// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Connection retry timing tests for <see cref="SyncEngine"/>.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>Verifies a wake received during a failed connection attempt skips the next retry delay.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task TriggerSyncAsyncDuringReconnectWakesNextRetryWithoutAdvancingClock()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var reconnectEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseReconnect = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var transport = new ScriptedConnectTransport(async (attempt, _) =>
        {
            if (attempt == 1)
            {
                throw new IOException(UnreachableMessage);
            }

            if (attempt == ExpectedCapacityCommitAttempts)
            {
                reconnectEntered.SetResult();
                await releaseReconnect.Task.ConfigureAwait(false);
                throw new IOException("reconnect failed");
            }

            return new RecordingSession();
        });
        var options = CreateOfflineOptions() with
        {
            CircuitBreaker = new() { FailureThreshold = BreakerFailureThreshold + ExpectedSingleOperation, OpenDuration = BreakerOpenDuration },
        };
        var states = new RecordingObserver<SyncState>();
        await using var engine = CreateEngineWithTransportAdapter(transport, options, clock);
        using var subscription = engine.SyncStates.Subscribe(states);

        await engine.StartAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        await WaitForConditionAsync(() => clock.HasTimerDueIn(OfflineRetryDelay));
        clock.Advance(OfflineRetryDelay);
        await reconnectEntered.Task.WaitAsync(GuardTimeout);

        await engine.TriggerSyncAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        releaseReconnect.SetResult();

        await WaitForConditionAsync(() => states.Values.Exists(static state => state.Status == SyncLifecycleStatus.Online));
        await Assert.That(transport.Attempts).IsEqualTo(BreakerFailureThreshold + ExpectedSingleOperation);
        await Assert.That(clock.GetUtcNow()).IsEqualTo(DateTimeOffset.UnixEpoch + OfflineRetryDelay);
    }

    /// <summary>Verifies an exhausted connection retry episode respects the larger of the policy maximum and server hint.</summary>
    /// <param name="retryAfterMilliseconds">The server hint on the failure that exhausts the episode.</param>
    /// <param name="expectedDelayMilliseconds">The delay before the next connection attempt.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(500, 1000)]
    [Arguments(20_000, 20_000)]
    public async Task BackgroundReconnectAfterExhaustionUsesServerRetryHint(
        int retryAfterMilliseconds,
        int expectedDelayMilliseconds)
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var transport = new RecordingTransport();
        transport.ConnectFailures.Enqueue(new IOException("first"));
        transport.ConnectFailures.Enqueue(CreateTransportFailure(
            RetryFailureKind.Transient,
            TimeSpan.FromMilliseconds(retryAfterMilliseconds)));
        var options = CreateOfflineOptions() with
        {
            Retry = CreateOfflineOptions().Retry with { MaximumRetryAttempts = 1 },
            CircuitBreaker = new() { FailureThreshold = BreakerFailureThreshold + ExpectedSingleOperation, OpenDuration = BreakerOpenDuration },
        };
        var states = new RecordingObserver<SyncState>();
        await using var engine = CreateEngine(transport: transport, options: options, timeProvider: clock);
        using var subscription = engine.SyncStates.Subscribe(states);

        await engine.StartAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        await WaitForConditionAsync(() => clock.HasTimerDueIn(OfflineRetryDelay));
        clock.Advance(OfflineRetryDelay);
        await WaitForConditionAsync(() => transport.ConnectCalls == ExpectedCapacityCommitAttempts);
        await WaitForConditionAsync(() => states.Values.Count(static state => state.Status == SyncLifecycleStatus.Offline)
            >= ExpectedCapacityCommitAttempts);

        var expectedDelay = TimeSpan.FromMilliseconds(expectedDelayMilliseconds);
        var exhausted = states.Values.FindLast(static state => state.Status == SyncLifecycleStatus.Offline)!;
        await Assert.That(exhausted.RetryAfter).IsEqualTo(expectedDelay);
        await WaitForConditionAsync(() => clock.HasTimerDueIn(expectedDelay));

        clock.Advance(expectedDelay - TimeSpan.FromMilliseconds(1));
        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedCapacityCommitAttempts);
        clock.Advance(TimeSpan.FromMilliseconds(1));
        await WaitForConditionAsync(() => states.Values.Exists(static state => state.Status == SyncLifecycleStatus.Online));
        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedCapacityCommitAttempts + ExpectedSingleOperation);
    }
}
