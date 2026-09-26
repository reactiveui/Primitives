// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Offline reconnection lifecycle race tests for <see cref="SyncEngine"/>.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>Verifies triggering synchronization after background reconnection faulted reports that the engine is not running.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task TriggerSyncAsyncAfterBackgroundConnectFaultThrows()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var transport = new RecordingTransport();
        transport.ConnectFailures.Enqueue(new IOException(UnreachableMessage));
        transport.ConnectFailures.Enqueue(new UnauthorizedAccessException("denied"));
        var states = new RecordingObserver<SyncState>();
        await using var engine = CreateEngine(transport: transport, options: CreateOfflineOptions(), timeProvider: clock);
        using var stateSubscription = engine.SyncStates.Subscribe(states);

        await engine.StartAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        await WaitForConditionAsync(() => clock.HasTimerDueIn(OfflineRetryDelay));
        clock.Advance(OfflineRetryDelay);
        await WaitForConditionAsync(() => states.Values.Exists(static state => state.Status == SyncLifecycleStatus.Faulted));

        await Assert.That(async () => await engine.TriggerSyncAsync(CancellationToken.None).ConfigureAwait(false))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedCapacityCommitAttempts);
    }

    /// <summary>Verifies a transport failure raised while stop cancels an in-flight reconnect does not fail the stop.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task StopAsyncIgnoresTransportFailureRaisedByCancelledReconnect()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var reconnectEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var transport = new ScriptedConnectTransport(async (attempt, token) =>
        {
            if (attempt == 1)
            {
                throw new IOException(UnreachableMessage);
            }

            reconnectEntered.SetResult();
            var cancelled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            await using (token.UnsafeRegister(static state => ((TaskCompletionSource)state!).SetResult(), cancelled))
            {
                await cancelled.Task.ConfigureAwait(false);
            }

            throw new ObjectDisposedException("socket");
        });
        await using var engine = CreateEngineWithTransportAdapter(transport, CreateOfflineOptions(), clock);

        await engine.StartAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        await WaitForConditionAsync(() => clock.HasTimerDueIn(OfflineRetryDelay));
        clock.Advance(OfflineRetryDelay);
        await reconnectEntered.Task.WaitAsync(GuardTimeout);

        await engine.StopAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);

        await Assert.That(transport.Attempts).IsEqualTo(ExpectedCapacityCommitAttempts);
    }

    /// <summary>Verifies a session connected after stop was accepted is disposed instead of published.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ReconnectCompletingAfterStopDisposesSession()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var reconnectEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseReconnect = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var lateSession = new RecordingSession();
        await using var transport = new ScriptedConnectTransport(async (attempt, _) =>
        {
            if (attempt == 1)
            {
                throw new IOException(UnreachableMessage);
            }

            reconnectEntered.SetResult();
            await releaseReconnect.Task.ConfigureAwait(false);
            return lateSession;
        });
        await using var engine = CreateEngineWithTransportAdapter(transport, CreateOfflineOptions(), clock);

        await engine.StartAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        await WaitForConditionAsync(() => clock.HasTimerDueIn(OfflineRetryDelay));
        clock.Advance(OfflineRetryDelay);
        await reconnectEntered.Task.WaitAsync(GuardTimeout);

        var stop = engine.StopAsync(CancellationToken.None).AsTask();
        releaseReconnect.SetResult();
        await stop.WaitAsync(GuardTimeout);

        await Assert.That(lateSession.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(async () => await engine.TriggerSyncAsync(CancellationToken.None).ConfigureAwait(false))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies disposing an offline engine cancels reconnection and releases owned dependencies.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DisposeAsyncWhileOfflineCancelsReconnect()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var transport = new RecordingTransport();
        transport.ConnectFailures.Enqueue(new IOException(UnreachableMessage));
        var engine = CreateEngine(transport: transport, options: CreateOfflineOptions(), timeProvider: clock);

        await engine.StartAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        await WaitForConditionAsync(() => clock.HasTimerDueIn(OfflineRetryDelay));
        await engine.DisposeAsync().AsTask().WaitAsync(GuardTimeout);
        clock.Advance(OfflineRetryDelay);

        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(transport.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(clock.TimerCount).IsEqualTo(0);
    }

    /// <summary>A transport whose connection attempts are scripted by the test.</summary>
    /// <param name="connect">The script invoked with the one-based attempt number and cancellation token.</param>
    private sealed class ScriptedConnectTransport(Func<int, CancellationToken, Task<IRemoteTransportSession>> connect) : IRemoteTransportAdapter
    {
        /// <summary>The number of connection attempts.</summary>
        private int _attempts;

        /// <summary>Gets the number of connection attempts.</summary>
        public int Attempts => Volatile.Read(ref _attempts);

        /// <inheritdoc/>
        public RemoteTransportCapabilities Capabilities => RecordingTransportUploadCapabilities;

        /// <inheritdoc/>
        public ValueTask<IRemoteTransportSession> ConnectAsync(TransportConnectRequest request, CancellationToken cancellationToken) =>
            new(connect(Interlocked.Increment(ref _attempts), cancellationToken));

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
