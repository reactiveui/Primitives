// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Pending connection shutdown tests for <see cref="SyncEngine"/>.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>Verifies shutdown cancels connection I/O without waiting for the unavailable remote peer.</summary>
    /// <param name="dispose">Whether to dispose instead of stopping the engine.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ShutdownCancelsPendingConnectionWithoutRemoteRelease(bool dispose)
    {
        var transport = new PendingConnectionTransport();
        await using var engine = CreatePendingConnectionEngine(transport);

        var start = engine.StartAsync(CancellationToken.None).AsTask();
        Task? shutdown = null;
        try
        {
            await transport.Entered.Task.WaitAsync(GuardTimeout);
            shutdown = dispose ? engine.DisposeAsync().AsTask() : engine.StopAsync(CancellationToken.None).AsTask();
            await transport.Canceled.Task.WaitAsync(GuardTimeout);
            await shutdown.WaitAsync(GuardTimeout);
            await Assert.That(transport.Release.Task.IsCompleted).IsFalse();
            _ = await Assert.ThrowsAsync<OperationCanceledException>(() => start.WaitAsync(GuardTimeout));
        }
        finally
        {
            _ = transport.Release.TrySetResult();
            await ObserveTaskCompletionAsync(start).ConfigureAwait(false);
            if (shutdown is not null)
            {
                await ObserveTaskCompletionAsync(shutdown).ConfigureAwait(false);
            }
        }
    }

    /// <summary>Verifies a session returned after shutdown is disposed without becoming an active session.</summary>
    /// <param name="dispose">Whether to dispose instead of stopping the engine.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ShutdownDuringConnectionDisposesLateSessionWithoutPublishingOnline(bool dispose)
    {
        var transport = new PendingConnectionTransport { ReturnSessionAfterCancellation = true };
        await using var engine = CreatePendingConnectionEngine(transport);
        var states = new RecordingObserver<SyncState>();
        using var subscription = engine.SyncStates.Subscribe(states);
        var start = engine.StartAsync(CancellationToken.None).AsTask();
        Task? stop = null;
        try
        {
            await transport.Entered.Task.WaitAsync(GuardTimeout);
            stop = dispose ? engine.DisposeAsync().AsTask() : engine.StopAsync(CancellationToken.None).AsTask();
            await transport.Canceled.Task.WaitAsync(GuardTimeout);
            await Assert.That(stop.IsCompleted).IsFalse();
            transport.Release.SetResult();
            await stop.WaitAsync(GuardTimeout);
            await Assert.That(transport.Session.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(states.Values.Exists(static state => state.Status == SyncLifecycleStatus.Online)).IsFalse();
        }
        finally
        {
            _ = transport.Release.TrySetResult();
            await ObserveTaskCompletionAsync(start);
            if (stop is not null)
            {
                await ObserveTaskCompletionAsync(stop);
            }
        }
    }

    /// <summary>Verifies canceled startup does not poison a subsequent independently owned connection attempt.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task StartAfterCanceledConnectionUsesFreshStartupCancellation()
    {
        var transport = new PendingConnectionTransport();
        await using var engine = CreatePendingConnectionEngine(transport);
        var firstStart = engine.StartAsync(CancellationToken.None).AsTask();
        try
        {
            await transport.Entered.Task.WaitAsync(GuardTimeout);
            await engine.StopAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
            await Assert.That(transport.Canceled.Task.IsCompleted).IsTrue();
            transport.Release.SetResult();
            await engine.StartAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
            await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedCapacityCommitAttempts);
            await Assert.That(transport.Session.DisposeCalls).IsEqualTo(0);
            await engine.StopAsync(CancellationToken.None);
            await Assert.That(transport.Session.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
        }
        finally
        {
            _ = transport.Release.TrySetResult();
            await ObserveTaskCompletionAsync(firstStart);
        }
    }

    /// <summary>Verifies canceling one startup caller leaves shared connection ownership with the engine.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CanceledStartCallerDoesNotCancelSharedConnection()
    {
        var transport = new PendingConnectionTransport();
        await using var engine = CreatePendingConnectionEngine(transport);
        using var cancellation = new CancellationTokenSource();
        var start = engine.StartAsync(cancellation.Token).AsTask();
        try
        {
            await transport.Entered.Task.WaitAsync(GuardTimeout);
            await cancellation.CancelAsync();
            _ = await Assert.ThrowsAsync<OperationCanceledException>(() => start.WaitAsync(GuardTimeout));
            await Assert.That(transport.Canceled.Task.IsCompleted).IsFalse();
            await engine.StopAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
            await Assert.That(transport.Canceled.Task.IsCompleted).IsTrue();
        }
        finally
        {
            _ = transport.Release.TrySetResult();
            await ObserveTaskCompletionAsync(start);
        }
    }

    /// <summary>Verifies cancellation callback failures remain observable while owned resources still drain.</summary>
    /// <param name="dispose">Whether to dispose instead of stopping the engine.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ShutdownDrainsPendingConnectionWhenCancellationCallbackThrows(bool dispose)
    {
        var transport = new PendingConnectionTransport { ThrowOnCancellation = true };
        var store = new RecordingStore();
        var engine = CreatePendingConnectionEngine(transport, store);
        var start = engine.StartAsync(CancellationToken.None).AsTask();
        Task? shutdown = null;
        try
        {
            await transport.Entered.Task.WaitAsync(GuardTimeout);
            shutdown = dispose ? engine.DisposeAsync().AsTask() : engine.StopAsync(CancellationToken.None).AsTask();
            _ = await Assert.ThrowsAsync<AggregateException>(() => shutdown.WaitAsync(GuardTimeout));
            await Assert.That(transport.Canceled.Task.IsCompleted).IsTrue();
            _ = await Assert.ThrowsAsync<AggregateException>(() => start.WaitAsync(GuardTimeout));
            if (!dispose)
            {
                await engine.DisposeAsync().AsTask().WaitAsync(GuardTimeout);
            }

            await Assert.That(store.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(transport.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
        }
        finally
        {
            _ = transport.Release.TrySetResult();
            await ObserveTaskCompletionAsync(start);
            if (shutdown is not null)
            {
                await ObserveTaskCompletionAsync(shutdown);
            }

            await ObserveTaskCompletionAsync(engine.DisposeAsync().AsTask());
        }
    }

    /// <summary>Creates an engine that owns the pending connection transport and test store.</summary>
    /// <param name="transport">The pending connection transport.</param>
    /// <param name="store">The optional store whose ownership is observed by the test.</param>
    /// <returns>The configured engine.</returns>
    private static SyncEngine CreatePendingConnectionEngine(PendingConnectionTransport transport, RecordingStore? store = null) =>
        new(new()
        {
            Store = store ?? new(),
            Transport = transport,
            StoreOwnership = SyncEngineDependencyOwnership.Owned,
            TransportOwnership = SyncEngineDependencyOwnership.Owned,
            Options = OccasionallyConnectedOptions.Default,
            StoreInitialization = new("sync-engine-tests", RequiredSchemaVersion: 1, RequireAuthenticatedEncryptionAtRest: false) { ClientId = EngineClientId },
            Client = new(EngineClientId),
        });

    /// <summary>Models a remote connection that remains unavailable until cancellation.</summary>
    private sealed class PendingConnectionTransport : IRemoteTransportAdapter
    {
        /// <summary>The callback retained until transport disposal so cancellation cannot unregister it before invocation.</summary>
        private CancellationTokenRegistration _cancellationRegistration;

        /// <summary>Gets the signal indicating connection entry.</summary>
        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets the signal indicating connection cancellation.</summary>
        public TaskCompletionSource Canceled { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets the emergency release used only by test cleanup.</summary>
        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets the session returned after the connection gate releases.</summary>
        public RecordingSession Session { get; } = new();

        /// <summary>Gets a value indicating whether a canceled attempt may still return a late session.</summary>
        public bool ReturnSessionAfterCancellation { get; init; }

        /// <summary>Gets a value indicating whether a transport cancellation callback throws.</summary>
        public bool ThrowOnCancellation { get; init; }

        /// <summary>Gets the number of connection attempts.</summary>
        public int ConnectCalls { get; private set; }

        /// <summary>Gets the number of adapter disposal calls.</summary>
        public int DisposeCalls { get; private set; }

        /// <inheritdoc/>
        public RemoteTransportCapabilities Capabilities => RecordingTransportUploadCapabilities;

        /// <inheritdoc/>
        public async ValueTask<IRemoteTransportSession> ConnectAsync(TransportConnectRequest request, CancellationToken cancellationToken)
        {
            ConnectCalls++;
            if (ThrowOnCancellation)
            {
                _cancellationRegistration = cancellationToken.Register(static () => throw new InvalidOperationException("Connection cancellation callback failed."));
            }

            _ = Entered.TrySetResult();
            try
            {
                await Release.Task.WaitAsync(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
                return Session;
            }
            catch (OperationCanceledException)
            {
                _ = Canceled.TrySetResult();
                if (ReturnSessionAfterCancellation)
                {
                    await Release.Task.ConfigureAwait(false);
                    return Session;
                }

                throw;
            }
        }

        /// <inheritdoc/>
        public async ValueTask DisposeAsync()
        {
            await _cancellationRegistration.DisposeAsync().ConfigureAwait(false);
            DisposeCalls++;
        }
    }
}
