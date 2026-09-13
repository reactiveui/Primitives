// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="LoopbackTransportAdapter"/>.</summary>
/// <content>Lifecycle tests for <see cref="LoopbackTransportAdapter"/>.</content>
public sealed partial class LoopbackTransportAdapterTests
{
    /// <summary>Verifies subscription disposal releases its lease even when the upstream dispose path faults.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SubscriptionDisposalFaultStillReleasesSession()
    {
        var source = new ThrowingDisposeEnumerable(CreateReceiveBatch(CreateRemoteEvent()));
        var hub = new RecordingHub { SubscribeHandler = (_, _, _) => source };
        await using var adapter = new LoopbackTransportAdapter(CreateOptions(hub));
        var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var enumerator = session.SubscribeAsync(CreateSubscribeRequest(), CancellationToken.None).GetAsyncEnumerator();
        await Assert.That(await enumerator.MoveNextAsync()).IsTrue();

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => enumerator.DisposeAsync().AsTask());
        await session.DisposeAsync();
        await using var next = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);

        await Assert.That(next.NegotiatedCapabilities).IsEqualTo(CreateCapabilities());
    }

    /// <summary>Verifies session disposal releases the adapter slot when an owned subscription disposal faults.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SessionDisposalFaultStillReleasesAdapterSlot()
    {
        var source = new ThrowingDisposeEnumerable(CreateReceiveBatch(CreateRemoteEvent()));
        var hub = new RecordingHub { SubscribeHandler = (_, _, _) => source };
        await using var adapter = new LoopbackTransportAdapter(CreateOptions(hub));
        var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var enumerator = session.SubscribeAsync(CreateSubscribeRequest(), CancellationToken.None).GetAsyncEnumerator();
        await Assert.That(await enumerator.MoveNextAsync()).IsTrue();

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => session.DisposeAsync().AsTask());
        await using var next = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        await Assert.That(next.NegotiatedCapabilities).IsEqualTo(CreateCapabilities());
    }

    /// <summary>Verifies adapter disposal propagates an active session disposal fault.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task AdapterDisposalPropagatesActiveSessionDisposalFault()
    {
        var source = new ThrowingDisposeEnumerable(CreateReceiveBatch(CreateRemoteEvent()));
        var hub = new RecordingHub { SubscribeHandler = (_, _, _) => source };
        var adapter = new LoopbackTransportAdapter(CreateOptions(hub));
        var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var enumerator = session.SubscribeAsync(CreateSubscribeRequest(), CancellationToken.None).GetAsyncEnumerator();
        await Assert.That(await enumerator.MoveNextAsync()).IsTrue();

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => adapter.DisposeAsync().AsTask());
    }

    /// <summary>Verifies a subscription registered after disposal starts is closed immediately.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SubscribeRegistrationClosesEnumeratorWhenDisposeStartsDuringHubCall()
    {
        IRemoteTransportSession capturedSession = new PlaceholderTransportSession();
        TaskCompletionSource<Task> disposeStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var hub = new RecordingHub
        {
            SubscribeHandler = (_, _, _) =>
            {
                _ = disposeStarted.TrySetResult(capturedSession.DisposeAsync().AsTask());
                return YieldBatches(CreateReceiveBatch(CreateRemoteEvent()));
            },
        };
        await using var adapter = new LoopbackTransportAdapter(CreateOptions(hub));
        capturedSession = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);

        var enumerator = capturedSession.SubscribeAsync(CreateSubscribeRequest(), CancellationToken.None).GetAsyncEnumerator();
        await (await disposeStarted.Task.ConfigureAwait(false)).ConfigureAwait(false);
        await Assert.That(hub.SubscribeCalls).IsEqualTo(1);
        await enumerator.DisposeAsync();
    }

    /// <summary>Verifies cancellation callback failures do not skip upstream enumerator disposal.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SubscriptionCancellationCallbackFaultStillDisposesUpstreamEnumerator()
    {
        TaskCompletionSource disposed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var source = new CancellationCallbackFaultEnumerable(disposed, false);
        var hub = new RecordingHub { SubscribeHandler = (_, _, _) => source };
        await using var adapter = new LoopbackTransportAdapter(CreateOptions(hub));
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var enumerator = session.SubscribeAsync(CreateSubscribeRequest(), CancellationToken.None).GetAsyncEnumerator();

        Exception? failure = null;
        try
        {
            await enumerator.DisposeAsync();
        }
        catch (Exception exception)
        {
            failure = exception;
        }

        await disposed.Task.WaitAsync(GateTimeout).ConfigureAwait(false);
        await Assert.That(failure).IsNotNull();
    }

    /// <summary>Verifies cancellation and upstream disposal failures are reported together after cleanup.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SubscriptionCancellationCallbackFaultAggregatesUpstreamDisposeFailure()
    {
        TaskCompletionSource disposed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var source = new CancellationCallbackFaultEnumerable(disposed, true);
        var hub = new RecordingHub { SubscribeHandler = (_, _, _) => source };
        await using var adapter = new LoopbackTransportAdapter(CreateOptions(hub));
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var enumerator = session.SubscribeAsync(CreateSubscribeRequest(), CancellationToken.None).GetAsyncEnumerator();

        _ = await Assert.ThrowsExactlyAsync<AggregateException>(() => enumerator.DisposeAsync().AsTask());
        await disposed.Task.WaitAsync(GateTimeout).ConfigureAwait(false);
    }

    /// <summary>Verifies a malformed upstream batch closes the subscription instead of resuming later batches.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SubscriptionMoveFailureClosesEnumeratorAndReleasesCapacity()
    {
        var invalid = new RemoteEventBatch(Guid.NewGuid(), new("sensor/humidity"), null, "cursor-2", []);
        var valid = new RemoteEventBatch(Guid.NewGuid(), Stream, null, "cursor-3", []);
        var subscriptionCalls = 0;
        var hub = new RecordingHub { SubscribeHandler = (_, _, _) => Interlocked.Increment(ref subscriptionCalls) == 1 ? YieldBatches(invalid, valid) : YieldBatches() };
        await using var adapter = new LoopbackTransportAdapter(CreateOptions(hub) with { MaximumConcurrentSubscriptions = 1 });
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var enumerator = session.SubscribeAsync(CreateSubscribeRequest(), CancellationToken.None).GetAsyncEnumerator();

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => enumerator.MoveNextAsync().AsTask());
        await Assert.That(await enumerator.MoveNextAsync()).IsFalse();

        var replacement = session.SubscribeAsync(CreateSubscribeRequest(), CancellationToken.None).GetAsyncEnumerator();
        await Assert.That(await replacement.MoveNextAsync()).IsFalse();
        await replacement.DisposeAsync();
    }

    /// <summary>Verifies a terminal move without a move failure propagates upstream disposal failure directly.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SubscriptionCompletionDisposalFailurePropagatesUpstreamFailure()
    {
        var source = new ThrowingDisposeEnumerable(CreateReceiveBatch(CreateRemoteEvent()));
        var hub = new RecordingHub { SubscribeHandler = (_, _, _) => source };
        await using var adapter = new LoopbackTransportAdapter(CreateOptions(hub));
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var enumerator = session.SubscribeAsync(CreateSubscribeRequest(), CancellationToken.None).GetAsyncEnumerator();

        await Assert.That(await enumerator.MoveNextAsync()).IsTrue();
        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => enumerator.MoveNextAsync().AsTask());
    }

    /// <summary>Verifies a move failure reports an upstream disposal failure without resuming later batches.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SubscriptionMoveFailureAggregatesUpstreamDisposeFailure()
    {
        var invalid = new RemoteEventBatch(Guid.NewGuid(), new("sensor/humidity"), null, "cursor-2", []);
        var source = new ThrowingDisposeEnumerable(invalid);
        var hub = new RecordingHub { SubscribeHandler = (_, _, _) => source };
        await using var adapter = new LoopbackTransportAdapter(CreateOptions(hub));
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var enumerator = session.SubscribeAsync(CreateSubscribeRequest(), CancellationToken.None).GetAsyncEnumerator();

        _ = await Assert.ThrowsExactlyAsync<AggregateException>(() => enumerator.MoveNextAsync().AsTask());
    }

    /// <summary>Verifies hub failures during subscription creation release the bounded subscription slot.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SubscribeHubCreationFailureReleasesSubscriptionCapacity()
    {
        var state = new ThrowOnceSubscribeState();
        var hub = new RecordingHub { SubscribeHandler = (_, _, _) => state.Subscribe() };
        await using var adapter = new LoopbackTransportAdapter(CreateOptions(hub) with { MaximumConcurrentSubscriptions = 1 });
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);

        _ = Assert.ThrowsExactly<InvalidOperationException>(() => session.SubscribeAsync(CreateSubscribeRequest(), CancellationToken.None).GetAsyncEnumerator());
        var replacement = session.SubscribeAsync(CreateSubscribeRequest(), CancellationToken.None).GetAsyncEnumerator();
        await Assert.That(await replacement.MoveNextAsync()).IsFalse();
        await replacement.DisposeAsync();
    }

    /// <summary>Throws on the first subscription and returns an empty sequence afterwards.</summary>
    private sealed class ThrowOnceSubscribeState
    {
        /// <summary>Whether the next subscription call should throw.</summary>
        private bool _shouldThrow = true;

        /// <summary>Returns the subscribe sequence for a hub call.</summary>
        /// <returns>The remote batch sequence.</returns>
        /// <exception cref="InvalidOperationException">The first subscription fails.</exception>
        public IAsyncEnumerable<RemoteEventBatch> Subscribe()
        {
            if (!_shouldThrow)
            {
                return YieldBatches();
            }

            _shouldThrow = false;
            throw new InvalidOperationException("subscribe failed");
        }
    }

    /// <summary>Provides a non-null session placeholder for synchronous callback capture tests.</summary>
    private sealed class PlaceholderTransportSession : IRemoteTransportSession
    {
        /// <inheritdoc/>
        public NegotiatedCapabilities NegotiatedCapabilities => CreateCapabilities();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<RemoteSyncResult> PushAsync(SyncBatch batch, CancellationToken cancellationToken) =>
            ValueTask.FromException<RemoteSyncResult>(new InvalidOperationException("Placeholder session cannot push."));

        /// <inheritdoc/>
        public IAsyncEnumerable<RemoteEventBatch> SubscribeAsync(RemoteSubscribeRequest request, CancellationToken cancellationToken) =>
            throw new InvalidOperationException("Placeholder session cannot subscribe.");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask AcknowledgeAsync(ReceiveAcknowledgement acknowledgement, CancellationToken cancellationToken) =>
            ValueTask.FromException(new InvalidOperationException("Placeholder session cannot acknowledge."));

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    /// <summary>Provides an enumerator with a cancellation callback that throws during disposal.</summary>
    /// <param name="disposed">Signals that upstream disposal ran.</param>
    /// <param name="throwOnDispose">Whether upstream disposal should fault.</param>
    private sealed class CancellationCallbackFaultEnumerable(TaskCompletionSource disposed, bool throwOnDispose) : IAsyncEnumerable<RemoteEventBatch>
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IAsyncEnumerator<RemoteEventBatch> GetAsyncEnumerator(CancellationToken cancellationToken = default) =>
            new CancellationCallbackFaultEnumerator(disposed, throwOnDispose, cancellationToken);
    }

    /// <summary>Tracks cancellation callback and disposal behavior for a subscription enumerator.</summary>
    /// <param name="disposed">Signals that upstream disposal ran.</param>
    /// <param name="throwOnDispose">Whether upstream disposal should fault.</param>
    /// <param name="cancellationToken">The lease token supplied to the upstream enumerator.</param>
    private sealed class CancellationCallbackFaultEnumerator(
        TaskCompletionSource disposed,
        bool throwOnDispose,
        CancellationToken cancellationToken) : IAsyncEnumerator<RemoteEventBatch>
    {
        /// <summary>The throwing callback registration.</summary>
        private readonly CancellationTokenRegistration _registration = cancellationToken.UnsafeRegister(static _ => throw new InvalidOperationException("callback failed"), null);

        /// <inheritdoc/>
        public RemoteEventBatch Current => CreateReceiveBatch(CreateRemoteEvent());

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<bool> MoveNextAsync() => ValueTask.FromResult(false);

        /// <inheritdoc/>
        public ValueTask DisposeAsync()
        {
            _registration.Dispose();
            _ = disposed.TrySetResult();
            return throwOnDispose ? ValueTask.FromException(new InvalidOperationException("dispose failed")) : ValueTask.CompletedTask;
        }
    }
}
