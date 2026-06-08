// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests for the Multicast operator.</summary>
public partial class CombiningOperatorTests
{
    /// <summary>Tests Multicast with ConnectAsync connects and emits.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMulticastConnectAsync_ThenEmitsToSubscribers()
    {
        var signal = Signal.Create<int>();
        var source = SignalAsync.Range(1, 3);
        var connectable = source.Multicast(signal);

        var items = new List<int>();
        await using var sub = await connectable.SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                return default;
            },
            null);

        await using var conn = await connectable.ConnectAsync(CancellationToken.None);

        await Assert.That(items).IsCollectionEqualTo([SampleValue1, SampleValue2, SampleValue3]);
    }

    /// <summary>
    /// Verifies that disposing the disconnect handle twice is safe because the second
    /// call hits the null check (connection is null) and returns early.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMulticastDisconnectHandleDisposedTwice_ThenSecondCallIsNoop()
    {
        var signal = Signal.Create<int>();
        var source = SignalAsync.Range(1, 3);
        var connectable = source.Multicast(signal);

        var disconnectHandle = await connectable.ConnectAsync(CancellationToken.None);

        // First dispose clears the connection
        await disconnectHandle.DisposeAsync();

        // Second dispose hits the null check, returning early
        await disconnectHandle.DisposeAsync();
    }

    /// <summary>
    /// Verifies that disposing a Multicast connect handle twice leaves the connectable
    /// in a state where a fresh connection can be established, confirming the null-check
    /// early-return path in the dispose closure does not corrupt internal state.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMulticastConnectHandleDisposedTwice_ThenCanReconnectSuccessfully()
    {
        var signal = Signal.Create<int>();
        var source = SignalAsync.Return(42);
        var connectable = source.Multicast(signal);

        var handle = await connectable.ConnectAsync(CancellationToken.None);

        // First dispose tears down the connection and nulls the local capture.
        await handle.DisposeAsync();

        // Second dispose enters the closure, sees connection is null, and returns early (line 60).
        await handle.DisposeAsync();

        // After the double-dispose the connectable must accept a new connection.
        await using var sub = await connectable.SubscribeAsync(static (_, _) =>
        {
            // Signal is already completed from first connect, so no items arrive.
            return ValueTask.CompletedTask;
        });

        // A new ConnectAsync succeeds, proving internal state was not corrupted.
        await using var newHandle = await connectable.ConnectAsync(CancellationToken.None);
        await Assert.That(newHandle).IsNotNull();
    }

    /// <summary>
    /// Verifies that Publish creates a connectable observable that emits all source items
    /// to subscribers after ConnectAsync is called.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPublish_ThenEmitsToSubscribersAfterConnect()
    {
        var source = SignalAsync.Range(1, 3);
        var connectable = source.Publish();

        var items = new List<int>();
        await using var sub = await connectable.SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                return default;
            },
            null);

        await using var conn = await connectable.ConnectAsync(CancellationToken.None);

        await Assert.That(items).IsCollectionEqualTo([SampleValue1, SampleValue2, SampleValue3]);
    }

    /// <summary>
    /// Verifies that Publish with SignalCreationOptions creates a connectable observable
    /// that emits all source items to subscribers.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPublishWithOptions_ThenEmitsToSubscribers()
    {
        var source = SignalAsync.Range(1, 3);
        var connectable = source.Publish(SignalCreationOptions.Default);

        var items = new List<int>();
        await using var sub = await connectable.SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                return default;
            },
            null);

        await using var conn = await connectable.ConnectAsync(CancellationToken.None);

        await Assert.That(items).IsCollectionEqualTo([SampleValue1, SampleValue2, SampleValue3]);
    }

    /// <summary>
    /// Verifies that StatelessPublish creates a connectable observable that emits all
    /// source items without retaining state between subscriptions.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenStatelessPublish_ThenEmitsToSubscribers()
    {
        var source = SignalAsync.Range(1, 3);
        var connectable = source.StatelessPublish();

        var items = new List<int>();
        await using var sub = await connectable.SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                return default;
            },
            null);

        await using var conn = await connectable.ConnectAsync(CancellationToken.None);

        await Assert.That(items).IsCollectionEqualTo([SampleValue1, SampleValue2, SampleValue3]);
    }

    /// <summary>
    /// Verifies that Publish with an initial value creates a connectable observable that
    /// replays the initial value to new subscribers and then emits source items.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPublishWithInitialValue_ThenSubscriberReceivesInitialValueAndSourceItems()
    {
        var source = SignalAsync.Range(1, 2);
        var connectable = source.Publish(0);

        var items = new List<int>();
        await using var sub = await connectable.SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                return default;
            },
            null);

        await using var conn = await connectable.ConnectAsync(CancellationToken.None);

        await Assert.That(items).Contains(0);
        await Assert.That(items).Contains(1);
        await Assert.That(items).Contains(SampleValue2);
    }

    /// <summary>
    /// Verifies that Publish with an initial value and BehaviorSignalCreationOptions creates
    /// a connectable observable that replays the initial value and source items.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPublishWithInitialValueAndOptions_ThenSubscriberReceivesInitialValueAndSourceItems()
    {
        var source = SignalAsync.Range(1, 2);
        var connectable = source.Publish(0, BehaviorSignalCreationOptions.Default);

        var items = new List<int>();
        await using var sub = await connectable.SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                return default;
            },
            null);

        await using var conn = await connectable.ConnectAsync(CancellationToken.None);

        await Assert.That(items).Contains(0);
        await Assert.That(items).Contains(1);
        await Assert.That(items).Contains(SampleValue2);
    }

    /// <summary>
    /// Verifies that StatelessPublish with an initial value creates a connectable observable
    /// that replays the initial value and does not retain state between connections.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenStatelessPublishWithInitialValue_ThenSubscriberReceivesInitialValueAndSourceItems()
    {
        var source = SignalAsync.Range(1, 2);
        var connectable = source.StatelessPublish(0);

        var items = new List<int>();
        await using var sub = await connectable.SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                return default;
            },
            null);

        await using var conn = await connectable.ConnectAsync(CancellationToken.None);

        await Assert.That(items).Contains(0);
        await Assert.That(items).Contains(1);
        await Assert.That(items).Contains(SampleValue2);
    }

    /// <summary>
    /// Verifies that ReplayLatestPublish creates a connectable observable that replays
    /// the most recent item to new subscribers.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenReplayLatestPublish_ThenEmitsToSubscribers()
    {
        var source = SignalAsync.Range(1, 3);
        var connectable = source.ReplayLatestPublish();

        var items = new List<int>();
        await using var sub = await connectable.SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                return default;
            },
            null);

        await using var conn = await connectable.ConnectAsync(CancellationToken.None);

        await Assert.That(items).IsCollectionEqualTo([SampleValue1, SampleValue2, SampleValue3]);
    }

    /// <summary>
    /// Verifies that ReplayLatestPublish with ReplayLatestSignalCreationOptions creates
    /// a connectable observable that emits source items to subscribers.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenReplayLatestPublishWithOptions_ThenEmitsToSubscribers()
    {
        var source = SignalAsync.Range(1, 3);
        var connectable = source.ReplayLatestPublish(ReplayLatestSignalCreationOptions.Default);

        var items = new List<int>();
        await using var sub = await connectable.SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                return default;
            },
            null);

        await using var conn = await connectable.ConnectAsync(CancellationToken.None);

        await Assert.That(items).IsCollectionEqualTo([SampleValue1, SampleValue2, SampleValue3]);
    }

    /// <summary>
    /// Verifies that StatelessReplayLatestPublish creates a connectable observable that
    /// replays the latest item and does not retain state between connections.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenStatelessReplayLatestPublish_ThenEmitsToSubscribers()
    {
        var source = SignalAsync.Range(1, 3);
        var connectable = source.StatelessReplayLatestPublish();

        var items = new List<int>();
        await using var sub = await connectable.SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                return default;
            },
            null);

        await using var conn = await connectable.ConnectAsync(CancellationToken.None);

        await Assert.That(items).IsCollectionEqualTo([SampleValue1, SampleValue2, SampleValue3]);
    }

    /// <summary>Verifies RoutePostDisposalException does nothing when result has no exception.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRoutePostDisposalExceptionWithSuccess_ThenNoExceptionRouted()
    {
        Exception? unhandled = null;
        UnhandledExceptionHandler.Register(ex => unhandled = ex);

        SignalAsyncExtensions.MergeEnumerableSignal<int>.MergeSequenceCoordinator.RoutePostDisposalException(
            Result.Success);
        SignalAsyncExtensions.MergeEnumerableSignal<int>.MergeSequenceCoordinator.RoutePostDisposalException(null);

        await Assert.That(unhandled).IsNull();
    }

    /// <summary>Verifies that disposing a RefCount observable with an active connection disposes the connection cleanly.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRefCountDisposedWithActiveConnection_ThenConnectionIsDisposed()
    {
        var source = Signal.Create<int>();
        var connectable = source.Values.Publish();
        var refCounted = connectable.RefCount();

        // Subscribe to trigger the connection.
        var items = new List<int>();
        await using var sub = await refCounted.SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                return default;
            },
            null);

        await source.OnNextAsync(Sentinel42, CancellationToken.None);

        await AsyncTestHelpers.WaitForConditionAsync(
            () => items.Count == 1,
            TimeSpan.FromSeconds(5));

        // Dispose the RefCountSignal via its IDisposable implementation.
        ((IDisposable)(object)refCounted).Dispose();

        await Assert.That(items).Contains(Sentinel42);

        await sub.DisposeAsync();
        await source.DisposeAsync();
    }

    /// <summary>Verifies that disposing a RefCount observable without any subscribers does not throw.</summary>
    [Test]
    public void WhenRefCountDisposedWithNoSubscribers_ThenDoesNotThrow()
    {
        var source = SignalAsync.Return(1);
        var connectable = source.Publish();
        var refCounted = connectable.RefCount();

        // Dispose without ever subscribing — _connection is null.
        ((IDisposable)(object)refCounted).Dispose();
    }

    /// <summary>Verifies that calling Dispose twice on a RefCount observable is idempotent.</summary>
    [Test]
    public void WhenRefCountDisposedTwice_ThenSecondDisposeIsNoop()
    {
        var source = SignalAsync.Return(1);
        var connectable = source.Publish();
        var refCounted = connectable.RefCount();

        var disposable = (IDisposable)(object)refCounted;
        disposable.Dispose();
        disposable.Dispose();
    }

    /// <summary>Verifies that calling <c>ConnectAsync</c> with a caller-supplied cancellation token takes the linked-CTS slow path.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMulticastConnectAsyncWithCustomToken_ThenLinkedCtsPathTaken()
    {
        var signal = Signal.Create<int>();
        var connectable = SignalAsync.Return(1).Multicast(signal);

        using var cts = new CancellationTokenSource();
        await using var connection = await connectable.ConnectAsync(cts.Token);

        await Assert.That(connection).IsNotNull();
    }

    /// <summary>Verifies that disposing the connection handle twice is idempotent — the second
    /// call hits the <c>connection is null</c> guard inside the dispose lambda.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMulticastConnectionDisposedTwice_ThenSecondIsNoOp()
    {
        var signal = Signal.Create<int>();
        var connectable = SignalAsync.Return(1).Multicast(signal);

        var connection = await connectable.ConnectAsync(CancellationToken.None);

        await connection.DisposeAsync();
        await connection.DisposeAsync();

        await Assert.That(connection).IsNotNull();
    }
}
