// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests for the Multicast operator.</summary>
public partial class CombiningOperatorTests
{
    /// <summary>Number of items the option-driven publish sources emit.</summary>
    private const int PublishedItemCount = 3;

    /// <summary>Initial value replayed by the behavior-flavoured publish overloads.</summary>
    private const int PublishedInitialValue = 0;

    /// <summary>A publishing option outside the defined enum range, used to reach the unsupported-options guard.</summary>
    private const PublishingOption UnsupportedPublishingOption = (PublishingOption)(-1);

    /// <summary>Tests Multicast with ConnectAsync connects and emits.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMulticastConnectAsync_ThenEmitsToSubscribers()
    {
        const int EmittedItemCount = 3;
        var signal = Signal.Create<int>();
        var source = SignalAsync.Range(1, EmittedItemCount);
        var connectable = source.Multicast(signal);

        List<int> items = [];
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
        const int EmittedItemCount = 3;
        var signal = Signal.Create<int>();
        var source = SignalAsync.Range(1, EmittedItemCount);
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
        var source = SignalAsync.Return(Sentinel42);
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
        const int EmittedItemCount = 3;
        var source = SignalAsync.Range(1, EmittedItemCount);
        var connectable = source.Publish();

        List<int> items = [];
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
        const int EmittedItemCount = 3;
        var source = SignalAsync.Range(1, EmittedItemCount);
        var connectable = source.Publish(SignalCreationOptions.Default);

        List<int> items = [];
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
        const int EmittedItemCount = 3;
        var source = SignalAsync.Range(1, EmittedItemCount);
        var connectable = source.StatelessPublish();

        List<int> items = [];
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
        const int EmittedItemCount = 2;
        var source = SignalAsync.Range(1, EmittedItemCount);
        var connectable = source.Publish(0);

        List<int> items = [];
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
        const int EmittedItemCount = 2;
        var source = SignalAsync.Range(1, EmittedItemCount);
        var connectable = source.Publish(0, BehaviorSignalCreationOptions.Default);

        List<int> items = [];
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
        const int EmittedItemCount = 2;
        var source = SignalAsync.Range(1, EmittedItemCount);
        var connectable = source.StatelessPublish(0);

        List<int> items = [];
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
        const int EmittedItemCount = 3;
        var source = SignalAsync.Range(1, EmittedItemCount);
        var connectable = source.ReplayLatestPublish();

        List<int> items = [];
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
        const int EmittedItemCount = 3;
        var source = SignalAsync.Range(1, EmittedItemCount);
        var connectable = source.ReplayLatestPublish(ReplayLatestSignalCreationOptions.Default);

        List<int> items = [];
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
        const int EmittedItemCount = 3;
        var source = SignalAsync.Range(1, EmittedItemCount);
        var connectable = source.StatelessReplayLatestPublish();

        List<int> items = [];
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

        SignalAsyncExtensions.BlendEnumerableSignal<int>.BlendSequenceCoordinator.RoutePostDisposalException(
            Result.Success);
        SignalAsyncExtensions.BlendEnumerableSignal<int>.BlendSequenceCoordinator.RoutePostDisposalException(null);

        await Assert.That(unhandled).IsNull();
    }

    /// <summary>Verifies that disposing a RefCount observable with an active connection disposes the connection cleanly.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRefCountDisposedWithActiveConnection_ThenConnectionIsDisposed()
    {
        const int ItemWaitTimeoutSeconds = 5;
        var source = Signal.Create<int>();
        var connectable = source.Values.Publish();
        var refCounted = connectable.RefCount();

        // Subscribe to trigger the connection.
        List<int> items = [];
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
            TimeSpan.FromSeconds(ItemWaitTimeoutSeconds));

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

        using CancellationTokenSource cts = new();
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

    /// <summary>Verifies <c>Publish(options)</c> with concurrent, stateful options multicasts every source item.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPublishWithConcurrentStatefulOptions_ThenEmitsToSubscribers()
    {
        SignalCreationOptions options = new()
        {
            PublishingOption = PublishingOption.Concurrent,
            IsStateless = false
        };

        var items = await ConnectAndCollectAsync(SignalAsync.Range(1, PublishedItemCount).Publish(options));

        await Assert.That(items).IsCollectionEqualTo([SampleValue1, SampleValue2, SampleValue3]);
    }

    /// <summary>Verifies <c>Publish(options)</c> with serial, stateless options multicasts every source item.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPublishWithSerialStatelessOptions_ThenEmitsToSubscribers()
    {
        SignalCreationOptions options = new()
        {
            PublishingOption = PublishingOption.Serial,
            IsStateless = true
        };

        var items = await ConnectAndCollectAsync(SignalAsync.Range(1, PublishedItemCount).Publish(options));

        await Assert.That(items).IsCollectionEqualTo([SampleValue1, SampleValue2, SampleValue3]);
    }

    /// <summary>Verifies <c>Publish(options)</c> with concurrent, stateless options multicasts every source item.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPublishWithConcurrentStatelessOptions_ThenEmitsToSubscribers()
    {
        SignalCreationOptions options = new()
        {
            PublishingOption = PublishingOption.Concurrent,
            IsStateless = true
        };

        var items = await ConnectAndCollectAsync(SignalAsync.Range(1, PublishedItemCount).Publish(options));

        await Assert.That(items).IsCollectionEqualTo([SampleValue1, SampleValue2, SampleValue3]);
    }

    /// <summary>Verifies <c>Publish(options)</c> rejects a publishing option it has no signal for.</summary>
    [Test]
    public void WhenPublishWithUnsupportedOptions_ThenThrowsArgumentOutOfRange()
    {
        SignalCreationOptions options = new()
        {
            PublishingOption = UnsupportedPublishingOption,
            IsStateless = false
        };

        _ = Assert.Throws<ArgumentOutOfRangeException>(
            () => SignalAsync.Range(1, PublishedItemCount).Publish(options));
    }

    /// <summary>Verifies <c>Publish(initialValue, options)</c> with concurrent, stateful options replays the initial value.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPublishWithInitialValueAndConcurrentStatefulOptions_ThenReplaysInitialValue()
    {
        BehaviorSignalCreationOptions options = new()
        {
            PublishingOption = PublishingOption.Concurrent,
            IsStateless = false
        };

        var items = await ConnectAndCollectAsync(
            SignalAsync.Range(1, PublishedItemCount).Publish(PublishedInitialValue, options));

        await Assert.That(items).Contains(PublishedInitialValue);
        await Assert.That(items).Contains(SampleValue3);
    }

    /// <summary>Verifies <c>Publish(initialValue, options)</c> with serial, stateless options replays the initial value.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPublishWithInitialValueAndSerialStatelessOptions_ThenReplaysInitialValue()
    {
        BehaviorSignalCreationOptions options = new()
        {
            PublishingOption = PublishingOption.Serial,
            IsStateless = true
        };

        var items = await ConnectAndCollectAsync(
            SignalAsync.Range(1, PublishedItemCount).Publish(PublishedInitialValue, options));

        await Assert.That(items).Contains(PublishedInitialValue);
        await Assert.That(items).Contains(SampleValue3);
    }

    /// <summary>Verifies <c>Publish(initialValue, options)</c> with concurrent, stateless options replays the initial value.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPublishWithInitialValueAndConcurrentStatelessOptions_ThenReplaysInitialValue()
    {
        BehaviorSignalCreationOptions options = new()
        {
            PublishingOption = PublishingOption.Concurrent,
            IsStateless = true
        };

        var items = await ConnectAndCollectAsync(
            SignalAsync.Range(1, PublishedItemCount).Publish(PublishedInitialValue, options));

        await Assert.That(items).Contains(PublishedInitialValue);
        await Assert.That(items).Contains(SampleValue3);
    }

    /// <summary>Verifies <c>Publish(initialValue, options)</c> rejects a publishing option it has no signal for.</summary>
    [Test]
    public void WhenPublishWithInitialValueAndUnsupportedOptions_ThenThrowsArgumentOutOfRange()
    {
        BehaviorSignalCreationOptions options = new()
        {
            PublishingOption = UnsupportedPublishingOption,
            IsStateless = false
        };

        _ = Assert.Throws<ArgumentOutOfRangeException>(
            () => SignalAsync.Range(1, PublishedItemCount).Publish(PublishedInitialValue, options));
    }

    /// <summary>Verifies <c>ReplayLatestPublish(options)</c> with concurrent, stateful options multicasts every source item.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenReplayLatestPublishWithConcurrentStatefulOptions_ThenEmitsToSubscribers()
    {
        ReplayLatestSignalCreationOptions options = new()
        {
            PublishingOption = PublishingOption.Concurrent,
            IsStateless = false
        };

        var items = await ConnectAndCollectAsync(
            SignalAsync.Range(1, PublishedItemCount).ReplayLatestPublish(options));

        await Assert.That(items).IsCollectionEqualTo([SampleValue1, SampleValue2, SampleValue3]);
    }

    /// <summary>Verifies <c>ReplayLatestPublish(options)</c> with serial, stateless options multicasts every source item.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenReplayLatestPublishWithSerialStatelessOptions_ThenEmitsToSubscribers()
    {
        ReplayLatestSignalCreationOptions options = new()
        {
            PublishingOption = PublishingOption.Serial,
            IsStateless = true
        };

        var items = await ConnectAndCollectAsync(
            SignalAsync.Range(1, PublishedItemCount).ReplayLatestPublish(options));

        await Assert.That(items).IsCollectionEqualTo([SampleValue1, SampleValue2, SampleValue3]);
    }

    /// <summary>Verifies <c>ReplayLatestPublish(options)</c> with concurrent, stateless options multicasts every source item.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenReplayLatestPublishWithConcurrentStatelessOptions_ThenEmitsToSubscribers()
    {
        ReplayLatestSignalCreationOptions options = new()
        {
            PublishingOption = PublishingOption.Concurrent,
            IsStateless = true
        };

        var items = await ConnectAndCollectAsync(
            SignalAsync.Range(1, PublishedItemCount).ReplayLatestPublish(options));

        await Assert.That(items).IsCollectionEqualTo([SampleValue1, SampleValue2, SampleValue3]);
    }

    /// <summary>Verifies <c>ReplayLatestPublish(options)</c> rejects a publishing option it has no signal for.</summary>
    [Test]
    public void WhenReplayLatestPublishWithUnsupportedOptions_ThenThrowsArgumentOutOfRange()
    {
        ReplayLatestSignalCreationOptions options = new()
        {
            PublishingOption = UnsupportedPublishingOption,
            IsStateless = true
        };

        _ = Assert.Throws<ArgumentOutOfRangeException>(
            () => SignalAsync.Range(1, PublishedItemCount).ReplayLatestPublish(options));
    }

    /// <summary>Subscribes to a connectable sequence, connects it, and returns everything the subscriber saw.</summary>
    /// <param name="connectable">The connectable sequence under test.</param>
    /// <returns>The items the subscriber received once the connection was established.</returns>
    private static async Task<List<int>> ConnectAndCollectAsync(ConnectableSignalAsync<int> connectable)
    {
        List<int> items = [];
        await using var sub = await connectable.SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                return default;
            },
            null);

        await using var conn = await connectable.ConnectAsync(CancellationToken.None);

        return items;
    }
}
