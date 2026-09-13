// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Subscription lifetime tests for <see cref="ServerStreamHub"/>.</summary>
public sealed partial class ServerStreamHubTests
{
    /// <summary>Verifies that disposing a subscription enumerator releases capacity.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task SubscribeStreamAsyncReleasesCapacityWhenEnumeratorIsDisposed()
    {
        await using var hub = ServerStreamHub.CreateInMemory(
            Options(new AllowPolicy(Tenant), new RecordingDomainHandler()) with
            {
                MaximumActiveSubscriptions = 1,
            });
        _ = await hub.ApplyOperationsAsync(Batch(Operation(1, PayloadA)), new(Tenant, Client), CancellationToken.None);
        var first = hub.SubscribeStreamAsync(new(Stream, SubscriptionId.New(), null, StartPosition.FromSequence(0)), new(Tenant, Client), CancellationToken.None);
        await using var firstEnumerator = first.GetAsyncEnumerator(CancellationToken.None);
        var firstMove = await firstEnumerator.MoveNextAsync();
        await Assert.That(firstMove).IsTrue();

        var second = hub.SubscribeStreamAsync(new(Stream, SubscriptionId.New(), null, StartPosition.FromSequence(0)), new(Tenant, Client), CancellationToken.None);
        await using var blockedEnumerator = second.GetAsyncEnumerator(CancellationToken.None);

        _ = await Assert.ThrowsExactlyAsync<QueueCapacityExceededException>(() => blockedEnumerator.MoveNextAsync().AsTask());
        await blockedEnumerator.DisposeAsync();
        var stillBlocked = hub.SubscribeStreamAsync(new(Stream, SubscriptionId.New(), null, StartPosition.FromSequence(0)), new(Tenant, Client), CancellationToken.None);
        await using (var stillBlockedEnumerator = stillBlocked.GetAsyncEnumerator(CancellationToken.None))
        {
            _ = await Assert.ThrowsExactlyAsync<QueueCapacityExceededException>(() => stillBlockedEnumerator.MoveNextAsync().AsTask());
        }

        await firstEnumerator.DisposeAsync();
        var third = hub.SubscribeStreamAsync(new(Stream, SubscriptionId.New(), null, StartPosition.FromSequence(0)), new(Tenant, Client), CancellationToken.None);
        await using var thirdEnumerator = third.GetAsyncEnumerator(CancellationToken.None);
        var thirdMove = await thirdEnumerator.MoveNextAsync();
        await Assert.That(thirdMove).IsTrue();
    }

    /// <summary>Verifies that disposing before the first move does not reserve subscription capacity.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task SubscribeStreamAsyncDisposeBeforeMoveDoesNotReserveCapacity()
    {
        await using var hub = ServerStreamHub.CreateInMemory(
            Options(new AllowPolicy(Tenant), new RecordingDomainHandler()) with
            {
                MaximumActiveSubscriptions = 1,
            });
        _ = await hub.ApplyOperationsAsync(Batch(Operation(1, PayloadA)), new(Tenant, Client), CancellationToken.None);
        var first = hub.SubscribeStreamAsync(new(Stream, SubscriptionId.New(), null, StartPosition.FromSequence(0)), new(Tenant, Client), CancellationToken.None);
        var unusedEnumerator = first.GetAsyncEnumerator(CancellationToken.None);
        await unusedEnumerator.DisposeAsync();

        var second = hub.SubscribeStreamAsync(new(Stream, SubscriptionId.New(), null, StartPosition.FromSequence(0)), new(Tenant, Client), CancellationToken.None);
        await using var secondEnumerator = second.GetAsyncEnumerator(CancellationToken.None);
        var hasPage = await secondEnumerator.MoveNextAsync();

        await Assert.That(hasPage).IsTrue();
    }

    /// <summary>Verifies a disposed enumerator reports completion on later moves.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task SubscribeStreamAsyncMoveNextAfterEnumeratorDisposeReturnsFalse()
    {
        await using var hub = ServerStreamHub.CreateInMemory(Options(new AllowPolicy(Tenant), new RecordingDomainHandler()));
        var enumerable = hub.SubscribeStreamAsync(
            new(Stream, SubscriptionId.New(), null, StartPosition.FromSequence(0)),
            new(Tenant, Client),
            CancellationToken.None);
        var enumerator = enumerable.GetAsyncEnumerator(CancellationToken.None);

        await enumerator.DisposeAsync();
        var hasPage = await enumerator.MoveNextAsync();

        await Assert.That(hasPage).IsFalse();
    }

    /// <summary>Verifies enumerator disposal cancels and drains an active empty-poll move.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task SubscribeStreamAsyncDisposeEnumeratorWaitsForPendingMoveNext()
    {
        var timeProvider = new SignalingFixedTimeProvider(Start);
        await using var hub = ServerStreamHub.CreateInMemory(
            Options(new AllowPolicy(Tenant), new RecordingDomainHandler()) with
            {
                EmptyPollDelay = TimeSpan.FromMinutes(LongPollMinutes),
                MaximumActiveSubscriptions = 1,
                TimeProvider = timeProvider,
            });
        var first = hub.SubscribeStreamAsync(new(Stream, SubscriptionId.New(), null, StartPosition.FromSequence(0)), new(Tenant, Client), CancellationToken.None);
        var firstEnumerator = first.GetAsyncEnumerator(CancellationToken.None);
        var pendingMove = firstEnumerator.MoveNextAsync().AsTask();
        await timeProvider.WaitUntilTimerCreatedAsync();

        var dispose = firstEnumerator.DisposeAsync().AsTask();
        await Assert.That(dispose.IsCompleted).IsFalse();
        var firstResult = await pendingMove;
        await dispose;

        await Assert.That(firstResult).IsFalse();
        _ = await hub.ApplyOperationsAsync(Batch(Operation(1, PayloadA)), new(Tenant, Client), CancellationToken.None);
        var second = hub.SubscribeStreamAsync(new(Stream, SubscriptionId.New(), null, StartPosition.FromSequence(0)), new(Tenant, Client), CancellationToken.None);
        await using var secondEnumerator = second.GetAsyncEnumerator(CancellationToken.None);
        var secondResult = await secondEnumerator.MoveNextAsync();
        await Assert.That(secondResult).IsTrue();
    }

    /// <summary>Verifies enumerator disposal before a page arrives does not publish current.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task SubscribeStreamAsyncDisposeEnumeratorBeforePageSuppressesCurrent()
    {
        var policy = new BlockingSubscribePolicy();
        await using var hub = ServerStreamHub.CreateInMemory(Options(policy, new RecordingDomainHandler()));
        _ = await hub.ApplyOperationsAsync(Batch(Operation(1, PayloadA)), new(Tenant, Client), CancellationToken.None);
        var enumerable = hub.SubscribeStreamAsync(
            new(Stream, SubscriptionId.New(), null, StartPosition.FromSequence(0)),
            new(Tenant, Client),
            CancellationToken.None);
        var enumerator = enumerable.GetAsyncEnumerator(CancellationToken.None);
        var pendingMove = enumerator.MoveNextAsync().AsTask();
        await policy.WaitUntilStartedAsync();

        var dispose = enumerator.DisposeAsync().AsTask();
        await policy.WaitUntilCanceledAsync();
        policy.Release();
        var hasPage = await pendingMove;
        await dispose;

        await Assert.That(hasPage).IsFalse();
        await Assert.That(enumerator.Current.Events).IsEmpty();
    }

    /// <summary>Verifies caller cancellation during a blocked page read still surfaces cancellation.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task SubscribeStreamAsyncObservesCallerCancellationDuringBlockedSubscribeAuthorization()
    {
        var policy = new BlockingSubscribePolicy();
        await using var hub = ServerStreamHub.CreateInMemory(Options(policy, new RecordingDomainHandler()));
        using var cancellation = new CancellationTokenSource();
        _ = await hub.ApplyOperationsAsync(Batch(Operation(1, PayloadA)), new(Tenant, Client), CancellationToken.None);
        var enumerable = hub.SubscribeStreamAsync(
            new(Stream, SubscriptionId.New(), null, StartPosition.FromSequence(0)),
            new(Tenant, Client),
            cancellation.Token);
        var enumerator = enumerable.GetAsyncEnumerator(cancellation.Token);
        var pendingMove = enumerator.MoveNextAsync().AsTask();
        await policy.WaitUntilStartedAsync();

        await cancellation.CancelAsync();
        await policy.WaitUntilCanceledAsync();
        policy.Release();
        _ = await Assert.ThrowsAsync<OperationCanceledException>(() => pendingMove);
        await enumerator.DisposeAsync();
    }

    /// <summary>Verifies hub disposal during a blocked page read completes the move without yielding current.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task SubscribeStreamAsyncObservesHubDisposeDuringBlockedSubscribeAuthorization()
    {
        var policy = new BlockingSubscribePolicy();
        var hub = ServerStreamHub.CreateInMemory(Options(policy, new RecordingDomainHandler()));
        _ = await hub.ApplyOperationsAsync(Batch(Operation(1, PayloadA)), new(Tenant, Client), CancellationToken.None);
        var enumerable = hub.SubscribeStreamAsync(
            new(Stream, SubscriptionId.New(), null, StartPosition.FromSequence(0)),
            new(Tenant, Client),
            CancellationToken.None);
        var enumerator = enumerable.GetAsyncEnumerator(CancellationToken.None);
        var pendingMove = enumerator.MoveNextAsync().AsTask();
        await policy.WaitUntilStartedAsync();

        var dispose = hub.DisposeAsync().AsTask();
        await policy.WaitUntilCanceledAsync();
        policy.Release();
        var hasPage = await pendingMove;
        await dispose;

        await Assert.That(hasPage).IsFalse();
        await Assert.That(enumerator.Current.Events).IsEmpty();
    }

    /// <summary>Verifies concurrent MoveNext calls are rejected without reserving subscription capacity twice.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task SubscribeStreamAsyncRejectsConcurrentMoveNext()
    {
        var policy = new BlockingSubscribePolicy();
        await using var hub = ServerStreamHub.CreateInMemory(
            Options(policy, new RecordingDomainHandler()) with { MaximumActiveSubscriptions = 1 });
        _ = await hub.ApplyOperationsAsync(Batch(Operation(1, PayloadA)), new(Tenant, Client), CancellationToken.None);
        var enumerable = hub.SubscribeStreamAsync(
            new(Stream, SubscriptionId.New(), null, StartPosition.FromSequence(0)),
            new(Tenant, Client),
            CancellationToken.None);
        await using var enumerator = enumerable.GetAsyncEnumerator(CancellationToken.None);
        var firstMove = enumerator.MoveNextAsync().AsTask();
        await policy.WaitUntilStartedAsync();

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => enumerator.MoveNextAsync().AsTask());
        policy.Release();
        var hasPage = await firstMove;

        await Assert.That(hasPage).IsTrue();
        await Assert.That(enumerator.Current.Events).Count().IsEqualTo(SingleCount);
    }

    /// <summary>Verifies enumerator disposal shares cancellation and cleanup completion across callers.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task SubscribeStreamAsyncDisposeEnumeratorWaitsForCancellationCallbacks()
    {
        var policy = new ThrowingBlockingSubscribePolicy();
        await using var hub = ServerStreamHub.CreateInMemory(
            Options(policy, new RecordingDomainHandler()) with { MaximumActiveSubscriptions = 1 });
        _ = await hub.ApplyOperationsAsync(Batch(Operation(1, PayloadA)), new(Tenant, Client), CancellationToken.None);
        var enumerable = hub.SubscribeStreamAsync(
            new(Stream, SubscriptionId.New(), null, StartPosition.FromSequence(0)),
            new(Tenant, Client),
            CancellationToken.None);
        var enumerator = enumerable.GetAsyncEnumerator(CancellationToken.None);
        var pendingMove = enumerator.MoveNextAsync().AsTask();
        await policy.WaitUntilStartedAsync();

        var firstDispose = enumerator.DisposeAsync().AsTask();
        var secondDispose = enumerator.DisposeAsync().AsTask();
        await policy.WaitUntilCanceledAsync();
        policy.ReleaseAuthorization();
        await Task.Yield();
        await Assert.That(firstDispose.IsCompleted).IsFalse();
        await Assert.That(secondDispose.IsCompleted).IsFalse();

        policy.ReleaseCallback();
        _ = await Assert.ThrowsAsync<Exception>(() => firstDispose);
        _ = await Assert.ThrowsAsync<Exception>(() => secondDispose);
        _ = await Assert.ThrowsAsync<Exception>(() => pendingMove);
        var secondEnumerable = hub.SubscribeStreamAsync(
            new(Stream, SubscriptionId.New(), null, StartPosition.FromSequence(0)),
            new(Tenant, Client),
            CancellationToken.None);
        await using var secondEnumerator = secondEnumerable.GetAsyncEnumerator(CancellationToken.None);
        var hasPage = await secondEnumerator.MoveNextAsync();

        await Assert.That(hasPage).IsTrue();
    }

    /// <summary>Verifies a subscription enumerable can create an independent second enumerator.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task SubscribeStreamAsyncCreatesIndependentSecondEnumerator()
    {
        await using var hub = ServerStreamHub.CreateInMemory(Options(new AllowPolicy(Tenant), new RecordingDomainHandler()));
        var enumerable = hub.SubscribeStreamAsync(
            new(Stream, SubscriptionId.New(), null, StartPosition.FromSequence(0)),
            new(Tenant, Client),
            CancellationToken.None);
        var first = enumerable.GetAsyncEnumerator(CancellationToken.None);
        var second = enumerable.GetAsyncEnumerator(CancellationToken.None);

        await first.DisposeAsync();
        await second.DisposeAsync();

        await Assert.That(ReferenceEquals(first, second)).IsFalse();
    }

    /// <summary>Verifies subscription enumeration combines different caller cancellation tokens.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task SubscribeStreamAsyncCombinesDifferentEnumeratorCancellationTokens()
    {
        await using var hub = ServerStreamHub.CreateInMemory(Options(new AllowPolicy(Tenant), new RecordingDomainHandler()));
        using var enumerableCancellation = new CancellationTokenSource();
        using var enumeratorCancellation = new CancellationTokenSource();
        var enumerable = hub.SubscribeStreamAsync(
            new(Stream, SubscriptionId.New(), null, StartPosition.FromSequence(0)),
            new(Tenant, Client),
            enumerableCancellation.Token);
        await using var enumerator = enumerable.GetAsyncEnumerator(enumeratorCancellation.Token);

        await enumeratorCancellation.CancelAsync();
        var exception = await Assert.ThrowsAsync<OperationCanceledException>(() => enumerator.MoveNextAsync().AsTask());

        await Assert.That(exception is OperationCanceledException).IsTrue();
    }

    /// <summary>Verifies subscription enumeration can be requested from a different thread.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task SubscribeStreamAsyncCreatesEnumeratorFromDifferentThread()
    {
        await using var hub = ServerStreamHub.CreateInMemory(Options(new AllowPolicy(Tenant), new RecordingDomainHandler()));
        var enumerable = hub.SubscribeStreamAsync(
            new(Stream, SubscriptionId.New(), null, StartPosition.FromSequence(0)),
            new(Tenant, Client),
            CancellationToken.None);

        var enumerator = await Task.Run(() => enumerable.GetAsyncEnumerator(CancellationToken.None));
        await enumerator.DisposeAsync();

        await Assert.That(enumerator).IsNotNull();
    }

    /// <summary>Verifies that hub disposal does not wait for a consumer paused after yield.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task DisposeAsyncDoesNotWaitForSubscriptionPausedAfterYield()
    {
        var hub = ServerStreamHub.CreateInMemory(Options(new AllowPolicy(Tenant), new RecordingDomainHandler()));
        _ = await hub.ApplyOperationsAsync(Batch(Operation(1, PayloadA)), new(Tenant, Client), CancellationToken.None);
        var enumerable = hub.SubscribeStreamAsync(
            new(Stream, SubscriptionId.New(), null, StartPosition.FromSequence(0)),
            new(Tenant, Client),
            CancellationToken.None);
        await using var enumerator = enumerable.GetAsyncEnumerator(CancellationToken.None);
        var hasPage = await enumerator.MoveNextAsync();
        await Assert.That(hasPage).IsTrue();

        var disposeTask = hub.DisposeAsync().AsTask();
        var completed = await Task.WhenAny(disposeTask, Task.Delay(TimeSpan.FromSeconds(LongPollMinutes)));

        await Assert.That(completed).IsEqualTo(disposeTask);
        await disposeTask;
        _ = await Assert.ThrowsExactlyAsync<ObjectDisposedException>(() => enumerator.MoveNextAsync().AsTask());
    }
}
