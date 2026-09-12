// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="BoundedAdmissionQueue{T}"/>.</summary>
public sealed partial class BoundedAdmissionQueueTests
{
    /// <summary>Defines a single byte item size.</summary>
    private const long OneByte = 1;

    /// <summary>Defines a two byte item size.</summary>
    private const long TwoBytes = 2;

    /// <summary>Defines two committed evictions.</summary>
    private const int TwoEvictions = 2;

    /// <summary>Defines a three byte item size.</summary>
    private const long ThreeBytes = 3;

    /// <summary>Defines a four byte item size.</summary>
    private const long FourBytes = 4;

    /// <summary>Defines a single item capacity.</summary>
    private const int OneItem = 1;

    /// <summary>Defines a two item capacity.</summary>
    private const int TwoItems = 2;

    /// <summary>Defines a four item capacity.</summary>
    private const int FourItems = 4;

    /// <summary>Defines an enum value outside the supported range.</summary>
    private const int UndefinedEnumValue = 42;

    /// <summary>Defines a single blocked producer limit.</summary>
    private const int OneBlockedProducer = 1;

    /// <summary>Defines a two blocked producer limit.</summary>
    private const int TwoBlockedProducers = 2;

    /// <summary>Defines the value used for durable queue items.</summary>
    private const string DurableValue = "durable";

    /// <summary>Defines the value used for control queue items.</summary>
    private const string ControlValue = "control";

    /// <summary>Defines the resident data value used by FIFO tests.</summary>
    private const string ResidentDataValue = "resident-data";

    /// <summary>Defines the first blocked data value used by FIFO tests.</summary>
    private const string FirstDataValue = "first-data";

    /// <summary>Defines the resident value used by cancellation commit tests.</summary>
    private const string ResidentValue = "resident";

    /// <summary>Defines the admitted value used by cancellation commit tests.</summary>
    private const string AdmittedValue = "admitted";

    /// <summary>Defines a short guard timeout for deterministic asynchronous tests.</summary>
    private static readonly TimeSpan GuardTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Verifies admitted items are dequeued in FIFO order while count and byte totals are tracked.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task EnqueueAsyncAdmitsWithinCountAndByteCapacity()
    {
        using var queue = new BoundedAdmissionQueue<string>(new BoundedAdmissionQueueOptions(TwoItems, ThreeBytes, OneBlockedProducer));

        var first = await queue.EnqueueAsync("a", OneByte, durable: true, control: false);
        var second = await queue.EnqueueAsync("b", TwoBytes, durable: true, control: false);

        await Assert.That(first.Kind).IsEqualTo(BoundedAdmissionResultKind.Admitted);
        await Assert.That(second.Kind).IsEqualTo(BoundedAdmissionResultKind.Admitted);
        await Assert.That(queue.Count).IsEqualTo(TwoItems);
        await Assert.That(queue.Bytes).IsEqualTo(ThreeBytes);
        await Assert.That(queue.TryDequeue(out var item)).IsTrue();
        await Assert.That(item.Value).IsEqualTo("a");
        await Assert.That(queue.Bytes).IsEqualTo(TwoBytes);
    }

    /// <summary>Verifies reject strategy reports overflow with a typed exception.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task EnqueueAsyncRejectsOverflowWithTypedException()
    {
        using var queue = new BoundedAdmissionQueue<string>(new BoundedAdmissionQueueOptions(OneItem, OneByte, OneBlockedProducer));

        await queue.EnqueueAsync("a", OneByte, durable: true, control: false);

        Func<Task> action = () => queue.EnqueueAsync("b", OneByte, durable: true, control: false);

        await Assert.That(action).ThrowsExactly<BoundedAdmissionRejectedException>();
    }

    /// <summary>Verifies non-positive byte sizes are rejected before admission.</summary>
    /// <param name="sizeBytes">The invalid byte size.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    [Arguments(0L)]
    [Arguments(-1L)]
    public async Task EnqueueAsyncRejectsNonPositiveByteSizes(long sizeBytes)
    {
        using var queue = new BoundedAdmissionQueue<string>(new BoundedAdmissionQueueOptions(OneItem, OneByte, OneBlockedProducer));

        Func<Task> action = () => queue.EnqueueAsync("a", sizeBytes, durable: true, control: false);

        await Assert.That(action).ThrowsExactly<ArgumentOutOfRangeException>();
    }

    /// <summary>Verifies undefined built-in strategies are rejected by the queue.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task EnqueueAsyncRejectsUndefinedStrategies()
    {
        using var queue = new BoundedAdmissionQueue<string>(new BoundedAdmissionQueueOptions(OneItem, OneByte, OneBlockedProducer));

        await queue.EnqueueAsync("a", OneByte, durable: true, control: false);

        Func<Task> action = () => queue.EnqueueAsync("b", OneByte, durable: true, control: false, strategy: (BufferStrategy)UndefinedEnumValue);

        await Assert.That(action).ThrowsExactly<BoundedAdmissionRejectedException>();
    }

    /// <summary>Verifies undefined strategies are rejected before a fast admission can commit.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task EnqueueAsyncRejectsUndefinedStrategiesBeforeFastAdmission()
    {
        using var queue = new BoundedAdmissionQueue<string>(new BoundedAdmissionQueueOptions(OneItem, OneByte, OneBlockedProducer));

        Func<Task> action = () => queue.EnqueueAsync("a", OneByte, durable: true, control: false, strategy: (BufferStrategy)UndefinedEnumValue);

        await Assert.That(action).ThrowsExactly<BoundedAdmissionRejectedException>();
        await Assert.That(queue.Count).IsEqualTo(0);
    }

    /// <summary>Verifies pre-cancelled blocking admissions do not commit through the fast path.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task EnqueueAsyncBlockObservesPreCancelledTokenBeforeFastAdmission()
    {
        using var queue = new BoundedAdmissionQueue<string>(new BoundedAdmissionQueueOptions(OneItem, OneByte, OneBlockedProducer));
        using var cancellation = new CancellationTokenSource();

        await cancellation.CancelAsync();

        Func<Task> action = () => queue.EnqueueAsync("a", OneByte, durable: true, control: false, strategy: BufferStrategy.Block, cancellationToken: cancellation.Token);

        await Assert.That(action).ThrowsExactly<OperationCanceledException>();
        await Assert.That(queue.Count).IsEqualTo(0);
    }

    /// <summary>Verifies invalid queue options are rejected.</summary>
    /// <param name="capacity">The item capacity.</param>
    /// <param name="capacityBytes">The byte capacity.</param>
    /// <param name="maximumBlockedProducers">The blocked producer limit.</param>
    /// <param name="reservedControlCapacity">The reserved control item capacity.</param>
    /// <param name="reservedControlBytes">The reserved control byte capacity.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    [Arguments(0, OneByte, OneBlockedProducer, 0, 0L)]
    [Arguments(OneItem, 0L, OneBlockedProducer, 0, 0L)]
    [Arguments(OneItem, OneByte, 0, 0, 0L)]
    [Arguments(OneItem, OneByte, OneBlockedProducer, -1, 0L)]
    [Arguments(OneItem, OneByte, OneBlockedProducer, 0, -1L)]
    [Arguments(OneItem, OneByte, OneBlockedProducer, OneItem, 0L)]
    [Arguments(OneItem, OneByte, OneBlockedProducer, 0, OneByte)]
    public async Task ConstructorRejectsInvalidQueueOptions(
        int capacity,
        long capacityBytes,
        int maximumBlockedProducers,
        int reservedControlCapacity,
        long reservedControlBytes)
    {
        Action action = () =>
        {
            using var queue = new BoundedAdmissionQueue<string>(
                new BoundedAdmissionQueueOptions(capacity, capacityBytes, maximumBlockedProducers, reservedControlCapacity, reservedControlBytes));
        };

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies the typed rejection exception exposes analyzer-required constructors.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task RejectedExceptionConstructorsCreateExpectedInstances()
    {
        var defaultException = new BoundedAdmissionRejectedException();
        var innerException = new InvalidOperationException("inner");
        var wrappedException = new BoundedAdmissionRejectedException("outer", innerException);

        await Assert.That(defaultException).IsNotNull();
        await Assert.That(wrappedException.Message).IsEqualTo("outer");
        await Assert.That(wrappedException.InnerException).IsSameReferenceAs(innerException);
    }

    /// <summary>Verifies drop oldest evicts only eligible queued items and reports committed evictions.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task EnqueueAsyncDropOldestEvictsEligibleItemsUntilBytesFit()
    {
        using var queue = new BoundedAdmissionQueue<string>(new BoundedAdmissionQueueOptions(FourItems, FourBytes, OneBlockedProducer));

        await queue.EnqueueAsync(DurableValue, OneByte, durable: true, control: false);
        await queue.EnqueueAsync("old-1", OneByte, durable: false, control: false);
        await queue.EnqueueAsync("old-2", OneByte, durable: false, control: false);
        await queue.EnqueueAsync(ControlValue, OneByte, durable: false, control: true);

        var result = await queue.EnqueueAsync("new", TwoBytes, durable: false, control: false, strategy: BufferStrategy.DropOldest);

        await Assert.That(result.Kind).IsEqualTo(BoundedAdmissionResultKind.Admitted);
        await Assert.That(result.EvictedItems).Count().IsEqualTo(TwoEvictions);
        await Assert.That(result.EvictedItems[0].Value).IsEqualTo("old-1");
        await Assert.That(result.EvictedItems[1].Value).IsEqualTo("old-2");
        await Assert.That(queue.TryDequeue(out var first)).IsTrue();
        await Assert.That(first.Value).IsEqualTo(DurableValue);
        await Assert.That(queue.TryDequeue(out var second)).IsTrue();
        await Assert.That(second.Value).IsEqualTo(ControlValue);
        await Assert.That(queue.TryDequeue(out var third)).IsTrue();
        await Assert.That(third.Value).IsEqualTo("new");
    }

    /// <summary>Verifies projected byte arithmetic cannot overflow into an admission.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task EnqueueAsyncRejectsWhenProjectedBytesWouldOverflow()
    {
        using var queue = new BoundedAdmissionQueue<string>(new BoundedAdmissionQueueOptions(TwoItems, long.MaxValue, OneBlockedProducer));

        await queue.EnqueueAsync("max", long.MaxValue, durable: true, control: false);

        Func<Task> action = () => queue.EnqueueAsync("overflow", OneByte, durable: true, control: false);

        await Assert.That(action).ThrowsExactly<BoundedAdmissionRejectedException>();
        await Assert.That(queue.Bytes).IsEqualTo(long.MaxValue);
    }

    /// <summary>Verifies evicting queued values does not invoke user equality while the queue lock is held.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task EnqueueAsyncDropOldestRemovesCommittedEvictionsWithoutUsingValueEquality()
    {
        CountingEqualsValue.Reset();
        using var queue = new BoundedAdmissionQueue<CountingEqualsValue>(new BoundedAdmissionQueueOptions(OneItem, OneByte, OneBlockedProducer));
        var oldValue = new CountingEqualsValue("old");
        var newValue = new CountingEqualsValue("new");

        await queue.EnqueueAsync(oldValue, OneByte, durable: false, control: false);

        var result = await queue.EnqueueAsync(newValue, OneByte, durable: false, control: false, strategy: BufferStrategy.DropOldest);

        await Assert.That(result.Kind).IsEqualTo(BoundedAdmissionResultKind.Admitted);
        await Assert.That(result.EvictedItems[0].Value.Name).IsEqualTo("old");
        await Assert.That(queue.TryDequeue(out var item)).IsTrue();
        await Assert.That(item.Value.Name).IsEqualTo("new");
        await Assert.That(CountingEqualsValue.GetEqualsCalls()).IsEqualTo(0);
    }

    /// <summary>Verifies drop oldest rejects overflow when eligible evictions cannot make enough room.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task EnqueueAsyncDropOldestRejectsWhenOnlyProtectedItemsRemain()
    {
        using var queue = new BoundedAdmissionQueue<string>(new BoundedAdmissionQueueOptions(TwoItems, TwoBytes, OneBlockedProducer));

        await queue.EnqueueAsync(DurableValue, OneByte, durable: true, control: false);
        await queue.EnqueueAsync(ControlValue, OneByte, durable: false, control: true);

        Func<Task> action = () => queue.EnqueueAsync("new", OneByte, durable: false, control: false, strategy: BufferStrategy.DropOldest);

        await Assert.That(action).ThrowsExactly<BoundedAdmissionRejectedException>();
    }

    /// <summary>Verifies drop newest can drop only incoming items that are safe to lose.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task EnqueueAsyncDropNewestRequiresIncomingItemToBeEligible()
    {
        using var queue = new BoundedAdmissionQueue<string>(new BoundedAdmissionQueueOptions(OneItem, OneByte, OneBlockedProducer));

        await queue.EnqueueAsync("a", OneByte, durable: true, control: false);

        var dropped = await queue.EnqueueAsync("new", OneByte, durable: false, control: false, strategy: BufferStrategy.DropNewest);
        Func<Task> durable = () => queue.EnqueueAsync("durable", OneByte, durable: true, control: false, strategy: BufferStrategy.DropNewest);
        Func<Task> control = () => queue.EnqueueAsync(ControlValue, OneByte, durable: false, control: true, strategy: BufferStrategy.DropNewest);

        await Assert.That(dropped.Kind).IsEqualTo(BoundedAdmissionResultKind.DroppedIncoming);
        await Assert.That(dropped.Item.Value).IsEqualTo("new");
        await Assert.That(durable).ThrowsExactly<BoundedAdmissionRejectedException>();
        await Assert.That(control).ThrowsExactly<BoundedAdmissionRejectedException>();
    }

    /// <summary>Verifies custom policy decisions can reject overflowing work.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task EnqueueAsyncCustomPolicyCanReject()
    {
        using var queue = new BoundedAdmissionQueue<string>(
            new BoundedAdmissionQueueOptions(OneItem, OneByte, OneBlockedProducer),
            static (_, _) => BoundedAdmissionDecision.Reject);

        await queue.EnqueueAsync("a", OneByte, durable: true, control: false);

        Func<Task> action = () => queue.EnqueueAsync("b", OneByte, durable: true, control: false, strategy: BufferStrategy.Custom);

        await Assert.That(action).ThrowsExactly<BoundedAdmissionRejectedException>();
    }

    /// <summary>Verifies custom policy decisions can drop newest eligible work.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task EnqueueAsyncCustomPolicyCanDropNewest()
    {
        using var queue = new BoundedAdmissionQueue<string>(
            new BoundedAdmissionQueueOptions(OneItem, OneByte, OneBlockedProducer),
            static (_, _) => BoundedAdmissionDecision.DropNewest);

        await queue.EnqueueAsync("a", OneByte, durable: true, control: false);

        var result = await queue.EnqueueAsync("b", OneByte, durable: false, control: false, strategy: BufferStrategy.Custom);

        await Assert.That(result.Kind).IsEqualTo(BoundedAdmissionResultKind.DroppedIncoming);
    }

    /// <summary>Verifies custom policy decisions can drop oldest eligible work.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task EnqueueAsyncCustomPolicyCanDropOldest()
    {
        using var queue = new BoundedAdmissionQueue<string>(
            new BoundedAdmissionQueueOptions(OneItem, OneByte, OneBlockedProducer),
            static (_, _) => BoundedAdmissionDecision.DropOldest(OneItem));

        await queue.EnqueueAsync("a", OneByte, durable: false, control: false);

        var result = await queue.EnqueueAsync("b", OneByte, durable: false, control: false, strategy: BufferStrategy.Custom);

        await Assert.That(result.Kind).IsEqualTo(BoundedAdmissionResultKind.Admitted);
        await Assert.That(result.EvictedItems).Count().IsEqualTo(OneItem);
    }

    /// <summary>Verifies custom strategy admits immediately when the item already fits.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task EnqueueAsyncCustomPolicyAdmitsImmediatelyWhenItemFits()
    {
        using var queue = new BoundedAdmissionQueue<string>(
            new BoundedAdmissionQueueOptions(OneItem, OneByte, OneBlockedProducer),
            static (_, _) => BoundedAdmissionDecision.Reject);

        var result = await queue.EnqueueAsync("a", OneByte, durable: true, control: false, strategy: BufferStrategy.Custom);

        await Assert.That(result.Kind).IsEqualTo(BoundedAdmissionResultKind.Admitted);
    }

    /// <summary>Verifies custom policy decisions must request at least one eviction.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task EnqueueAsyncCustomPolicyRejectsZeroDropOldestDecision()
    {
        using var queue = new BoundedAdmissionQueue<string>(
            new BoundedAdmissionQueueOptions(OneItem, OneByte, OneBlockedProducer),
            static (_, _) => BoundedAdmissionDecision.DropOldest(0));

        await queue.EnqueueAsync("a", OneByte, durable: true, control: false);

        Func<Task> action = () => queue.EnqueueAsync("b", OneByte, durable: false, control: false, strategy: BufferStrategy.Custom);

        await Assert.That(action).ThrowsExactly<BoundedAdmissionRejectedException>();
    }

    /// <summary>Verifies custom policy decisions must use a defined decision kind.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task EnqueueAsyncCustomPolicyRejectsUndefinedDecisionKind()
    {
        using var queue = new BoundedAdmissionQueue<string>(
            new BoundedAdmissionQueueOptions(OneItem, OneByte, OneBlockedProducer),
            static (_, _) => new BoundedAdmissionDecision((BoundedAdmissionDecisionKind)UndefinedEnumValue, 0));

        await queue.EnqueueAsync("a", OneByte, durable: true, control: false);

        Func<Task> action = () => queue.EnqueueAsync("b", OneByte, durable: false, control: false, strategy: BufferStrategy.Custom);

        await Assert.That(action).ThrowsExactly<BoundedAdmissionRejectedException>();
    }

    /// <summary>Verifies custom strategy requires a registered policy.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task EnqueueAsyncCustomPolicyRequiresRegistration()
    {
        using var queue = new BoundedAdmissionQueue<string>(new BoundedAdmissionQueueOptions(OneItem, OneByte, OneBlockedProducer));

        await queue.EnqueueAsync("a", OneByte, durable: true, control: false);

        Func<Task> action = () => queue.EnqueueAsync("b", OneByte, durable: true, control: false, strategy: BufferStrategy.Custom);

        await Assert.That(action).ThrowsExactly<BoundedAdmissionRejectedException>();
    }

    /// <summary>Verifies custom policies run from a snapshot and their committed decision is revalidated.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task EnqueueAsyncCustomPolicyRevalidatesCommittedEvictions()
    {
        BoundedAdmissionQueue<string>? queue = null;
        queue = new(
            new BoundedAdmissionQueueOptions(TwoItems, TwoBytes, OneBlockedProducer),
            (snapshot, incoming) =>
            {
                if (queue is null || !queue.TryDequeue(out var ignoredItem))
                {
                    throw new InvalidOperationException("The custom policy expected a queued item.");
                }

                _ = ignoredItem;
                _ = incoming;
                return BoundedAdmissionDecision.DropOldest(snapshot.Count);
            });

        await queue.EnqueueAsync("old", OneByte, durable: false, control: false);
        await queue.EnqueueAsync("protected", OneByte, durable: true, control: false);

        Func<Task> action = () => queue.EnqueueAsync("new", OneByte, durable: false, control: false, strategy: BufferStrategy.Custom);

        await Assert.That(action).ThrowsExactly<BoundedAdmissionRejectedException>();
        await Assert.That(queue.TryDequeue(out var first)).IsTrue();
        await Assert.That(first.Value).IsEqualTo("protected");
        queue.Dispose();
    }

    /// <summary>Verifies block strategy waits fairly for capacity and observes cancellation before admission.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task EnqueueAsyncBlockWaitsForCapacityAndCancellationPreventsAdmission()
    {
        using var queue = new BoundedAdmissionQueue<string>(new BoundedAdmissionQueueOptions(OneItem, OneByte, TwoBlockedProducers));
        using var cancellation = new CancellationTokenSource();

        await queue.EnqueueAsync("a", OneByte, durable: true, control: false);
        var cancelled = queue.EnqueueAsync("cancelled", OneByte, durable: true, control: false, strategy: BufferStrategy.Block, cancellationToken: cancellation.Token);
        var admitted = queue.EnqueueAsync("b", OneByte, durable: true, control: false, strategy: BufferStrategy.Block);

        await cancellation.CancelAsync();
        await Assert.That(cancelled).ThrowsExactly<TaskCanceledException>();
        await Assert.That(queue.TryDequeue(out var item)).IsTrue();
        await Assert.That(item.Value).IsEqualTo("a");

        var result = await admitted.WaitAsync(GuardTimeout);

        await Assert.That(result.Kind).IsEqualTo(BoundedAdmissionResultKind.Admitted);
        await Assert.That(queue.TryDequeue(out var second)).IsTrue();
        await Assert.That(second.Value).IsEqualTo("b");
    }

    /// <summary>Verifies a later data producer cannot bypass an earlier blocked data producer.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task EnqueueAsyncDataProducerDoesNotBypassBlockedDataProducer()
    {
        using var queue = new BoundedAdmissionQueue<string>(
            new BoundedAdmissionQueueOptions(TwoItems, TwoBytes, TwoBlockedProducers, OneItem, OneByte));

        await queue.EnqueueAsync(ResidentDataValue, OneByte, durable: false, control: false);
        var firstData = queue.EnqueueAsync(FirstDataValue, OneByte, durable: true, control: false, strategy: BufferStrategy.Block);

        Func<Task> laterData = () => queue.EnqueueAsync("later-data", OneByte, durable: false, control: false, strategy: BufferStrategy.DropOldest);

        await Assert.That(laterData).ThrowsExactly<BoundedAdmissionRejectedException>();
        await Assert.That(firstData.IsCompleted).IsFalse();
        await Assert.That(queue.TryDequeue(out var item)).IsTrue();
        await Assert.That(item.Value).IsEqualTo(ResidentDataValue);

        var result = await firstData.WaitAsync(GuardTimeout);

        await Assert.That(result.Item.Value).IsEqualTo(FirstDataValue);
    }

    /// <summary>Verifies a later custom data producer cannot bypass an earlier blocked data producer.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task EnqueueAsyncCustomDataProducerDoesNotBypassBlockedDataProducer()
    {
        using var queue = new BoundedAdmissionQueue<string>(
            new BoundedAdmissionQueueOptions(TwoItems, TwoBytes, TwoBlockedProducers, OneItem, OneByte),
            static (_, _) => BoundedAdmissionDecision.DropNewest);

        await queue.EnqueueAsync(ResidentDataValue, OneByte, durable: false, control: false);
        var firstData = queue.EnqueueAsync(FirstDataValue, OneByte, durable: true, control: false, strategy: BufferStrategy.Block);

        Func<Task> laterData = () => queue.EnqueueAsync("later-data", OneByte, durable: false, control: false, strategy: BufferStrategy.Custom);

        await Assert.That(laterData).ThrowsExactly<BoundedAdmissionRejectedException>();
        await Assert.That(firstData.IsCompleted).IsFalse();
    }

    /// <summary>Verifies cancellation after blocked admission cannot change the committed result.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task EnqueueAsyncBlockIgnoresCancellationAfterAdmissionCommits()
    {
        using var queue = new BoundedAdmissionQueue<string>(new BoundedAdmissionQueueOptions(OneItem, OneByte, OneBlockedProducer));
        using var cancellation = new CancellationTokenSource();

        await queue.EnqueueAsync(ResidentValue, OneByte, durable: true, control: false);
        var pending = queue.EnqueueAsync(AdmittedValue, OneByte, durable: true, control: false, strategy: BufferStrategy.Block, cancellationToken: cancellation.Token);

        await Assert.That(queue.TryDequeue(out var item)).IsTrue();
        await Assert.That(item.Value).IsEqualTo(ResidentValue);

        var result = await pending.WaitAsync(GuardTimeout);
        await cancellation.CancelAsync();

        await Assert.That(result.Kind).IsEqualTo(BoundedAdmissionResultKind.Admitted);
        await Assert.That(result.Item.Value).IsEqualTo(AdmittedValue);
    }

    /// <summary>Verifies a late cancellation callback is a no-op after blocked admission has been removed.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task EnqueueAsyncBlockIgnoresLateCancellationCallbackAfterAdmissionCommits()
    {
        using var queue = new BoundedAdmissionQueue<string>(new BoundedAdmissionQueueOptions(OneItem, OneByte, OneBlockedProducer));
        using var cancellation = new CancellationTokenSource();
        var dequeued = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);

        await queue.EnqueueAsync(ResidentValue, OneByte, durable: true, control: false);
        var pending = queue.EnqueueAsync(AdmittedValue, OneByte, durable: true, control: false, strategy: BufferStrategy.Block, cancellationToken: cancellation.Token);
#if NET8_0_OR_GREATER
        await using var registration = cancellation.Token.UnsafeRegister(
            static state =>
#else
        await using var registration = cancellation.Token.Register(
            static state =>
#endif
            {
                if (state is not LateCancellationContext context)
                {
                    throw new InvalidOperationException("The cancellation callback received an invalid test context.");
                }

                if (!context.Queue.TryDequeue(out var item))
                {
                    return;
                }

                context.Dequeued.SetResult(item.Value);
            },
            new LateCancellationContext(queue, dequeued));

        await cancellation.CancelAsync().WaitAsync(GuardTimeout);

        var result = await pending.WaitAsync(GuardTimeout);
        await Assert.That(await dequeued.Task.WaitAsync(GuardTimeout)).IsEqualTo(ResidentValue);
        await Assert.That(result.Kind).IsEqualTo(BoundedAdmissionResultKind.Admitted);
        await Assert.That(result.Item.Value).IsEqualTo(AdmittedValue);
    }

    /// <summary>Verifies a cancellation binding created after admission cannot replace the committed receipt.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task BlockedProducerCancelReturnsWhenProducerIsAlreadyRemoved()
    {
        using var queue = new BoundedAdmissionQueue<string>(new BoundedAdmissionQueueOptions(OneItem, OneByte, OneBlockedProducer));
        var item = new BoundedAdmissionItem<string>(AdmittedValue, OneByte, Durable: true, Control: false);
        using var cancellation = new CancellationTokenSource();
        var producer = new BoundedAdmissionQueue<string>.BlockedProducer(queue, item, cancellation.Token);
        var receipt = new BoundedAdmissionResult<string>(BoundedAdmissionResultKind.Admitted, item, []);
        producer.ReleaseResult(receipt);

        await cancellation.CancelAsync();
        producer.RegisterCancellation();
        producer.Cancel();

        await Assert.That(await producer.Task).IsEqualTo(receipt);
    }

    /// <summary>Verifies the queue bounds blocked producer registrations.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task EnqueueAsyncBlockRejectsWhenBlockedProducerLimitIsReached()
    {
        using var queue = new BoundedAdmissionQueue<string>(new BoundedAdmissionQueueOptions(OneItem, OneByte, OneBlockedProducer));

        await queue.EnqueueAsync("a", OneByte, durable: true, control: false);
        var firstWaiter = queue.EnqueueAsync("first-waiter", OneByte, durable: true, control: false, strategy: BufferStrategy.Block);

        Func<Task> action = () => queue.EnqueueAsync("second-waiter", OneByte, durable: true, control: false, strategy: BufferStrategy.Block);

        await Assert.That(action).ThrowsExactly<BoundedAdmissionRejectedException>();
        await Assert.That(firstWaiter.IsCompleted).IsFalse();
    }

    /// <summary>Verifies only blocked producers that fit are released after a dequeue.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task TryDequeueReleasesOnlyBlockedProducersThatFit()
    {
        using var queue = new BoundedAdmissionQueue<string>(new BoundedAdmissionQueueOptions(TwoItems, TwoBytes, TwoBlockedProducers));

        await queue.EnqueueAsync("a", OneByte, durable: true, control: false);
        await queue.EnqueueAsync("b", OneByte, durable: true, control: false);
        var firstWaiter = queue.EnqueueAsync("c", OneByte, durable: true, control: false, strategy: BufferStrategy.Block);
        var secondWaiter = queue.EnqueueAsync("d", OneByte, durable: true, control: false, strategy: BufferStrategy.Block);

        await Assert.That(queue.TryDequeue(out var item)).IsTrue();
        await Assert.That(item.Value).IsEqualTo("a");
        await Assert.That(await firstWaiter.WaitAsync(GuardTimeout)).IsNotEqualTo(default);
        await Assert.That(secondWaiter.IsCompleted).IsFalse();
    }

    /// <summary>Verifies control producers can use reserved capacity behind a blocked data head.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task TryDequeueReleasesControlProducerBehindBlockedDataProducer()
    {
        using var queue = new BoundedAdmissionQueue<string>(
            new BoundedAdmissionQueueOptions(TwoItems, TwoBytes, TwoBlockedProducers, OneItem, OneByte));

        await queue.EnqueueAsync(ControlValue, OneByte, durable: true, control: true);
        await queue.EnqueueAsync("data", OneByte, durable: true, control: false);
        var blockedData = queue.EnqueueAsync("blocked-data", OneByte, durable: true, control: false, strategy: BufferStrategy.Block);
        var blockedControl = queue.EnqueueAsync("blocked-control", OneByte, durable: true, control: true, strategy: BufferStrategy.Block);

        await Assert.That(queue.TryDequeue(out var item)).IsTrue();
        await Assert.That(item.Value).IsEqualTo(ControlValue);

        var result = await blockedControl.WaitAsync(GuardTimeout);

        await Assert.That(result.Item.Value).IsEqualTo("blocked-control");
        await Assert.That(blockedData.IsCompleted).IsFalse();
    }

    /// <summary>Verifies later control producers preserve FIFO behind an earlier blocked control producer.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task TryDequeueDoesNotReleaseControlProducerBehindBlockedControlProducer()
    {
        using var queue = new BoundedAdmissionQueue<string>(new BoundedAdmissionQueueOptions(TwoItems, TwoBytes, TwoBlockedProducers));

        await queue.EnqueueAsync("data", OneByte, durable: true, control: false);
        await queue.EnqueueAsync("control", OneByte, durable: true, control: true);
        var firstControl = queue.EnqueueAsync("first-control", TwoBytes, durable: true, control: true, strategy: BufferStrategy.Block);
        var secondControl = queue.EnqueueAsync("second-control", OneByte, durable: true, control: true, strategy: BufferStrategy.Block);

        await Assert.That(queue.TryDequeue(out var item)).IsTrue();
        await Assert.That(item.Value).IsEqualTo("data");
        await Assert.That(firstControl.IsCompleted).IsFalse();
        await Assert.That(secondControl.IsCompleted).IsFalse();
    }

    /// <summary>Verifies block strategy rejects work that can never fit the configured capacity.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task EnqueueAsyncBlockRejectsItemThatCannotFitEmptyQueue()
    {
        using var queue = new BoundedAdmissionQueue<string>(new BoundedAdmissionQueueOptions(OneItem, OneByte, OneBlockedProducer));

        Func<Task> action = () => queue.EnqueueAsync("large", TwoBytes, durable: true, control: false, strategy: BufferStrategy.Block);

        await Assert.That(action).ThrowsExactly<BoundedAdmissionRejectedException>();
    }

    /// <summary>Verifies empty dequeue returns false and disposal can be repeated safely.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task TryDequeueEmptyReturnsFalseAndDisposeIsIdempotent()
    {
        var queue = new BoundedAdmissionQueue<string>(new BoundedAdmissionQueueOptions(OneItem, OneByte, OneBlockedProducer));

        var hasItem = queue.TryDequeue(out var item);

        queue.Dispose();
        queue.Dispose();

        await Assert.That(hasItem).IsFalse();
        await Assert.That(item).IsEqualTo(default);
    }

    /// <summary>Verifies disposal releases blocked producers.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task DisposeUnblocksPendingProducers()
    {
        var queue = new BoundedAdmissionQueue<string>(new BoundedAdmissionQueueOptions(OneItem, OneByte, OneBlockedProducer));

        await queue.EnqueueAsync("a", OneByte, durable: true, control: false);
        var pending = queue.EnqueueAsync("b", OneByte, durable: true, control: false, strategy: BufferStrategy.Block);

        queue.Dispose();

        await Assert.That(pending).ThrowsExactly<ObjectDisposedException>();
    }

    /// <summary>Verifies reserved control capacity keeps space available for control traffic.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task EnqueueAsyncReserveControlCapacityProtectsControlTraffic()
    {
        using var queue = new BoundedAdmissionQueue<string>(new BoundedAdmissionQueueOptions(TwoItems, TwoBytes, OneBlockedProducer, OneItem, OneByte));

        await queue.EnqueueAsync("data", OneByte, durable: true, control: false);
        Func<Task> dataOverflow = () => queue.EnqueueAsync("more-data", OneByte, durable: true, control: false);
        var control = await queue.EnqueueAsync("ack", OneByte, durable: true, control: true);

        await Assert.That(dataOverflow).ThrowsExactly<BoundedAdmissionRejectedException>();
        await Assert.That(control.Kind).IsEqualTo(BoundedAdmissionResultKind.Admitted);
        await Assert.That(queue.Count).IsEqualTo(TwoItems);
    }

    /// <summary>Stores a value whose equality use can be observed by queue tests.</summary>
    /// <param name="Name">The value name.</param>
    private sealed record CountingEqualsValue(string Name)
    {
        /// <summary>Tracks equality calls made by queue internals.</summary>
        private static int _equalsCalls;

        /// <inheritdoc />
        public bool Equals(CountingEqualsValue? other)
        {
            _ = Interlocked.Increment(ref _equalsCalls);
            return ReferenceEquals(this, other);
        }

        /// <inheritdoc />
        public override int GetHashCode() => Name.GetHashCode(StringComparison.Ordinal);

        /// <summary>Resets equality calls.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void Reset() => Volatile.Write(ref _equalsCalls, 0);

        /// <summary>Gets equality calls made by queue internals.</summary>
        /// <returns>The equality call count.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int GetEqualsCalls() => Volatile.Read(ref _equalsCalls);
    }

    /// <summary>Stores state for the late cancellation callback test.</summary>
    /// <param name="Queue">The queue under test.</param>
    /// <param name="Dequeued">The observed dequeue completion.</param>
    private sealed record LateCancellationContext(BoundedAdmissionQueue<string> Queue, TaskCompletionSource<string> Dequeued);
}
