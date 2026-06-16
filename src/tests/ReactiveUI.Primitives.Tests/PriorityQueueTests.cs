// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Core;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Verifies <see cref="PriorityQueue{T}"/> indexed-item equality contracts.</summary>
public class PriorityQueueTests
{
    /// <summary>The second test value enqueued.</summary>
    private const int SecondValue = 2;

    /// <summary>The third test value enqueued.</summary>
    private const int ThirdValue = 3;

    /// <summary>The fourth test value enqueued.</summary>
    private const int FourthValue = 4;

    /// <summary>The fifth test value enqueued.</summary>
    private const int FifthValue = 5;

    /// <summary>The sixth test value enqueued.</summary>
    private const int SixthValue = 6;

    /// <summary>The seventh test value enqueued.</summary>
    private const int SeventhValue = 7;

    /// <summary>The eighth test value enqueued.</summary>
    private const int EighthValue = 8;

    /// <summary>The length of the destination buffer used when copying.</summary>
    private const int DestinationLength = 2;

    /// <summary>The number of items to dequeue in the test.</summary>
    private const int DequeueLimit = 2;

    /// <summary>Verifies public helper methods preserve priority order.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task PublicHelpersPreservePriorityOrder()
    {
        PriorityQueue<int> queue = new(1);

        await Assert.That(queue.TryPeek(out var emptyPeek)).IsFalse();
        await Assert.That(emptyPeek).IsEqualTo(default);
        await Assert.That(queue.TryDequeue(out var emptyDequeue)).IsFalse();
        await Assert.That(emptyDequeue).IsEqualTo(default);
        await Assert.That(queue.VerifyHeapProperty()).IsTrue();

        queue.EnqueueRange([ThirdValue, 1, SecondValue]);

        await Assert.That(queue.VerifyHeapProperty()).IsTrue();
        await Assert.That(queue.TryPeek(out var first)).IsTrue();
        await Assert.That(first).IsEqualTo(1);

        var buffer = new int[DestinationLength];
        var written = queue.DequeueRange(buffer);

        await Assert.That(written).IsEqualTo(DestinationLength);
        await Assert.That(buffer.SequenceEqual([1, SecondValue])).IsTrue();
        await Assert.That(queue.TryDequeue(out var last)).IsTrue();
        await Assert.That(last).IsEqualTo(ThirdValue);
        await Assert.That(queue.Count).IsEqualTo(0);

        queue.Enqueue(SecondValue);

        await Assert.That(queue.DequeueSome(DequeueLimit).SequenceEqual([SecondValue])).IsTrue();
        await Assert.That(queue.Count).IsEqualTo(0);

        queue.Enqueue(1);
        queue.Enqueue(SecondValue);

        await Assert.That(queue.VerifyHeapProperty()).IsTrue();

        await Assert.That(queue.Dequeue()).IsEqualTo(1);
        await Assert.That(queue.Dequeue()).IsEqualTo(SecondValue);

        queue.EnqueueRange([FifthValue, FourthValue, SixthValue]);

        await Assert.That(queue.DequeueSome(DequeueLimit).SequenceEqual([FourthValue, FifthValue])).IsTrue();

        queue.EnqueueRange([EighthValue, SeventhValue]);

        await Assert.That(queue.DequeueAll().SequenceEqual([SixthValue, SeventhValue, EighthValue])).IsTrue();
        await Assert.That(queue.VerifyHeapProperty()).IsTrue();
    }

    /// <summary>Verifies public helper methods validate invalid arguments.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task PublicHelpersValidateArguments()
    {
        PriorityQueue<int> queue = new();

        Assert.Throws<ArgumentNullException>(() => queue.EnqueueRange(null!));
        Assert.Throws<ArgumentNullException>(() => queue.DequeueRange(null!));
        Assert.Throws<ArgumentOutOfRangeException>(() => queue.DequeueSome(-1));

        await Assert.That(queue.DequeueSome(0).Length).IsEqualTo(0);
        await Assert.That(queue.DequeueRange(new int[DestinationLength])).IsEqualTo(0);
    }

    /// <summary>Verifies heap verification detects invalid child priority ordering.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task VerifyHeapPropertyDetectsMutablePriorityDrift()
    {
        var leftQueue = CreateMutableQueue(out _, out var left, out _);
        left.Priority = 0;

        await Assert.That(leftQueue.VerifyHeapProperty()).IsFalse();

        var rightQueue = CreateMutableQueue(out _, out _, out var right);
        right.Priority = 0;

        await Assert.That(rightQueue.VerifyHeapProperty()).IsFalse();
    }

    /// <summary>Covers indexed-item equality, hashing, and type mismatch handling.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task IndexedItemEqualityCoversContracts()
    {
        PriorityQueue<int>.IndexedItem left = new() { Id = 1L, Value = 1 };
        PriorityQueue<int>.IndexedItem right = new() { Id = 1L, Value = 1 };
        PriorityQueue<int>.IndexedItem later = new() { Id = 2L, Value = 1 };

        await Assert.That(left.Equals(right)).IsTrue();
        await Assert.That(left.Equals((object)right)).IsTrue();
        await Assert.That(left.Equals("not-item")).IsFalse();
        await Assert.That(left.GetHashCode()).IsNotEqualTo(0);
        await Assert.That(left < later).IsTrue();
        await Assert.That(left <= right).IsTrue();
        await Assert.That(later > left).IsTrue();
        await Assert.That(right >= left).IsTrue();
    }

    /// <summary>Creates a three-item queue whose item priorities can be mutated after enqueue.</summary>
    /// <param name="parent">The root priority item.</param>
    /// <param name="left">The expected left child priority item.</param>
    /// <param name="right">The expected right child priority item.</param>
    /// <returns>The populated priority queue.</returns>
    private static PriorityQueue<PriorityItem> CreateMutableQueue(
        out PriorityItem parent,
        out PriorityItem left,
        out PriorityItem right)
    {
        parent = new(1);
        left = new(SecondValue);
        right = new(ThirdValue);

        PriorityQueue<PriorityItem> queue = new();
        queue.Enqueue(parent);
        queue.Enqueue(left);
        queue.Enqueue(right);

        return queue;
    }

    /// <summary>A mutable comparable item used to invalidate heap ordering after enqueue.</summary>
    /// <param name="priority">The initial priority.</param>
    private sealed class PriorityItem(int priority) : IComparable<PriorityItem>
    {
        /// <summary>Gets or sets the comparable priority.</summary>
        public int Priority { get; set; } = priority;

        /// <inheritdoc/>
        public int CompareTo(PriorityItem? other)
        {
            return other is null ? 1 : Priority.CompareTo(other.Priority);
        }
    }
}
