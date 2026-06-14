// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Core;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Verifies <see cref="PriorityQueue{T}"/> indexed-item equality contracts.</summary>
public class PriorityQueueTests
{
    private const int SecondValue = 2;

    private const int ThirdValue = 3;

    private const int FourthValue = 4;

    private const int FifthValue = 5;

    private const int SixthValue = 6;

    private const int SeventhValue = 7;

    private const int EighthValue = 8;

    private const int DestinationLength = 2;

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

    /// <summary>Covers indexed-item equality, hashing, and type mismatch handling.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task IndexedItemEqualityCoversContracts()
    {
        PriorityQueue<int>.IndexedItem left = new() { Id = 1L, Value = 1 };
        PriorityQueue<int>.IndexedItem right = new() { Id = 1L, Value = 1 };
        await Assert.That(left.Equals(right)).IsTrue();
        await Assert.That(left.Equals((object)right)).IsTrue();
        await Assert.That(left.Equals("not-item")).IsFalse();
        await Assert.That(left.GetHashCode()).IsNotEqualTo(0);
    }
}
