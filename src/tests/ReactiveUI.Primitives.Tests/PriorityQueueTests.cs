// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Core;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Verifies <see cref="PriorityQueue{T}"/> indexed-item equality contracts.</summary>
public class PriorityQueueTests
{
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
