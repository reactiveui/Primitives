// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected.Crdt;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="CrdtMutation"/>.</summary>
public sealed class CrdtMutationTests
{
    /// <summary>The element passed to the OR-set removal.</summary>
    private static readonly byte[] Element = [1];

    /// <summary>Verifies mutation ownership reads observed dot counts once before allocating copies.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ObservedDotsOwnershipReadsCallerCountOnce()
    {
        var dots = new ThrowOnSecondDotList();

        _ = CrdtMutation.ORSetRemove(Element, dots);

        await Assert.That(dots.CountReads).IsEqualTo(1);
    }

    /// <summary>Dot list that throws if its count is read after validation.</summary>
    private sealed class ThrowOnSecondDotList : IReadOnlyList<CrdtDot>
    {
        /// <summary>Gets the number of count reads.</summary>
        public int CountReads { get; private set; }

        /// <inheritdoc/>
        public int Count
        {
            get
            {
                CountReads++;
                return CountReads == 1 ? 1 : throw new InvalidOperationException("Count must only be read once.");
            }
        }

        /// <inheritdoc/>
        public CrdtDot this[int index] => new() { ClientId = "client", ClientSequence = 1 };

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IEnumerator<CrdtDot> GetEnumerator()
        {
            yield return this[0];
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
