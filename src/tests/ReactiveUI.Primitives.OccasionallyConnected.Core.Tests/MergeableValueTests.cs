// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected.Mergeable;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="MergeableValue"/>.</summary>
public sealed class MergeableValueTests
{
    /// <summary>Verifies value ownership reads element counts once before allocating copies.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ElementOwnershipReadsCallerCountOnce()
    {
        var elements = new ThrowOnSecondMemoryList();

        _ = new MergeableValue { Kind = MergeableKind.AddWinsSet, Elements = elements };

        await Assert.That(elements.CountReads).IsEqualTo(1);
    }

    /// <summary>Memory list that throws if its count is read after validation.</summary>
    private sealed class ThrowOnSecondMemoryList : IReadOnlyList<ReadOnlyMemory<byte>>
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
        public ReadOnlyMemory<byte> this[int index] => new byte[] { 1 };

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IEnumerator<ReadOnlyMemory<byte>> GetEnumerator()
        {
            yield return this[0];
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
