// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Tests for <see cref="ServerCommitSnapshot"/>.</summary>
public sealed class ServerCommitSnapshotTests
{
    /// <summary>Verifies snapshot capture rejects invalid negative counts.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ConstructorRejectsNegativeEntryCount()
    {
        var streamKey = new ServerStreamKey("tenant", new("stream"));
        var entries = new NegativeCountList<ServerLedgerEntry>();

        await Assert.That(() => new ServerCommitSnapshot(streamKey, 0, null, null, entries, null, 0)).ThrowsExactly<ArgumentOutOfRangeException>();
    }

    /// <summary>Read-only list that reports an invalid count.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    private sealed class NegativeCountList<T> : IReadOnlyList<T>
    {
        /// <inheritdoc/>
        public int Count => -1;

        /// <inheritdoc/>
        public T this[int index] => throw new InvalidOperationException();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IEnumerator<T> GetEnumerator()
        {
            yield break;
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
