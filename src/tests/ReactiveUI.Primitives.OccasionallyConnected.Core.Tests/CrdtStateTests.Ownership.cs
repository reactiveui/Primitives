// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected.Crdt;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Additional ownership tests for <see cref="CrdtState"/>.</summary>
public sealed partial class CrdtStateTests
{
    /// <summary>The test client identifier.</summary>
    private const string ClientId = "client";

    /// <summary>Verifies state ownership reads collection counts once before allocating copies.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task StateOwnershipReadsEachCallerCountOnce()
    {
        var components = new ThrowOnSecondDictionary();
        var bindings = new ThrowOnSecondDotElementList();

        _ = new CrdtState { Kind = CrdtKind.GCounter, GCounterComponents = components };
        _ = new CrdtState { Kind = CrdtKind.ORSet, DotBindings = bindings };

        await Assert.That(components.CountReads).IsEqualTo(1);
        await Assert.That(bindings.CountReads).IsEqualTo(1);
    }

    /// <summary>Dictionary that throws if its count is read after validation.</summary>
    private sealed class ThrowOnSecondDictionary : IReadOnlyDictionary<string, long>
    {
        /// <summary>Gets the number of count reads.</summary>
        public int CountReads { get; private set; }

        /// <inheritdoc/>
        public IEnumerable<string> Keys => [ClientId];

        /// <inheritdoc/>
        public IEnumerable<long> Values => [1];

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
        public long this[string key] => 1;

        /// <inheritdoc/>
        public bool ContainsKey(string key) => key == ClientId;

        /// <inheritdoc/>
        public bool TryGetValue(string key, out long value)
        {
            value = 1;
            return key == ClientId;
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IEnumerator<KeyValuePair<string, long>> GetEnumerator()
        {
            yield return new(ClientId, 1);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }

    /// <summary>Dot element list that throws if its count is read after validation.</summary>
    private sealed class ThrowOnSecondDotElementList : IReadOnlyList<CrdtDotElement>
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
        public CrdtDotElement this[int index] => new() { Dot = new() { ClientId = ClientId, ClientSequence = 1 }, Element = new byte[] { 1 } };

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IEnumerator<CrdtDotElement> GetEnumerator()
        {
            yield return this[0];
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
