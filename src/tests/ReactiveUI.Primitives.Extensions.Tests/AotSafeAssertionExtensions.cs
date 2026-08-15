// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using TUnit.Assertions.Conditions;
using TUnit.Assertions.Core;
using TUnit.Assertions.Enums;

namespace ReactiveUI.Primitives.Extensions.Tests;

/// <summary>AOT-safe collection-equality helpers.</summary>
internal static class AotSafeAssertionExtensions
{
    /// <summary>Collection-equality helpers for an assertion source.</summary>
    /// <typeparam name="TCollection">The collection type being asserted.</typeparam>
    /// <typeparam name="TItem">The element type.</typeparam>
    /// <param name="source">The assertion source.</param>
    extension<TCollection, TItem>(IAssertionSource<TCollection> source)
        where TCollection : IEnumerable<TItem>
    {
        /// <summary>
        /// Asserts the collection is equivalent to <paramref name="expected"/>
        /// using the element type's default <see cref="EqualityComparer{T}"/>
        /// (order-insensitive, mirroring <c>IsEquivalentTo</c>'s default
        /// <see cref="CollectionOrdering.Any"/>).
        /// </summary>
        /// <param name="expected">The expected element sequence.</param>
        /// <returns>The chained collection-equivalency assertion.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal IsEquivalentToAssertion<TCollection, TItem> IsCollectionEqualTo(
            IEnumerable<TItem> expected) =>
            source.IsEquivalentTo(expected, EqualityComparer<TItem>.Default);
    }
}
