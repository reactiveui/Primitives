// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Extensions;
using System.Reactive.Linq;
using System.Reactive.Subjects;

using TUnit.Assertions.Conditions;
using TUnit.Assertions.Core;
using TUnit.Assertions.Enums;

using System.IO;
using ReactiveUI.Primitives.Extensions.Internal;
using ReactiveUI.Primitives.Extensions.Operators;
using ReactiveUI.Primitives.Extensions.Tests;
using System.Reactive;
using System.Reactive.Disposables;
using System.Reactive.Threading.Tasks;

namespace ReactiveUI.Primitives.Extensions.Tests;

/// <summary>
/// AOT-safe collection-equality helpers.
/// </summary>
internal static class AotSafeAssertionExtensions
{
    /// <summary>
    /// Asserts the collection is equivalent to <paramref name="expected"/>
    /// using the element type's default <see cref="EqualityComparer{T}"/>
    /// (order-insensitive, mirroring <c>IsEquivalentTo</c>'s default
    /// <see cref="CollectionOrdering.Any"/>).
    /// </summary>
    /// <typeparam name="TCollection">The collection type being asserted.</typeparam>
    /// <typeparam name="TItem">The element type.</typeparam>
    /// <param name="source">The assertion source.</param>
    /// <param name="expected">The expected element sequence.</param>
    /// <returns>The chained collection-equivalency assertion.</returns>
    public static IsEquivalentToAssertion<TCollection, TItem> IsCollectionEqualTo<TCollection, TItem>(
        this IAssertionSource<TCollection> source,
        IEnumerable<TItem> expected)
        where TCollection : IEnumerable<TItem>
        => source.IsEquivalentTo(expected, EqualityComparer<TItem>.Default);
}
