// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Fuses two consecutive synchronous predicates into one witness layer without closure allocation.</summary>
/// <typeparam name="T">The element type.</typeparam>
/// <param name="source">The upstream observable sequence.</param>
/// <param name="first">The first synchronous predicate.</param>
/// <param name="second">The second synchronous predicate.</param>
[System.Diagnostics.DebuggerDisplay("KeepKeepSyncSignal<{typeof(T).Name,nq}>")]
public sealed class KeepKeepSyncSignal<T>(
    IObservableAsync<T> source,
    Func<T, bool> first,
    Func<T, bool> second) : IObservableAsync<T>
{
    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken) =>
        WitnessSubscription.SubscribeAsync(
            source,
            new KeepKeepSyncWitness<T>(observer, first, second, cancellationToken),
            observer,
            cancellationToken);
}
