// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Filters source elements asynchronously using <paramref name="predicate"/>.</summary>
/// <typeparam name="T">The element type of the source sequence.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="predicate">The asynchronous predicate.</param>
[System.Diagnostics.DebuggerDisplay("KeepAsyncSignal<{typeof(T).Name,nq}>")]
public sealed class KeepAsyncSignal<T>(
    IObservableAsync<T> source,
    Func<T, CancellationToken, ValueTask<bool>> predicate) : IObservableAsync<T>
{
    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken) =>
        WitnessSubscription.SubscribeAsync(
            source,
            new KeepAsyncWitness<T>(observer, predicate, cancellationToken),
            observer,
            cancellationToken);
}
