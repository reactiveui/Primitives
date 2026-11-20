// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Forwards every element of each enumerable the source emits, in order.</summary>
/// <typeparam name="T">The flattened element type.</typeparam>
[System.Diagnostics.DebuggerDisplay("ForEachEnumerableSignal: Source = {_source}")]
public sealed class ForEachEnumerableSignal<T> : IObservableAsync<T>
{
    /// <summary>The upstream observable of <see cref="IEnumerable{T}"/> snapshots.</summary>
    private readonly IObservableAsync<IEnumerable<T>> _source;

    /// <summary>Initializes a new instance of the <see cref="ForEachEnumerableSignal{T}"/> class.</summary>
    /// <param name="source">The upstream observable of <see cref="IEnumerable{T}"/> snapshots.</param>
    public ForEachEnumerableSignal(IObservableAsync<IEnumerable<T>> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        _source = source;
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken) =>
        WitnessSubscription.SubscribeAsync(
            _source,
            new ForEachEnumerableWitness<T>(observer, cancellationToken),
            observer,
            cancellationToken);
}
