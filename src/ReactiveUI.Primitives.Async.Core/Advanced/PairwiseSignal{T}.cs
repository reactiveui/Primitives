// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Emits each adjacent <c>(previous, current)</c> pair of source values.</summary>
/// <typeparam name="T">The element type.</typeparam>
[System.Diagnostics.DebuggerDisplay("PairwiseSignal: Source = {_source}")]
public sealed class PairwiseSignal<T> : IObservableAsync<(T Previous, T Current)>
{
    /// <summary>The upstream observable.</summary>
    private readonly IObservableAsync<T> _source;

    /// <summary>Initializes a new instance of the <see cref="PairwiseSignal{T}"/> class.</summary>
    /// <param name="source">The upstream observable.</param>
    public PairwiseSignal(IObservableAsync<T> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        _source = source;
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask<IAsyncDisposable> IObservableAsync<(T Previous, T Current)>.SubscribeAsync(
        IObserverAsync<(T Previous, T Current)> observer,
        CancellationToken cancellationToken) =>
        WitnessSubscription.SubscribeAsync(
            _source,
            new PairwiseWitness<T>(observer, cancellationToken),
            observer,
            cancellationToken);
}
