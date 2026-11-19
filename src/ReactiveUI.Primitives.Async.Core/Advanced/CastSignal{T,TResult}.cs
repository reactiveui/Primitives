// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Single-observer-layer <c>Cast</c>; failed casts terminate the sequence with failure.</summary>
/// <typeparam name="T">The upstream element type.</typeparam>
/// <typeparam name="TResult">The target element type.</typeparam>
[System.Diagnostics.DebuggerDisplay("CastSignal: Source = {_source}")]
public sealed class CastSignal<T, TResult> : IObservableAsync<TResult>
{
    /// <summary>The upstream observable.</summary>
    private readonly IObservableAsync<T> _source;

    /// <summary>Initializes a new instance of the <see cref="CastSignal{T, TResult}"/> class.</summary>
    /// <param name="source">The upstream observable.</param>
    public CastSignal(IObservableAsync<T> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        _source = source;
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask<IAsyncDisposable> IObservableAsync<TResult>.SubscribeAsync(
        IObserverAsync<TResult> observer,
        CancellationToken cancellationToken) =>
        WitnessSubscription.SubscribeAsync(
            _source,
            new CastWitness<T, TResult>(observer, cancellationToken),
            observer,
            cancellationToken);
}
