// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Emits the initial accumulator on subscribe, then folds each source value into it and forwards the result.</summary>
/// <typeparam name="TSource">The upstream element type.</typeparam>
/// <typeparam name="TAccumulate">The accumulator type.</typeparam>
[System.Diagnostics.DebuggerDisplay("ScanWithInitialSignal: Source = {_source}")]
public sealed class ScanWithInitialSignal<TSource, TAccumulate> : IObservableAsync<TAccumulate>
{
    /// <summary>The upstream observable.</summary>
    private readonly IObservableAsync<TSource> _source;

    /// <summary>The initial accumulator value, emitted on subscribe.</summary>
    private readonly TAccumulate _initial;

    /// <summary>The synchronous accumulator.</summary>
    private readonly Func<TAccumulate, TSource, TAccumulate> _accumulator;

    /// <summary>Initializes a new instance of the <see cref="ScanWithInitialSignal{TSource, TAccumulate}"/> class.</summary>
    /// <param name="source">The upstream observable.</param>
    /// <param name="initial">The initial accumulator value, emitted on subscribe.</param>
    /// <param name="accumulator">The synchronous accumulator.</param>
    public ScanWithInitialSignal(
        IObservableAsync<TSource> source,
        TAccumulate initial,
        Func<TAccumulate, TSource, TAccumulate> accumulator)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(accumulator);
        _source = source;
        _initial = initial;
        _accumulator = accumulator;
    }

    /// <inheritdoc/>
    async ValueTask<IAsyncDisposable> IObservableAsync<TAccumulate>.SubscribeAsync(
        IObserverAsync<TAccumulate> observer,
        CancellationToken cancellationToken)
    {
        ScanWithInitialWitness<TSource, TAccumulate> sink = new(observer, _initial, _accumulator, cancellationToken);

        if (observer is IWitnessAsync<TAccumulate> downstreamBase)
        {
            downstreamBase.LinkUpstreamCancellation(sink.InternalDisposedToken);
        }

        await observer.OnNextAsync(_initial, cancellationToken).ConfigureAwait(false);

        var subscription = await _source.SubscribeAsync(sink, cancellationToken).ConfigureAwait(false);
        await sink.AssignSourceSubscriptionAsync(subscription).ConfigureAwait(false);
        return sink;
    }
}
