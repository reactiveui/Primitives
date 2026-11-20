// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Async-accumulator variant of <see cref="ScanWithInitialSignal{TSource, TAccumulate}"/>.</summary>
/// <typeparam name="TSource">The upstream element type.</typeparam>
/// <typeparam name="TAccumulate">The accumulator type.</typeparam>
[System.Diagnostics.DebuggerDisplay("ScanWithInitialAsyncSignal: Source = {_source}")]
public sealed class ScanWithInitialAsyncSignal<TSource, TAccumulate> : IObservableAsync<TAccumulate>
{
    /// <summary>The upstream observable.</summary>
    private readonly IObservableAsync<TSource> _source;

    /// <summary>The initial accumulator value, emitted on subscribe.</summary>
    private readonly TAccumulate _initial;

    /// <summary>The asynchronous accumulator.</summary>
    private readonly Func<TAccumulate, TSource, CancellationToken, ValueTask<TAccumulate>> _accumulator;

    /// <summary>Initializes a new instance of the <see cref="ScanWithInitialAsyncSignal{TSource, TAccumulate}"/> class.</summary>
    /// <param name="source">The upstream observable.</param>
    /// <param name="initial">The initial accumulator value, emitted on subscribe.</param>
    /// <param name="accumulator">The asynchronous accumulator.</param>
    public ScanWithInitialAsyncSignal(
        IObservableAsync<TSource> source,
        TAccumulate initial,
        Func<TAccumulate, TSource, CancellationToken, ValueTask<TAccumulate>> accumulator)
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
        ScanWithInitialAsyncWitness<TSource, TAccumulate> sink = new(observer, _initial, _accumulator, cancellationToken);

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
