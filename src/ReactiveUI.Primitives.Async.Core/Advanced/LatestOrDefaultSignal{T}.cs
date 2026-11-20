// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Emits the seed on subscribe, then forwards only the source values that differ from the last emission.</summary>
/// <typeparam name="T">The element type.</typeparam>
[System.Diagnostics.DebuggerDisplay("LatestOrDefaultSignal: Source = {_source}")]
public sealed class LatestOrDefaultSignal<T> : IObservableAsync<T>
{
    /// <summary>The source observable.</summary>
    private readonly IObservableAsync<T> _source;

    /// <summary>The seed value emitted on subscribe.</summary>
    private readonly T _defaultValue;

    /// <summary>Initializes a new instance of the <see cref="LatestOrDefaultSignal{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="defaultValue">The seed value emitted on subscribe.</param>
    public LatestOrDefaultSignal(IObservableAsync<T> source, T defaultValue)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        _source = source;
        _defaultValue = defaultValue;
    }

    /// <inheritdoc/>
    async ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken)
    {
        LatestOrDefaultWitness<T> sink = new(observer, _defaultValue, cancellationToken);

        if (observer is IWitnessAsync<T> downstreamBase)
        {
            downstreamBase.LinkUpstreamCancellation(sink.InternalDisposedToken);
        }

        await observer.OnNextAsync(_defaultValue, cancellationToken).ConfigureAwait(false);

        var subscription = await _source.SubscribeAsync(sink, cancellationToken).ConfigureAwait(false);
        await sink.AssignSourceSubscriptionAsync(subscription).ConfigureAwait(false);
        return sink;
    }
}
