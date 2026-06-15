// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Sink that emits a running accumulation for every source value.</summary>
/// <typeparam name="TSource">The source value type.</typeparam>
/// <typeparam name="TAccumulate">The accumulated value type.</typeparam>
public sealed class FoldWitness<TSource, TAccumulate> : IObserver<TSource>, IDisposable
{
    /// <summary>The downstream observer.</summary>
    private readonly IObserver<TAccumulate> _observer;

    /// <summary>The accumulator function.</summary>
    private readonly Func<TAccumulate, TSource, TAccumulate> _accumulator;

    /// <summary>The current accumulated value.</summary>
    private TAccumulate _current;

    /// <summary>The upstream subscription.</summary>
    private IDisposable? _subscription;

    /// <summary>Initializes a new instance of the <see cref="FoldWitness{TSource, TAccumulate}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="seed">The initial accumulated value.</param>
    /// <param name="accumulator">The accumulator function.</param>
    public FoldWitness(IObserver<TAccumulate> observer, TAccumulate seed, Func<TAccumulate, TSource, TAccumulate> accumulator)
    {
        _observer = observer;
        _current = seed;
        _accumulator = accumulator;
    }

    /// <inheritdoc/>
    public void OnNext(TSource value)
    {
        _current = _accumulator(_current, value);
        try
        {
            _observer.OnNext(_current);
        }
        catch
        {
            Dispose();
            throw;
        }
    }

    /// <inheritdoc/>
    public void OnError(Exception error) => SinkTerminal.Fault(_observer, error, this);

    /// <inheritdoc/>
    public void OnCompleted() => SinkTerminal.Complete(_observer, this);

    /// <summary>Assigns the upstream subscription, disposing it if one is already held.</summary>
    /// <param name="subscription">The upstream subscription.</param>
    public void SetSubscription(IDisposable subscription) => SinkSubscription.Set(ref _subscription, subscription);

    /// <inheritdoc/>
    public void Dispose() => SinkSubscription.Dispose(ref _subscription);
}
