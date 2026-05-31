// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives;

/// <summary>
/// Sink that emits a running accumulation for every source value.
/// </summary>
/// <typeparam name="TSource">The source value type.</typeparam>
/// <typeparam name="TAccumulate">The accumulated value type.</typeparam>
public sealed class FoldObserver<TSource, TAccumulate> : SingleSourceObserver<TSource>
{
    /// <summary>The downstream observer.</summary>
    private readonly IObserver<TAccumulate> _observer;

    /// <summary>The accumulator function.</summary>
    private readonly Func<TAccumulate, TSource, TAccumulate> _accumulator;

    /// <summary>The current accumulated value.</summary>
    private TAccumulate _current;

    /// <summary>
    /// Initializes a new instance of the <see cref="FoldObserver{TSource, TAccumulate}"/> class.
    /// </summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="seed">The initial accumulated value.</param>
    /// <param name="accumulator">The accumulator function.</param>
    public FoldObserver(IObserver<TAccumulate> observer, TAccumulate seed, Func<TAccumulate, TSource, TAccumulate> accumulator)
    {
        _observer = observer;
        _current = seed;
        _accumulator = accumulator;
    }

    /// <inheritdoc/>
    public override void OnNext(TSource value)
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
    public override void OnError(Exception error)
    {
        try
        {
            _observer.OnError(error);
        }
        finally
        {
            Dispose();
        }
    }

    /// <inheritdoc/>
    public override void OnCompleted()
    {
        try
        {
            _observer.OnCompleted();
        }
        finally
        {
            Dispose();
        }
    }
}
