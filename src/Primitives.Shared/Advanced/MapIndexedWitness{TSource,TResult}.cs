// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Applies an indexed selector to source values.</summary>
/// <typeparam name="TSource">The source value type.</typeparam>
/// <typeparam name="TResult">The projected value type.</typeparam>
public sealed class MapIndexedWitness<TSource, TResult> : IObserver<TSource>
{
    /// <summary>The downstream observer.</summary>
    private readonly IObserver<TResult> _observer;

    /// <summary>The indexed selector.</summary>
    private readonly Func<TSource, int, TResult> _selector;

    /// <summary>The next zero-based index.</summary>
    private int _index;

    /// <summary>Whether a terminal notification has been forwarded.</summary>
    private bool _stopped;

    /// <summary>Initializes a new instance of the <see cref="MapIndexedWitness{TSource, TResult}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="selector">The indexed selector.</param>
    public MapIndexedWitness(IObserver<TResult> observer, Func<TSource, int, TResult> selector)
    {
        _observer = observer;
        _selector = selector;
    }

    /// <inheritdoc/>
    public void OnNext(TSource value)
    {
        if (_stopped)
        {
            return;
        }

        TResult result;
        try
        {
            result = _selector(value, _index++);
        }
        catch (Exception error) when (!FatalExceptionHelper.IsFatal(error))
        {
            OnError(error);
            return;
        }

        _observer.OnNext(result);
    }

    /// <inheritdoc/>
    public void OnError(Exception error)
    {
        if (_stopped)
        {
            return;
        }

        _stopped = true;
        _observer.OnError(error);
    }

    /// <inheritdoc/>
    public void OnCompleted()
    {
        if (_stopped)
        {
            return;
        }

        _stopped = true;
        _observer.OnCompleted();
    }
}
