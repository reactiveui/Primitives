// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives;

/// <summary>Sink that runs side-effects before forwarding each notification.</summary>
/// <typeparam name="T">The value type.</typeparam>
public sealed class TapObserver<T> : SingleSourceObserver<T>
{
    /// <summary>The downstream observer.</summary>
    private readonly IObserver<T> _observer;

    /// <summary>The value side-effect.</summary>
    private readonly Action<T> _onNext;

    /// <summary>The error side-effect.</summary>
    private readonly Action<Exception> _onError;

    /// <summary>The completion side-effect.</summary>
    private readonly Action _onCompleted;

    /// <summary>Initializes a new instance of the <see cref="TapObserver{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="onNext">The value side-effect.</param>
    /// <param name="onError">The error side-effect.</param>
    /// <param name="onCompleted">The completion side-effect.</param>
    public TapObserver(IObserver<T> observer, Action<T> onNext, Action<Exception> onError, Action onCompleted)
    {
        _observer = observer;
        _onNext = onNext;
        _onError = onError;
        _onCompleted = onCompleted;
    }

    /// <inheritdoc/>
    public override void OnNext(T value)
    {
        _onNext(value);
        _observer.OnNext(value);
    }

    /// <inheritdoc/>
    public override void OnError(Exception error)
    {
        try
        {
            _onError(error);
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
            _onCompleted();
            _observer.OnCompleted();
        }
        finally
        {
            Dispose();
        }
    }
}
