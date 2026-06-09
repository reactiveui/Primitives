// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives;

/// <summary>Sink that runs side-effects before forwarding each notification.</summary>
/// <typeparam name="T">The value type.</typeparam>
public sealed class TapWitness<T> : IObserver<T>, IDisposable
{
    /// <summary>The downstream observer.</summary>
    private readonly IObserver<T> _observer;

    /// <summary>The value side-effect.</summary>
    private readonly Action<T> _onNext;

    /// <summary>The error side-effect.</summary>
    private readonly Action<Exception> _onError;

    /// <summary>The completion side-effect.</summary>
    private readonly Action _onCompleted;

    /// <summary>The upstream subscription.</summary>
    private IDisposable? _subscription;

    /// <summary>Initializes a new instance of the <see cref="TapWitness{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="onNext">The value side-effect.</param>
    /// <param name="onError">The error side-effect.</param>
    /// <param name="onCompleted">The completion side-effect.</param>
    public TapWitness(IObserver<T> observer, Action<T> onNext, Action<Exception> onError, Action onCompleted)
    {
        _observer = observer;
        _onNext = onNext;
        _onError = onError;
        _onCompleted = onCompleted;
    }

    /// <inheritdoc/>
    public void OnNext(T value)
    {
        _onNext(value);
        _observer.OnNext(value);
    }

    /// <inheritdoc/>
    public void OnError(Exception error)
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
    public void OnCompleted()
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

    /// <summary>Assigns the upstream subscription, disposing it if one is already held.</summary>
    /// <param name="subscription">The upstream subscription.</param>
    public void SetSubscription(IDisposable subscription) => SinkSubscription.Set(ref _subscription, subscription);

    /// <inheritdoc/>
    public void Dispose() => SinkSubscription.Dispose(ref _subscription);
}
