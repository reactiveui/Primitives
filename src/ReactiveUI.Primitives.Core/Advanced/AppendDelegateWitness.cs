// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Delegate-backed observer for fused prepend/append inline subscriptions.</summary>
/// <typeparam name="T">The source value type.</typeparam>
/// <param name="onNext">The next callback.</param>
/// <param name="onError">The error callback.</param>
/// <param name="onCompleted">The completion callback.</param>
/// <param name="value">The appended value.</param>
public sealed class AppendDelegateWitness<T>(Action<T> onNext, Action<Exception> onError, Action onCompleted, T value)
    : IObserver<T>, IDisposable
{
    /// <summary>The next callback.</summary>
    private readonly Action<T> _onNext = onNext;

    /// <summary>The error callback.</summary>
    private readonly Action<Exception> _onError = onError;

    /// <summary>The completion callback.</summary>
    private readonly Action _onCompleted = onCompleted;

    /// <summary>The appended value.</summary>
    private readonly T _value = value;

    /// <summary>The upstream subscription.</summary>
    private IDisposable? _subscription;

    /// <inheritdoc/>
    public void OnNext(T value)
    {
        try
        {
            _onNext(value);
        }
        catch
        {
            Dispose();
            throw;
        }
    }

    /// <inheritdoc/>
    public void OnError(Exception error)
    {
        try
        {
            _onError(error);
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
            _onNext(_value);
            _onCompleted();
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
