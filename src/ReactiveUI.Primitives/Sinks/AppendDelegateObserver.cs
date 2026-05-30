// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives;

/// <summary>
/// Delegate-backed observer for fused prepend/append inline subscriptions.
/// </summary>
/// <typeparam name="T">The source value type.</typeparam>
public sealed class AppendDelegateObserver<T> : SingleSourceObserver<T>
{
    /// <summary>
    /// The next callback.
    /// </summary>
    private readonly Action<T> _onNext;

    /// <summary>
    /// The error callback.
    /// </summary>
    private readonly Action<Exception> _onError;

    /// <summary>
    /// The completion callback.
    /// </summary>
    private readonly Action _onCompleted;

    /// <summary>
    /// The appended value.
    /// </summary>
    private readonly T _value;

    /// <summary>
    /// Initializes a new instance of the <see cref="AppendDelegateObserver{T}"/> class.
    /// </summary>
    /// <param name="onNext">The next callback.</param>
    /// <param name="onError">The error callback.</param>
    /// <param name="onCompleted">The completion callback.</param>
    /// <param name="value">The appended value.</param>
    public AppendDelegateObserver(Action<T> onNext, Action<Exception> onError, Action onCompleted, T value)
    {
        _onNext = onNext;
        _onError = onError;
        _onCompleted = onCompleted;
        _value = value;
    }

    /// <inheritdoc/>
    public override void OnNext(T value)
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
    public override void OnError(Exception error)
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
    public override void OnCompleted()
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
}
