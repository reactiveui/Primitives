// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Signals.Core;

/// <summary>
/// Represents the RepeatSignal class.
/// </summary>
/// <typeparam name="T">The T type.</typeparam>
internal sealed class RepeatSignal<T> : IRequireCurrentThread<T>, IInlineSignal<T>
{
    /// <summary>
    /// Stores state for the signal implementation.
    /// </summary>
    private readonly T _value;

    /// <summary>
    /// Stores state for the signal implementation.
    /// </summary>
    private readonly int _count;

    /// <summary>
    /// Initializes a new instance of the <see cref="RepeatSignal{T}"/> class.
    /// </summary>
    /// <param name="value">The value.</param>
    /// <param name="count">The count value.</param>
    public RepeatSignal(T value, int count)
    {
        _value = value;
        _count = count;
    }

    /// <summary>
    /// Executes the IsRequiredSubscribeOnCurrentThread operation.
    /// </summary>
    /// <returns>The result.</returns>
    public bool IsRequiredSubscribeOnCurrentThread() => false;

    /// <summary>
    /// Executes the Subscribe operation.
    /// </summary>
    /// <param name="observer">The observer value.</param>
    /// <returns>The result.</returns>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        if (observer == null)
        {
            throw new ArgumentNullException(nameof(observer));
        }

        for (var i = 0; i < _count; i++)
        {
            observer.OnNext(_value);
        }

        observer.OnCompleted();
        return Disposable.Empty;
    }

    /// <summary>
    /// Executes the Subscribe operation.
    /// </summary>
    /// <param name="onNext">The onNext value.</param>
    /// <param name="onError">The onError value.</param>
    /// <param name="onCompleted">The onCompleted value.</param>
    /// <returns>The result.</returns>
    public IDisposable Subscribe(Action<T> onNext, Action<Exception> onError, Action onCompleted)
    {
        if (onNext == null)
        {
            throw new ArgumentNullException(nameof(onNext));
        }

        for (var i = 0; i < _count; i++)
        {
            onNext(_value);
        }

        onCompleted();
        return Disposable.Empty;
    }
}
