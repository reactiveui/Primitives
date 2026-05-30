// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Signals.Core;

/// <summary>
/// Represents the immediate Throw signal fast path.
/// </summary>
/// <typeparam name="T">The T type.</typeparam>
internal sealed class ImmediateThrowSignal<T> : IRequireCurrentThread<T>, IInlineSignal<T>
{
    /// <summary>
    /// Stores the terminal error.
    /// </summary>
    private readonly Exception _error;

    /// <summary>
    /// Initializes a new instance of the <see cref="ImmediateThrowSignal{T}"/> class.
    /// </summary>
    /// <param name="error">The terminal error.</param>
    public ImmediateThrowSignal(Exception error) => _error = error;

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

        observer.OnError(_error);
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
        onError(_error);
        return Disposable.Empty;
    }
}
