// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Signals.Core;

/// <summary>
/// Represents a finite signal backed by an enumerable sequence.
/// </summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
internal sealed class FromEnumerableSignal<T> : IRequireCurrentThread<T>, IInlineSignal<T>
{
    /// <summary>
    /// Stores the source values.
    /// </summary>
    private readonly IEnumerable<T> _values;

    /// <summary>
    /// Initializes a new instance of the <see cref="FromEnumerableSignal{T}"/> class.
    /// </summary>
    /// <param name="values">The source values.</param>
    public FromEnumerableSignal(IEnumerable<T> values) =>
        _values = values;

    /// <summary>
    /// Executes the IsRequiredSubscribeOnCurrentThread operation.
    /// </summary>
    /// <returns><see langword="false"/>.</returns>
    public bool IsRequiredSubscribeOnCurrentThread() => false;

    /// <summary>
    /// Executes the Subscribe operation.
    /// </summary>
    /// <param name="observer">The observer value.</param>
    /// <returns>The subscription.</returns>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        if (observer == null)
        {
            throw new ArgumentNullException(nameof(observer));
        }

        if (_values is T[] array)
        {
            for (var i = 0; i < array.Length; i++)
            {
                observer.OnNext(array[i]);
            }

            observer.OnCompleted();
            return Disposable.Empty;
        }

        if (_values is IReadOnlyList<T> readOnlyList)
        {
            for (var i = 0; i < readOnlyList.Count; i++)
            {
                observer.OnNext(readOnlyList[i]);
            }

            observer.OnCompleted();
            return Disposable.Empty;
        }

        foreach (var value in _values)
        {
            observer.OnNext(value);
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
    /// <returns>The subscription.</returns>
    public IDisposable Subscribe(Action<T> onNext, Action<Exception> onError, Action onCompleted)
    {
        if (onNext == null)
        {
            throw new ArgumentNullException(nameof(onNext));
        }

        if (onCompleted == null)
        {
            throw new ArgumentNullException(nameof(onCompleted));
        }

        if (_values is T[] array)
        {
            for (var i = 0; i < array.Length; i++)
            {
                onNext(array[i]);
            }

            onCompleted();
            return Disposable.Empty;
        }

        if (_values is IReadOnlyList<T> readOnlyList)
        {
            for (var i = 0; i < readOnlyList.Count; i++)
            {
                onNext(readOnlyList[i]);
            }

            onCompleted();
            return Disposable.Empty;
        }

        foreach (var value in _values)
        {
            onNext(value);
        }

        onCompleted();
        return Disposable.Empty;
    }
}
