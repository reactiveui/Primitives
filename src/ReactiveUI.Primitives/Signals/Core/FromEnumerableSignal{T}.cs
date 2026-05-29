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
internal sealed class FromEnumerableSignal<T> : IRequireCurrentThread<T>, IInlineSignal<T>
{
    /// <summary>
    /// Stores the source values.
    /// </summary>
    private readonly IEnumerable<T> _values;

    /// <summary>
    /// Cancels synchronous enumeration when requested.
    /// </summary>
    private readonly CancellationToken _cancellationToken;

    /// <summary>
    /// Initializes a new instance of the <see cref="FromEnumerableSignal{T}"/> class.
    /// </summary>
    /// <param name="values">The source values.</param>
    public FromEnumerableSignal(IEnumerable<T> values) =>
        _values = values;

    /// <summary>
    /// Initializes a new instance of the <see cref="FromEnumerableSignal{T}"/> class.
    /// </summary>
    /// <param name="values">The source values.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    public FromEnumerableSignal(IEnumerable<T> values, CancellationToken cancellationToken)
    {
        _values = values;
        _cancellationToken = cancellationToken;
    }

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

        if (!_cancellationToken.CanBeCanceled && _values is T[] array)
        {
            for (var i = 0; i < array.Length; i++)
            {
                observer.OnNext(array[i]);
            }

            observer.OnCompleted();
            return Disposable.Empty;
        }

        if (!_cancellationToken.CanBeCanceled && _values is IReadOnlyList<T> readOnlyList)
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
            if (_cancellationToken.IsCancellationRequested)
            {
                return Disposable.Empty;
            }

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
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Major Code Smell",
        "S1541:Methods and properties should not be too complex",
        Justification = "The method keeps array, read-only-list, iterator, and cancellation fast paths allocation-free.")]
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

        if (!_cancellationToken.CanBeCanceled && _values is T[] array)
        {
            for (var i = 0; i < array.Length; i++)
            {
                onNext(array[i]);
            }

            onCompleted();
            return Disposable.Empty;
        }

        if (!_cancellationToken.CanBeCanceled && _values is IReadOnlyList<T> readOnlyList)
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
            if (_cancellationToken.IsCancellationRequested)
            {
                return Disposable.Empty;
            }

            onNext(value);
        }

        onCompleted();
        return Disposable.Empty;
    }

    /// <summary>
    /// Attempts to expose the backing sequence when it is already indexable and cannot be cancelled.
    /// </summary>
    /// <param name="values">The indexable values.</param>
    /// <returns><see langword="true"/> when values can be read without enumeration allocations.</returns>
    internal bool TryGetReadOnlyValues(out IReadOnlyList<T> values)
    {
        if (_cancellationToken.CanBeCanceled)
        {
            values = [];
            return false;
        }

        if (_values is IReadOnlyList<T> readOnlyList)
        {
            values = readOnlyList;
            return true;
        }

        values = [];
        return false;
    }
}
