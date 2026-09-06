// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Represents a finite signal backed by an enumerable sequence.</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("FromEnumerableSignal: Values = {_values}, CancellationToken = {_cancellationToken}")]
public sealed class FromEnumerableSignal<T> : IRequireCurrentThread<T>, IInlineSignal<T>
{
    /// <summary>Stores the source values.</summary>
    private readonly IEnumerable<T> _values;

    /// <summary>Cancels synchronous enumeration when requested.</summary>
    private readonly CancellationToken _cancellationToken;

    /// <summary>Initializes a new instance of the <see cref="FromEnumerableSignal{T}"/> class.</summary>
    /// <param name="values">The source values.</param>
    public FromEnumerableSignal(IEnumerable<T> values) =>
        _values = values;

    /// <summary>Initializes a new instance of the <see cref="FromEnumerableSignal{T}"/> class.</summary>
    /// <param name="values">The source values.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    public FromEnumerableSignal(IEnumerable<T> values, CancellationToken cancellationToken)
    {
        _values = values;
        _cancellationToken = cancellationToken;
    }

    /// <summary>Executes the IsRequiredSubscribeOnCurrentThread operation.</summary>
    /// <returns><see langword="false"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsRequiredSubscribeOnCurrentThread() => false;

    /// <summary>Executes the Subscribe operation.</summary>
    /// <param name="observer">The observer value.</param>
    /// <returns>The subscription.</returns>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        if (!_cancellationToken.CanBeCanceled && _values is T[] array)
        {
            for (var i = 0; i < array.Length; i++)
            {
                observer.OnNext(array[i]);
            }

            observer.OnCompleted();
            return EmptyDisposable.Instance;
        }

        if (!_cancellationToken.CanBeCanceled && _values is IReadOnlyList<T> readOnlyList)
        {
            for (var i = 0; i < readOnlyList.Count; i++)
            {
                observer.OnNext(readOnlyList[i]);
            }

            observer.OnCompleted();
            return EmptyDisposable.Instance;
        }

        foreach (var value in _values)
        {
            if (_cancellationToken.IsCancellationRequested)
            {
                return EmptyDisposable.Instance;
            }

            observer.OnNext(value);
        }

        observer.OnCompleted();
        return EmptyDisposable.Instance;
    }

    /// <summary>Executes the Subscribe operation.</summary>
    /// <param name="onNext">The onNext value.</param>
    /// <param name="onError">The onError value.</param>
    /// <param name="onCompleted">The onCompleted value.</param>
    /// <returns>The subscription.</returns>
    public IDisposable Subscribe(Action<T> onNext, Action<Exception> onError, Action onCompleted)
    {
        ArgumentExceptionHelper.ThrowIfNull(onNext);

        ArgumentExceptionHelper.ThrowIfNull(onCompleted);

        if (TryDrainIndexable(onNext, onCompleted, out var fast))
        {
            return fast;
        }

        foreach (var value in _values)
        {
            if (_cancellationToken.IsCancellationRequested)
            {
                return EmptyDisposable.Instance;
            }

            onNext(value);
        }

        onCompleted();
        return EmptyDisposable.Instance;
    }

    /// <summary>Attempts to expose the backing sequence when it is already indexable and cannot be cancelled.</summary>
    /// <param name="values">The indexable values.</param>
    /// <returns><see langword="true"/> when values can be read without enumeration allocations.</returns>
    public bool TryGetReadOnlyValues(out IReadOnlyList<T> values)
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

    /// <summary>Drains an indexable, non-cancellable backing sequence without enumerator allocation.</summary>
    /// <param name="onNext">The value callback.</param>
    /// <param name="onCompleted">The completion callback.</param>
    /// <param name="result">The empty subscription when drained.</param>
    /// <returns><see langword="true"/> when the sequence was drained here.</returns>
    private bool TryDrainIndexable(Action<T> onNext, Action onCompleted, out IDisposable result)
    {
        result = EmptyDisposable.Instance;
        if (_cancellationToken.CanBeCanceled)
        {
            return false;
        }

        if (_values is T[] array)
        {
            for (var i = 0; i < array.Length; i++)
            {
                onNext(array[i]);
            }

            onCompleted();
            return true;
        }

        if (_values is IReadOnlyList<T> readOnlyList)
        {
            for (var i = 0; i < readOnlyList.Count; i++)
            {
                onNext(readOnlyList[i]);
            }

            onCompleted();
            return true;
        }

        return false;
    }
}
