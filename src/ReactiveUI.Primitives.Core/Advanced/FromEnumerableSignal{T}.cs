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

    /// <summary>Indicates whether subscription has to happen on the calling thread.</summary>
    /// <returns>Always <see langword="false"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsRequiredSubscribeOnCurrentThread() => false;

    /// <summary>Enumerates the source into <paramref name="observer"/> on the calling thread, completing it at the end.</summary>
    /// <param name="observer">The observer to notify.</param>
    /// <returns>An empty disposable; enumeration has finished by the time this returns.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="observer"/> is <see langword="null"/>.</exception>
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

    /// <summary>Enumerates the source into the callbacks on the calling thread, invoking <paramref name="onCompleted"/> at the end.</summary>
    /// <param name="onNext">Invoked for each value.</param>
    /// <param name="onError">Never invoked; enumeration faults propagate to the caller.</param>
    /// <param name="onCompleted">Invoked once the source is drained, unless cancellation stops enumeration first.</param>
    /// <returns>An empty disposable; enumeration has finished by the time this returns.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="onNext"/> or <paramref name="onCompleted"/> is <see langword="null"/>.</exception>
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

    /// <summary>Exposes the backing sequence directly when it is an indexable list and no cancellation token was supplied.</summary>
    /// <param name="values">The indexable values, or an empty list when the sequence cannot be exposed.</param>
    /// <returns><see langword="true"/> when the values can be read without enumerating.</returns>
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
