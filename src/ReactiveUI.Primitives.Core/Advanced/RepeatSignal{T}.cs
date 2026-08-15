// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Represents the RepeatSignal class.</summary>
/// <typeparam name="T">The T type.</typeparam>
/// <param name="value">The value.</param>
/// <param name="count">The count value.</param>
[System.Diagnostics.DebuggerDisplay("Value = {_value}, Count = {_count}")]
public sealed class RepeatSignal<T>(T value, int count) : IRequireCurrentThread<T>, IInlineSignal<T>
{
    /// <summary>Stores state for the signal implementation.</summary>
    private readonly T _value = value;

    /// <summary>Stores state for the signal implementation.</summary>
    private readonly int _count = count;

    /// <summary>Executes the IsRequiredSubscribeOnCurrentThread operation.</summary>
    /// <returns>The result.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsRequiredSubscribeOnCurrentThread() => false;

    /// <summary>Executes the Subscribe operation.</summary>
    /// <param name="observer">The observer value.</param>
    /// <returns>The result.</returns>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        for (var i = 0; i < _count; i++)
        {
            observer.OnNext(_value);
        }

        observer.OnCompleted();
        return EmptyDisposable.Instance;
    }

    /// <summary>Executes the Subscribe operation.</summary>
    /// <param name="onNext">The onNext value.</param>
    /// <param name="onError">The onError value.</param>
    /// <param name="onCompleted">The onCompleted value.</param>
    /// <returns>The result.</returns>
    public IDisposable Subscribe(Action<T> onNext, Action<Exception> onError, Action onCompleted)
    {
        ArgumentExceptionHelper.ThrowIfNull(onNext);

        for (var i = 0; i < _count; i++)
        {
            onNext(_value);
        }

        onCompleted();
        return EmptyDisposable.Instance;
    }
}
