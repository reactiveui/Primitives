// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Signal that emits one value a fixed number of times, synchronously inside <c>Subscribe</c>.</summary>
/// <typeparam name="T">The value type.</typeparam>
/// <param name="value">The value to emit.</param>
/// <param name="count">The number of times to emit the value.</param>
[System.Diagnostics.DebuggerDisplay("RepeatSignal: Value = {_value}, Count = {_count}")]
public sealed class RepeatSignal<T>(T value, int count) : IRequireCurrentThread<T>, IInlineSignal<T>
{
    /// <summary>The value to emit.</summary>
    private readonly T _value = value;

    /// <summary>The number of times to emit the value.</summary>
    private readonly int _count = count;

    /// <summary>Indicates whether subscription has to happen on the calling thread.</summary>
    /// <returns>Always <see langword="false"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsRequiredSubscribeOnCurrentThread() => false;

    /// <summary>Emits the value the configured number of times to <paramref name="observer"/>, then completes it.</summary>
    /// <param name="observer">The observer to notify.</param>
    /// <returns>An empty disposable; the signal has finished by the time this returns.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="observer"/> is <see langword="null"/>.</exception>
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

    /// <summary>Invokes <paramref name="onNext"/> with the value the configured number of times, then <paramref name="onCompleted"/>.</summary>
    /// <param name="onNext">Invoked once per repetition.</param>
    /// <param name="onError">Never invoked.</param>
    /// <param name="onCompleted">Invoked after the last repetition.</param>
    /// <returns>An empty disposable; the signal has finished by the time this returns.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="onNext"/> is <see langword="null"/>.</exception>
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
