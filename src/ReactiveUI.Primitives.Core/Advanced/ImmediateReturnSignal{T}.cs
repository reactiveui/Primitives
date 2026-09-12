// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Signal that emits one value and completes synchronously inside <c>Subscribe</c>.</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("ImmediateReturnSignal: Value = {_value}")]
public sealed class ImmediateReturnSignal<T> : IRequireCurrentThread<T>, IInlineSignal<T>
{
    /// <summary>The value emitted to every subscriber.</summary>
    private readonly T _value;

    /// <summary>Initializes a new instance of the <see cref="ImmediateReturnSignal{T}"/> class.</summary>
    /// <param name="value">The value emitted to every subscriber.</param>
    public ImmediateReturnSignal(T value) => _value = value;

    /// <summary>Indicates whether subscription has to happen on the calling thread.</summary>
    /// <returns>Always <see langword="false"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsRequiredSubscribeOnCurrentThread() => false;

    /// <summary>Emits the value to <paramref name="observer"/> and completes it before returning.</summary>
    /// <param name="observer">The observer to notify.</param>
    /// <returns>An empty disposable; the signal has finished by the time this returns.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="observer"/> is <see langword="null"/>.</exception>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        observer.OnNext(_value);
        observer.OnCompleted();
        return EmptyDisposable.Instance;
    }

    /// <summary>Invokes <paramref name="onNext"/> with the value, then <paramref name="onCompleted"/>, before returning.</summary>
    /// <param name="onNext">Invoked with the value.</param>
    /// <param name="onError">Never invoked.</param>
    /// <param name="onCompleted">Invoked after the value.</param>
    /// <returns>An empty disposable; the signal has finished by the time this returns.</returns>
    public IDisposable Subscribe(Action<T> onNext, Action<Exception> onError, Action onCompleted)
    {
        onNext(_value);
        onCompleted();
        return EmptyDisposable.Instance;
    }
}
