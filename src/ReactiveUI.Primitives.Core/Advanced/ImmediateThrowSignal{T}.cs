// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Signal that delivers a terminal error synchronously inside <c>Subscribe</c>.</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("ImmediateThrowSignal: Error = {_error}")]
public sealed class ImmediateThrowSignal<T> : IRequireCurrentThread<T>, IInlineSignal<T>
{
    /// <summary>Stores the terminal error.</summary>
    private readonly Exception _error;

    /// <summary>Initializes a new instance of the <see cref="ImmediateThrowSignal{T}"/> class.</summary>
    /// <param name="error">The terminal error.</param>
    public ImmediateThrowSignal(Exception error) => _error = error;

    /// <summary>Indicates whether subscription has to happen on the calling thread.</summary>
    /// <returns>Always <see langword="false"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsRequiredSubscribeOnCurrentThread() => false;

    /// <summary>Faults <paramref name="observer"/> with the stored error before returning.</summary>
    /// <param name="observer">The observer to fault.</param>
    /// <returns>An empty disposable; the signal has finished by the time this returns.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="observer"/> is <see langword="null"/>.</exception>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        observer.OnError(_error);
        return EmptyDisposable.Instance;
    }

    /// <summary>Invokes <paramref name="onError"/> with the stored error before returning.</summary>
    /// <param name="onNext">The unused value callback.</param>
    /// <param name="onError">Invoked with the stored error.</param>
    /// <param name="onCompleted">The unused completion callback.</param>
    /// <returns>An empty disposable; the signal has finished by the time this returns.</returns>
    public IDisposable Subscribe(Action<T> onNext, Action<Exception> onError, Action onCompleted)
    {
        onError(_error);
        return EmptyDisposable.Instance;
    }
}
