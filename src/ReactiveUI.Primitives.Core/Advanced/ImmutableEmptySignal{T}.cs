// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Signal that completes synchronously inside <c>Subscribe</c> without emitting a value.</summary>
/// <typeparam name="T">The value type.</typeparam>
public sealed class ImmutableEmptySignal<T> : IRequireCurrentThread<T>, IInlineSignal<T>
{
    /// <summary>The shared instance; the signal carries no per-subscription state.</summary>
    public static readonly ImmutableEmptySignal<T> Instance = new();

    /// <summary>Initializes a new instance of the <see cref="ImmutableEmptySignal{T}"/> class.</summary>
    private ImmutableEmptySignal()
    {
    }

    /// <summary>Indicates whether subscription has to happen on the calling thread.</summary>
    /// <returns>Always <see langword="false"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsRequiredSubscribeOnCurrentThread() => false;

    /// <summary>Completes <paramref name="observer"/> before returning.</summary>
    /// <param name="observer">The observer to complete.</param>
    /// <returns>An empty disposable; the signal has finished by the time this returns.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="observer"/> is <see langword="null"/>.</exception>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        observer.OnCompleted();
        return EmptyDisposable.Instance;
    }

    /// <summary>Invokes <paramref name="onCompleted"/> before returning.</summary>
    /// <param name="onNext">The unused value callback.</param>
    /// <param name="onError">The unused error callback.</param>
    /// <param name="onCompleted">Invoked before this method returns.</param>
    /// <returns>An empty disposable; the signal has finished by the time this returns.</returns>
    public IDisposable Subscribe(Action<T> onNext, Action<Exception> onError, Action onCompleted)
    {
        onCompleted();
        return EmptyDisposable.Instance;
    }
}
