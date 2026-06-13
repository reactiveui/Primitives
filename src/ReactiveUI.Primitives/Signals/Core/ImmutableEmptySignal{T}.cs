// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Signals.Core;

/// <summary>Represents the ImmutableEmptySignal class.</summary>
/// <typeparam name="T">The T type.</typeparam>
internal sealed class ImmutableEmptySignal<T> : IRequireCurrentThread<T>, IInlineSignal<T>
{
    /// <summary>Executes the new operation.</summary>
    /// <returns>The result.</returns>
    internal static readonly ImmutableEmptySignal<T> Instance = new();

    /// <summary>Initializes a new instance of the <see cref="ImmutableEmptySignal{T}"/> class.</summary>
    private ImmutableEmptySignal()
    {
    }

    /// <summary>Executes the IsRequiredSubscribeOnCurrentThread operation.</summary>
    /// <returns>The result.</returns>
    public bool IsRequiredSubscribeOnCurrentThread() => false;

    /// <summary>Executes the Subscribe operation.</summary>
    /// <param name="observer">The observer value.</param>
    /// <returns>The result.</returns>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

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
        onCompleted();
        return EmptyDisposable.Instance;
    }
}
