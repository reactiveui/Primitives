// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives;

/// <summary>Observable backed by a subscription delegate.</summary>
/// <typeparam name="T">The observed value type.</typeparam>
public sealed class AnonymousSignal<T> : IObservable<T>
{
    /// <summary>Subscription delegate.</summary>
    private readonly Func<IObserver<T>, IDisposable> _subscribe;

    /// <summary>Initializes a new instance of the <see cref="AnonymousSignal{T}"/> class.</summary>
    /// <param name="subscribe">Subscription delegate.</param>
    public AnonymousSignal(Func<IObserver<T>, IDisposable> subscribe) => _subscribe = subscribe ?? throw new ArgumentNullException(nameof(subscribe));

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);
        return _subscribe(observer) ?? EmptyDisposable.Instance;
    }
}
