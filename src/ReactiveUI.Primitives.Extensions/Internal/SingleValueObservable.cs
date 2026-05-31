// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Extensions.Internal;

/// <summary>
/// Synchronously emits a single cached value to each subscriber and completes. Drop-in for
/// the Rx <c>Observable.Return</c> pattern when sharing the observable instance across calls.
/// </summary>
/// <typeparam name="T">The element type.</typeparam>
/// <param name="value">The value emitted to every subscriber.</param>
internal sealed class SingleValueObservable<T>(T value) : IObservable<T>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);
        observer.OnNext(value);
        observer.OnCompleted();
        return EmptyDisposable.Instance;
    }
}
