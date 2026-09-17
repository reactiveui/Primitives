// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>Emits the fixed initial value to each subscriber before subscribing independently to the source.</summary>
/// <typeparam name="T">The element type of the source observable.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="initialValue">The initial value emitted to every new subscriber.</param>
public sealed class ReplayLastOnSubscribeObservable<T>(IObservable<T> source, T initialValue) : IObservable<T>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(observer);

        observer.OnNext(initialValue);
        return source.Subscribe(observer);
    }
}
