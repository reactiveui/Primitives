// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>Runs an action before subscribing the observer to the source, so an exception from the action propagates out of <c>Subscribe</c>.</summary>
/// <typeparam name="T">The type of elements in the source sequence.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="action">The action to execute when subscribed.</param>
public sealed class DoOnSubscribeObservable<T>(
    IObservable<T> source,
    Action action) : IObservable<T>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        InvalidOperationExceptionHelper.ThrowIfNull(action);
        ArgumentExceptionHelper.ThrowIfNull(observer);

        action();
        return source.Subscribe(observer);
    }
}
