// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>
/// Emits a stored initial value to every new subscriber, then forwards subsequent emissions from the
/// shared source. Each subscriber gets its own independent subscription to the source; the per-subscriber
/// replay is the fixed <c>initialValue</c> supplied at construction (matching the legacy BehaviorSubject
/// semantics — late subscribers do NOT see the latest value emitted to earlier subscribers).
/// </summary>
/// <typeparam name="T">The element type of the source observable.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="initialValue">The initial value emitted to every new subscriber.</param>
internal sealed class ReplayLastOnSubscribeObservable<T>(IObservable<T> source, T initialValue) : IObservable<T>
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
