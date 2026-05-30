// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>
/// Forwards source values; on any source error, swallows it and completes silently.
/// </summary>
/// <typeparam name="T">Element type.</typeparam>
/// <param name="source">Upstream source.</param>
internal sealed class CatchIgnoreEmptyObservable<T>(IObservable<T> source) : IObservable<T>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(observer);
        return source.Subscribe(new CatchIgnoreEmptyObserver(observer));
    }

    /// <summary>
    /// Forwarding observer that passes <see cref="OnNext"/> / <see cref="OnCompleted"/>
    /// through and replaces <see cref="OnError"/> with terminal
    /// <see cref="IObserver{T}.OnCompleted"/>.
    /// </summary>
    /// <param name="downstream">The downstream observer.</param>
    private sealed class CatchIgnoreEmptyObserver(IObserver<T> downstream) : IObserver<T>
    {
        /// <inheritdoc/>
        public void OnNext(T value) => downstream.OnNext(value);

        /// <inheritdoc/>
        public void OnError(Exception error) => downstream.OnCompleted();

        /// <inheritdoc/>
        public void OnCompleted() => downstream.OnCompleted();
    }
}
