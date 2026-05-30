// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>
/// Forwards source values; on any source error, emits the stored fallback then completes.
/// </summary>
/// <typeparam name="T">Element type.</typeparam>
/// <param name="source">Upstream source.</param>
/// <param name="fallback">Value emitted on the error path.</param>
internal sealed class CatchReturnObservable<T>(IObservable<T> source, T fallback) : IObservable<T>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(observer);
        return source.Subscribe(new CatchReturnObserver(observer, fallback));
    }

    /// <summary>
    /// Forwarding observer that passes <see cref="OnNext"/>/<see cref="OnCompleted"/>
    /// through and replaces <see cref="OnError"/> with an inline emit of the stored
    /// fallback followed by terminal <see cref="IObserver{T}.OnCompleted"/>.
    /// </summary>
    /// <param name="downstream">The downstream observer receiving the forwarded signals.</param>
    /// <param name="fallback">The fallback value to emit when the source errors.</param>
    private sealed class CatchReturnObserver(IObserver<T> downstream, T fallback) : IObserver<T>
    {
        /// <inheritdoc/>
        public void OnNext(T value) => downstream.OnNext(value);

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
            downstream.OnNext(fallback);
            downstream.OnCompleted();
        }

        /// <inheritdoc/>
        public void OnCompleted() => downstream.OnCompleted();
    }
}
