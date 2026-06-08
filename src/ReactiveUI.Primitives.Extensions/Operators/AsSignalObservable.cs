// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>
/// Projection operator that emits <see cref="RxVoid.Default"/> for every source
/// element. Replaces the <c>source.Select(_ =&gt; RxVoid.Default)</c> pattern,
/// avoiding the per-subscription closure allocation that the projection lambda
/// would otherwise capture.
/// </summary>
/// <typeparam name="T">The element type of the source observable (ignored).</typeparam>
/// <param name="source">The source observable whose values are ignored.</param>
internal sealed class AsSignalObservable<T>(IObservable<T> source) : IObservable<RxVoid>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<RxVoid> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(observer);
        return source.Subscribe(new AsSignalObserver(observer));
    }

    /// <summary>Forwarding observer that replaces every <see cref="OnNext"/> value with <see cref="RxVoid.Default"/>. Error and completion signals pass through unchanged.</summary>
    /// <param name="downstream">The downstream observer.</param>
    private sealed class AsSignalObserver(IObserver<RxVoid> downstream) : IObserver<T>
    {
        /// <inheritdoc/>
        public void OnNext(T value) => downstream.OnNext(RxVoid.Default);

        /// <inheritdoc/>
        public void OnError(Exception error) => downstream.OnError(error);

        /// <inheritdoc/>
        public void OnCompleted() => downstream.OnCompleted();
    }
}
