// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>
/// Boolean negation operator. Replaces the <c>source.Select(b =&gt; !b)</c>
/// pattern with a dedicated forwarding observer, avoiding the per-subscription
/// closure allocation that the projection lambda would otherwise capture.
/// </summary>
/// <param name="source">The boolean source observable.</param>
internal sealed class NotObservable(IObservable<bool> source) : IObservable<bool>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<bool> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(observer);
        return source.Subscribe(new NotWitness(observer));
    }

    /// <summary>Forwarding observer that negates every boolean <see cref="OnNext"/>.</summary>
    /// <param name="downstream">The downstream observer.</param>
    private sealed class NotWitness(IObserver<bool> downstream) : IObserver<bool>
    {
        /// <inheritdoc/>
        public void OnNext(bool value) => downstream.OnNext(!value);

        /// <inheritdoc/>
        public void OnError(Exception error) => downstream.OnError(error);

        /// <inheritdoc/>
        public void OnCompleted() => downstream.OnCompleted();
    }
}
