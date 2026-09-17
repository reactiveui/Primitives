// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>Emits each source boolean negated, passing errors and completion through unchanged.</summary>
/// <param name="source">The boolean source observable.</param>
public sealed class NotObservable(IObservable<bool> source) : IObservable<bool>
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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(bool value) => downstream.OnNext(!value);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => downstream.OnError(error);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted() => downstream.OnCompleted();
    }
}
