// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>
/// Filtering operator that forwards only the non-null values of the source
/// sequence. Replaces the <c>source.Where(x =&gt; x is not null)</c> pattern,
/// avoiding the per-subscription closure allocation that the predicate lambda
/// would otherwise capture.
/// </summary>
/// <typeparam name="T">The element type of the source observable.</typeparam>
/// <param name="source">The source observable whose null values are filtered out.</param>
public sealed class WhereIsNotNullObservable<T>(IObservable<T> source) : IObservable<T>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(observer);
        return source.Subscribe(new WhereIsNotNullWitness(observer));
    }

    /// <summary>Forwarding observer that suppresses null <see cref="OnNext"/> values and passes <see cref="OnError"/> and <see cref="OnCompleted"/> through unchanged.</summary>
    /// <param name="downstream">The downstream observer.</param>
    private sealed class WhereIsNotNullWitness(IObserver<T> downstream) : IObserver<T>
    {
        /// <inheritdoc/>
        public void OnNext(T value)
        {
            if (value is null)
            {
                return;
            }

            downstream.OnNext(value);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => downstream.OnError(error);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted() => downstream.OnCompleted();
    }
}
