// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>Drops leading null values, then forwards every value, including subsequent nulls.</summary>
/// <typeparam name="T">The element type of the source observable; must be a reference type so the
/// null check is meaningful.</typeparam>
/// <param name="source">The source observable.</param>
public sealed class SkipWhileNullObservable<T>(IObservable<T> source) : IObservable<T>
    where T : class
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(observer);
        return source.Subscribe(new SkipWhileNullWitness(observer));
    }

    /// <summary>Forwarding observer that drops leading nulls and forwards everything from the first non-null value onward.</summary>
    /// <param name="downstream">The downstream observer.</param>
    private sealed class SkipWhileNullWitness(IObserver<T> downstream) : IObserver<T>
    {
        /// <summary>Latches to <see langword="true"/> after the first non-null value has been forwarded.</summary>
        private bool _gateOpen;

        /// <inheritdoc/>
        public void OnNext(T value)
        {
            if (!_gateOpen)
            {
                if (value is null)
                {
                    return;
                }

                _gateOpen = true;
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
