// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>Emits each adjacent pair of source values as <c>(Previous, Current)</c>, so the first value produces nothing on its own.</summary>
/// <typeparam name="T">The type of elements in the source sequence.</typeparam>
/// <param name="source">The source observable.</param>
public sealed class PairwiseObservable<T>(IObservable<T> source) : IObservable<(T Previous, T Current)>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<(T Previous, T Current)> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(observer);

        return source.Subscribe(new PairwiseWitness(observer));
    }

    /// <summary>Observer that holds the last value under a gate and pairs it with the next one.</summary>
    /// <param name="downstream">The downstream observer.</param>
    /// <remarks>Deliveries are serialized, and no lock is held while the observer runs.</remarks>
    private sealed class PairwiseWitness(IObserver<(T Previous, T Current)> downstream) : IObserver<T>
    {
        /// <summary>Guards the previous value; never held while the observer runs.</summary>
        private readonly Lock _gate = new();

        /// <summary>Serializes downstream deliveries.</summary>
        private SerializedDelivery<(T Previous, T Current)> _delivery = new();

        /// <summary>The previous value.</summary>
        private T? _previous;

        /// <summary>A value indicating whether there is a previous value.</summary>
        private bool _hasPrevious;

        /// <inheritdoc/>
        public void OnNext(T value)
        {
            T previous;
            bool hadPrevious;
            lock (_gate)
            {
                previous = _previous!;
                hadPrevious = _hasPrevious;
                _previous = value;
                _hasPrevious = true;
            }

            if (!hadPrevious)
            {
                return;
            }

            _delivery.OnNext(downstream, (previous, value), new PendingDrain(this));
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => _delivery.OnError(error, new PendingDrain(this));

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted() => _delivery.OnCompleted(new PendingDrain(this));

        /// <summary>Delivers the queued notifications to the downstream observer.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void DrainPending() => _ = _delivery.DrainTo(downstream);

        /// <summary>Drains this observer's queued notifications for the delivery gate.</summary>
        /// <param name="Owner">The observer.</param>
        private readonly record struct PendingDrain(PairwiseWitness Owner) : IDrainTarget
        {
            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Drain() => Owner.DrainPending();
        }
    }
}
