// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>Emits values through the first predicate match and completes, terminating before emission if the predicate throws.</summary>
/// <typeparam name="T">The type of elements in the source sequence.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="predicate">The predicate to determine when to stop taking elements.</param>
public sealed class TakeUntilInclusiveObservable<T>(
    IObservable<T> source,
    Func<T, bool> predicate) : IObservable<T>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        InvalidOperationExceptionHelper.ThrowIfNull(predicate);
        ArgumentExceptionHelper.ThrowIfNull(observer);

        return source.Subscribe(new TakeUntilInclusiveWitness(observer, predicate));
    }

    /// <summary>Observer that completes the sequence right after forwarding the first element the predicate accepts.</summary>
    /// <param name="downstream">The downstream observer.</param>
    /// <param name="predicate">The predicate to determine when to stop taking elements.</param>
    /// <remarks>Deliveries are serialized, and the predicate and the observer run without a lock held.</remarks>
    private sealed class TakeUntilInclusiveWitness(
        IObserver<T> downstream,
        Func<T, bool> predicate) : IObserver<T>
    {
        /// <summary>Serializes downstream deliveries; the first terminal notification wins.</summary>
        private SerializedDelivery<T> _delivery = new();

        /// <summary>Whether the observer is done.</summary>
        private bool _done;

        /// <inheritdoc/>
        /// <param name="value">The value to forward.</param>
        public void OnNext(T value)
        {
            if (Volatile.Read(ref _done))
            {
                return;
            }

            bool isMatch;
            try
            {
                isMatch = predicate(value);
            }
            catch (Exception ex)
            {
                Volatile.Write(ref _done, true);
                _delivery.OnError(ex, new PendingDrain(this));
                return;
            }

            _delivery.OnNext(downstream, value, new PendingDrain(this));

            if (!isMatch)
            {
                return;
            }

            Volatile.Write(ref _done, true);
            _delivery.OnCompleted(new PendingDrain(this));
        }

        /// <inheritdoc/>
        /// <param name="error">The error.</param>
        public void OnError(Exception error)
        {
            Volatile.Write(ref _done, true);
            _delivery.OnError(error, new PendingDrain(this));
        }

        /// <inheritdoc/>
        public void OnCompleted()
        {
            Volatile.Write(ref _done, true);
            _delivery.OnCompleted(new PendingDrain(this));
        }

        /// <summary>Delivers the queued notifications to the downstream observer.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void DrainPending() => _ = _delivery.DrainTo(downstream);

        /// <summary>Drains this observer's queued notifications for the delivery gate.</summary>
        /// <param name="Owner">The observer.</param>
        private readonly record struct PendingDrain(TakeUntilInclusiveWitness Owner) : IDrainTarget
        {
            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Drain() => Owner.DrainPending();
        }
    }
}
