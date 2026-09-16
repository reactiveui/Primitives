// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>
/// Emits the first predicate match and completes, propagating predicate failures and completing without a value if the source ends first.
/// </summary>
/// <typeparam name="T">The element type of the source observable.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="predicate">The match predicate; the first value returning <see langword="true"/> is emitted.</param>
public sealed class WaitUntilObservable<T>(
    IObservable<T> source,
    Func<T, bool> predicate) : IObservable<T>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        InvalidOperationExceptionHelper.ThrowIfNull(predicate);
        ArgumentExceptionHelper.ThrowIfNull(observer);

        OnceDisposable subscription = new();
        WaitUntilWitness sink = new(observer, predicate, subscription);
        subscription.Disposable = source.Subscribe(sink);
        return subscription;
    }

    /// <summary>Forwarding observer that filters by <paramref name="predicate"/> and completes after the first match, disposing the upstream subscription handle.</summary>
    /// <param name="downstream">The downstream observer.</param>
    /// <param name="predicate">The match predicate.</param>
    /// <param name="subscription">The handle controlling the upstream subscription.</param>
    /// <remarks>Deliveries are serialized, and the predicate and the observer run without a lock held.</remarks>
    private sealed class WaitUntilWitness(
        IObserver<T> downstream,
        Func<T, bool> predicate,
        IDisposable subscription) : IObserver<T>
    {
        /// <summary>Serializes downstream deliveries; the first terminal notification wins.</summary>
        private SerializedDelivery<T> _delivery = new();

        /// <summary>Whether the observer is done.</summary>
        private bool _done;

        /// <inheritdoc/>
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
                subscription.Dispose();
                return;
            }

            if (!isMatch)
            {
                return;
            }

            Volatile.Write(ref _done, true);
            _ = _delivery.Post(value);
            _ = _delivery.PostCompleted();
            _delivery.Flush(new PendingDrain(this));
            subscription.Dispose();
        }

        /// <inheritdoc/>
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
        private readonly record struct PendingDrain(WaitUntilWitness Owner) : IDrainTarget
        {
            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Drain() => Owner.DrainPending();
        }
    }
}
