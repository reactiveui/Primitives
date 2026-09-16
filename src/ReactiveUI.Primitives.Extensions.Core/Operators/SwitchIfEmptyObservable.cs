// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>Subscribes to the fallback only when the source completes without emitting, propagating source errors unchanged.</summary>
/// <typeparam name="T">The type of elements in the source sequence.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="fallback">The fallback observable.</param>
public sealed class SwitchIfEmptyObservable<T>(
    IObservable<T> source,
    IObservable<T> fallback) : IObservable<T>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        InvalidOperationExceptionHelper.ThrowIfNull(fallback);
        ArgumentExceptionHelper.ThrowIfNull(observer);

        SwitchIfEmptySink sink = new(observer, fallback);
        var sub = source.Subscribe(sink);
        sink.SetSubscription(sub);
        return sink;
    }

    /// <summary>Observer that tracks whether the source emitted and swaps in the fallback subscription when it did not.</summary>
    /// <param name="downstream">The downstream observer.</param>
    /// <param name="fallback">The fallback observable.</param>
    /// <remarks>Source deliveries are serialized, and the fallback is subscribed without the gate held.</remarks>
    private sealed class SwitchIfEmptySink(
        IObserver<T> downstream,
        IObservable<T> fallback) : IObserver<T>, IDisposable
    {
        /// <summary>Guards the flags; never held while the observer runs or the fallback is subscribed.</summary>
        private readonly Lock _gate = new();

        /// <summary>The active subscription, replaced by the fallback's when the source turns out empty.</summary>
        private readonly MutableDisposable _subscription = new();

        /// <summary>Serializes downstream deliveries from the source.</summary>
        private SerializedDelivery<T> _delivery = new();

        /// <summary>Whether the source has emitted a value.</summary>
        private bool _hasValue;

        /// <summary>Whether the sink has completed or been disposed.</summary>
        private bool _done;

        /// <summary>Stores the source subscription so the fallback subscription can take its place.</summary>
        /// <param name="sub">The subscription.</param>
        public void SetSubscription(IDisposable sub) => _subscription.Disposable = sub;

        /// <inheritdoc/>
        public void OnNext(T value)
        {
            lock (_gate)
            {
                if (_done)
                {
                    return;
                }

                _hasValue = true;
            }

            _delivery.OnNext(downstream, value, new PendingDrain(this));
        }

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
            lock (_gate)
            {
                if (_done)
                {
                    return;
                }

                _done = true;
            }

            _delivery.OnError(error, new PendingDrain(this));
        }

        /// <inheritdoc/>
        public void OnCompleted()
        {
            bool hasValue;
            lock (_gate)
            {
                if (_done)
                {
                    return;
                }

                hasValue = _hasValue;
                _done = hasValue;
            }

            if (hasValue)
            {
                _delivery.OnCompleted(new PendingDrain(this));
                return;
            }

            _subscription.Disposable = fallback.Subscribe(downstream);
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            lock (_gate)
            {
                _done = true;
                _subscription.Dispose();
            }
        }

        /// <summary>Delivers the queued notifications to the downstream observer.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void DrainPending() => _ = _delivery.DrainTo(downstream);

        /// <summary>Drains this sink's queued notifications for the delivery gate.</summary>
        /// <param name="Owner">The sink.</param>
        private readonly record struct PendingDrain(SwitchIfEmptySink Owner) : IDrainTarget
        {
            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Drain() => Owner.DrainPending();
        }
    }
}
