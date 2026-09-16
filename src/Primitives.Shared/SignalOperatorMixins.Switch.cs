// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Extensions;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive;
#else
namespace ReactiveUI.Primitives;
#endif

/// <summary>The Switch operator: subscribes to the most recent inner sequence and drops the previous one.</summary>
public static partial class LinqExtensions
{
    /// <summary>Coordinates a switch operation.</summary>
    /// <typeparam name="T">The source value type.</typeparam>
    /// <remarks>
    /// The gate only guards the generation bookkeeping: a notification is accepted or dropped and queued on a
    /// <see cref="SerializedDelivery{T}"/> under it, then delivered after it is released, so no lock is held while the
    /// observer, an inner subscription or its disposal runs.
    /// </remarks>
    internal sealed class SwitchCoordinator<T> : IDisposable
    {
        /// <summary>Guards the generation bookkeeping; never held while user code runs.</summary>
        private readonly Lock _gate = new();

        /// <summary>The downstream observer.</summary>
        private readonly IObserver<T> _observer;

        /// <summary>The outer subscription.</summary>
        private readonly MultipleDisposable _subscriptions = [];

        /// <summary>Serializes downstream deliveries.</summary>
        private SerializedDelivery<T> _delivery = new();

        /// <summary>The subscription to the current inner source; guarded by the gate.</summary>
        private IDisposable? _inner;

        /// <summary>A value indicating whether the outer source completed.</summary>
        private bool _outerCompleted;

        /// <summary>A value indicating whether an inner source is active.</summary>
        private bool _innerActive;

        /// <summary>The current inner source version.</summary>
        private int _version;

        /// <summary>A value indicating whether a terminal notification has been queued or the coordinator disposed.</summary>
        private bool _done;

        /// <summary>Initializes a new instance of the <see cref="SwitchCoordinator{T}"/> class.</summary>
        /// <param name="observer">The downstream observer.</param>
        internal SwitchCoordinator(IObserver<T> observer) => _observer = observer;

        /// <summary>Gets the gate guarding the generation bookkeeping.</summary>
        internal Lock Gate => _gate;

        /// <summary>Releases the active subscriptions.</summary>
        public void Dispose()
        {
            IDisposable? inner;
            lock (_gate)
            {
                _done = true;
                inner = _inner;
                _inner = null;
            }

            inner?.Dispose();
            _subscriptions.Dispose();
        }

        /// <summary>Subscribes to the outer source.</summary>
        /// <param name="sources">The outer source.</param>
        /// <returns>The coordinator that owns the subscription cleanup.</returns>
        internal SwitchCoordinator<T> Run(IObservable<IObservable<T>> sources)
        {
            _subscriptions.Add(sources.Subscribe(OnSource, OnOuterError, OnOuterCompleted));
            return this;
        }

        /// <summary>Activates the next inner generation unless a terminal notification has been sent.</summary>
        /// <param name="version">The activated generation, or zero when the coordinator is done.</param>
        /// <returns>True when an inner generation was activated; otherwise, false.</returns>
        internal bool TryBeginSource(out int version)
        {
            lock (_gate)
            {
                if (_done)
                {
                    version = 0;
                    return false;
                }

                version = _version + 1;

                Volatile.Write(ref _version, version);
                _innerActive = true;
                return true;
            }
        }

        /// <summary>Keeps the subscription for a generation when it is still current, otherwise disposes it.</summary>
        /// <param name="version">The generation the subscription belongs to.</param>
        /// <param name="subscription">The inner subscription.</param>
        internal void Install(int version, IDisposable subscription)
        {
            IDisposable? displaced;
            lock (_gate)
            {
                if (_done || version != _version)
                {
                    displaced = subscription;
                }
                else
                {
                    displaced = _inner;
                    _inner = subscription;
                }
            }

            displaced?.Dispose();
        }

        /// <summary>Marks the outer source as complete.</summary>
        internal void OnOuterCompleted()
        {
            lock (_gate)
            {
                if (_done)
                {
                    return;
                }

                _outerCompleted = true;
                if (!TryPostCompletion())
                {
                    return;
                }
            }

            _delivery.Flush(new PendingDrain(this));
        }

        /// <summary>Forwards an outer source error once.</summary>
        /// <param name="error">The error to forward.</param>
        internal void OnOuterError(Exception error)
        {
            lock (_gate)
            {
                if (_done)
                {
                    return;
                }

                _done = true;
                _ = _delivery.PostError(error);
            }

            _delivery.Flush(new PendingDrain(this));
        }

        /// <summary>Forwards an inner value when it belongs to the current source.</summary>
        /// <param name="version">The inner version.</param>
        /// <param name="value">The value to forward.</param>
        internal void OnNext(int version, T value)
        {
            lock (_gate)
            {
                if (_done || version != _version)
                {
                    return;
                }

                _ = _delivery.Post(value);
            }

            _delivery.Flush(new PendingDrain(this));
        }

        /// <summary>Forwards an inner error when it belongs to the current source.</summary>
        /// <param name="version">The inner version.</param>
        /// <param name="error">The error to forward.</param>
        internal void OnError(int version, Exception error)
        {
            lock (_gate)
            {
                if (_done || version != _version)
                {
                    return;
                }

                _done = true;
                _ = _delivery.PostError(error);
            }

            _delivery.Flush(new PendingDrain(this));
        }

        /// <summary>Completes an inner source when it belongs to the current source.</summary>
        /// <param name="version">The inner version.</param>
        internal void OnCompleted(int version)
        {
            lock (_gate)
            {
                if (_done || version != _version)
                {
                    return;
                }

                _innerActive = false;
                if (!TryPostCompletion())
                {
                    return;
                }
            }

            _delivery.Flush(new PendingDrain(this));
        }

        /// <summary>Switches to a new inner source.</summary>
        /// <param name="source">The new inner source.</param>
        private void OnSource(IObservable<T> source)
        {
            if (!TryBeginSource(out var current))
            {
                return;
            }

            Install(
                current,
                source.Subscribe(
                    value => OnNext(current, value),
                    error => OnError(current, error),
                    () => OnCompleted(current)));
        }

        /// <summary>Queues completion when both outer and inner sources are complete; called under the gate.</summary>
        /// <returns><see langword="true"/> when completion was queued.</returns>
        private bool TryPostCompletion()
        {
            if (!_outerCompleted || _innerActive)
            {
                return false;
            }

            _done = true;
            return _delivery.PostCompleted();
        }

        /// <summary>Drains this coordinator's queued notifications for the delivery gate.</summary>
        /// <param name="Owner">The coordinator.</param>
        private readonly record struct PendingDrain(SwitchCoordinator<T> Owner) : IDrainTarget
        {
            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Drain() => _ = Owner._delivery.DrainTo(Owner._observer);
        }
    }

    /// <summary>Dedicated signal for <c>SwitchTo</c> that hands each subscription to a coordinator.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class SwitchSignal<T> : IObservable<T>
    {
        /// <summary>The outer sequence of inner sources.</summary>
        private readonly IObservable<IObservable<T>> _sources;

        /// <summary>Initializes a new instance of the <see cref="SwitchSignal{T}"/> class.</summary>
        /// <param name="sources">The outer sequence of inner sources.</param>
        internal SwitchSignal(IObservable<IObservable<T>> sources) => _sources = sources;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            return new SwitchCoordinator<T>(observer).Run(_sources);
        }
    }
}
