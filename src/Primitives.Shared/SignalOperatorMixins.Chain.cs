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

/// <summary>The Chain operator: subscribes to each inner sequence in turn, one after the previous completes.</summary>
public static partial class LinqExtensions
{
    /// <summary>Dedicated signal for <c>Chain</c> (sequential concat of inner sources).</summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class ChainSignal<T> : IObservable<T>
    {
        /// <summary>The outer sequence of inner sources, when constructed from a source-of-sources.</summary>
        private readonly IObservable<IObservable<T>>? _sources;

        /// <summary>The first inner source, when constructed from two sources.</summary>
        private readonly IObservable<T>? _first;

        /// <summary>The second inner source, when constructed from two sources.</summary>
        private readonly IObservable<T>? _second;

        /// <summary>Initializes a new instance of the <see cref="ChainSignal{T}"/> class from a source-of-sources.</summary>
        /// <param name="sources">The outer sequence of inner sources.</param>
        internal ChainSignal(IObservable<IObservable<T>> sources) => _sources = sources;

        /// <summary>Initializes a new instance of the <see cref="ChainSignal{T}"/> class from two sources.</summary>
        /// <param name="first">The first source.</param>
        /// <param name="second">The second source.</param>
        internal ChainSignal(IObservable<T> first, IObservable<T> second)
        {
            _first = first;
            _second = second;
        }

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            ChainCoordinator<T> coordinator = new(observer);
            return _sources is not null ? coordinator.Run(_sources) : coordinator.Run(_first!, _second!);
        }
    }

    /// <summary>Coordinates sequential concatenation of inner sources for <c>Chain</c>.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <remarks>
    /// The gate only guards the queue and flags. Deliveries are serialized by a <see cref="SerializedDelivery{T}"/> and the
    /// next inner source is subscribed after the gate is released, so no lock is held while the observer or an inner source
    /// runs.
    /// </remarks>
    private sealed class ChainCoordinator<T> : IDisposable
    {
        /// <summary>Guards the queue and flags; never held while user code runs.</summary>
        private readonly Lock _gate = new();

        /// <summary>Queued inner sources awaiting the active one to complete.</summary>
        private readonly Queue<IObservable<T>> _queue = new();

        /// <summary>Active subscriptions.</summary>
        private readonly MultipleDisposable _pocket = [];

        /// <summary>The downstream observer.</summary>
        private readonly IObserver<T> _observer;

        /// <summary>Serializes downstream deliveries.</summary>
        private SerializedDelivery<T> _delivery = new();

        /// <summary>A value indicating whether an inner source is active.</summary>
        private bool _active;

        /// <summary>A value indicating whether the outer source completed.</summary>
        private bool _outerCompleted;

        /// <summary>Whether a terminal notification has been queued or the coordinator disposed; written under the gate.</summary>
        private bool _done;

        /// <summary>Initializes a new instance of the <see cref="ChainCoordinator{T}"/> class.</summary>
        /// <param name="observer">The downstream observer.</param>
        internal ChainCoordinator(IObserver<T> observer) => _observer = observer;

        /// <inheritdoc/>
        public void Dispose()
        {
            lock (_gate)
            {
                Volatile.Write(ref _done, true);
                _queue.Clear();
            }

            _pocket.Dispose();
        }

        /// <summary>Subscribes to the outer source.</summary>
        /// <param name="sources">The outer sequence of inner sources.</param>
        /// <returns>The coordinator that owns the subscription cleanup.</returns>
        internal ChainCoordinator<T> Run(IObservable<IObservable<T>> sources)
        {
            _pocket.Add(sources.Subscribe(OnSource, OnError, OnOuterCompleted));
            return this;
        }

        /// <summary>Subscribes the two fixed inner sources in order.</summary>
        /// <param name="first">The first source.</param>
        /// <param name="second">The second source.</param>
        /// <returns>The coordinator that owns the subscription cleanup.</returns>
        internal ChainCoordinator<T> Run(IObservable<T> first, IObservable<T> second)
        {
            lock (_gate)
            {
                _queue.Enqueue(first);
                _queue.Enqueue(second);
                _outerCompleted = true;
            }

            Drain();
            return this;
        }

        /// <summary>Queues a new inner source and pumps the drain.</summary>
        /// <param name="source">The inner source.</param>
        private void OnSource(IObservable<T> source)
        {
            if (source is null)
            {
                OnError(new InvalidOperationException("Chain source contained null."));
                return;
            }

            lock (_gate)
            {
                _queue.Enqueue(source);
            }

            Drain();
        }

        /// <summary>Marks the outer source complete and pumps the drain.</summary>
        private void OnOuterCompleted()
        {
            lock (_gate)
            {
                _outerCompleted = true;
            }

            Drain();
        }

        /// <summary>Marks the active inner complete and pumps the drain.</summary>
        private void OnInnerCompleted()
        {
            lock (_gate)
            {
                _active = false;
            }

            Drain();
        }

        /// <summary>Forwards an inner value unless the coordinator has terminated or been disposed.</summary>
        /// <param name="value">The value to forward.</param>
        private void OnInnerNext(T value)
        {
            if (Volatile.Read(ref _done))
            {
                return;
            }

            _delivery.OnNext(_observer, value, new PendingDrain(this));
        }

        /// <summary>Queues the first terminal error and stops subscribing further sources.</summary>
        /// <param name="error">The error to forward.</param>
        private void OnError(Exception error)
        {
            lock (_gate)
            {
                if (_done)
                {
                    return;
                }

                Volatile.Write(ref _done, true);
                _queue.Clear();
                _ = _delivery.PostError(error);
            }

            _delivery.Flush(new PendingDrain(this));
        }

        /// <summary>Subscribes the next queued inner source, or completes when the outer source is done and nothing is left.</summary>
        private void Drain()
        {
            IObservable<T>? next = null;
            lock (_gate)
            {
                if (_done)
                {
                    _queue.Clear();
                    return;
                }

                if (_active)
                {
                    return;
                }

                if (_queue.Count > 0)
                {
                    _active = true;
                    next = _queue.Dequeue();
                }
                else if (_outerCompleted)
                {
                    Volatile.Write(ref _done, true);
                    _ = _delivery.PostCompleted();
                }
                else
                {
                    return;
                }
            }

            if (next is null)
            {
                _delivery.Flush(new PendingDrain(this));
                return;
            }

            _pocket.Add(next.Subscribe(OnInnerNext, OnError, OnInnerCompleted));
        }

        /// <summary>Drains this coordinator's queued notifications for the delivery gate.</summary>
        /// <param name="Owner">The coordinator.</param>
        private readonly record struct PendingDrain(ChainCoordinator<T> Owner) : IDrainTarget
        {
            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Drain() => _ = Owner._delivery.DrainTo(Owner._observer);
        }
    }
}
