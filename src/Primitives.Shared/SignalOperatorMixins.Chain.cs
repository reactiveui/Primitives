// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

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
    private sealed class ChainCoordinator<T> : IDisposable
    {
        /// <summary>Guards the queue and active/completed flags.</summary>
        private readonly Lock _gate = new();

        /// <summary>Queued inner sources awaiting the active one to complete.</summary>
        private readonly Queue<IObservable<T>> _queue = new();

        /// <summary>Active subscriptions.</summary>
        private readonly MultipleDisposable _pocket = [];

        /// <summary>The downstream observer.</summary>
        private readonly IObserver<T> _observer;

        /// <summary>A value indicating whether an inner source is active.</summary>
        private bool _active;

        /// <summary>A value indicating whether the outer source completed.</summary>
        private bool _outerCompleted;

        /// <summary>Initializes a new instance of the <see cref="ChainCoordinator{T}"/> class.</summary>
        /// <param name="observer">The downstream observer.</param>
        internal ChainCoordinator(IObserver<T> observer) => _observer = observer;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose() => _pocket.Dispose();

        /// <summary>Subscribes to the outer source.</summary>
        /// <param name="sources">The outer sequence of inner sources.</param>
        /// <returns>The coordinator that owns the subscription cleanup.</returns>
        internal ChainCoordinator<T> Run(IObservable<IObservable<T>> sources)
        {
            _pocket.Add(sources.Subscribe(OnSource, _observer.OnError, OnOuterCompleted));
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
                _observer.OnError(new InvalidOperationException("Chain source contained null."));
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

        /// <summary>Subscribes the next queued inner source, or completes when the chain is drained.</summary>
        private void Drain()
        {
            IObservable<T>? next = null;
            lock (_gate)
            {
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
                    _observer.OnCompleted();
                    return;
                }
            }

            if (next is null)
            {
                return;
            }

            _pocket.Add(next.Subscribe(_observer.OnNext, _observer.OnError, OnInnerCompleted));
        }
    }
}
