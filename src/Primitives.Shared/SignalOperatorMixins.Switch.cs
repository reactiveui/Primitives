// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive;
#else
namespace ReactiveUI.Primitives;
#endif

/// <summary>The Switch operator: subscribes to the most recent inner sequence and drops the previous one.</summary>
public static partial class LinqExtensions
{
    /// <summary>Dedicated signal for <c>SwitchTo</c>; runs the coordinator without a Create closure.</summary>
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

    /// <summary>Coordinates a switch operation.</summary>
    /// <typeparam name="T">The source value type.</typeparam>
    private sealed class SwitchCoordinator<T> : IDisposable
    {
        /// <summary>The synchronization gate.</summary>
        private readonly Lock _gate = new();

        /// <summary>The downstream observer.</summary>
        private readonly IObserver<T> _observer;

        /// <summary>The active subscriptions.</summary>
        private readonly MultipleDisposable _subscriptions = [];

        /// <summary>The active inner subscription.</summary>
        private readonly SingleReplaceableDisposable _innerSlot = new();

        /// <summary>A value indicating whether the outer source completed.</summary>
        private bool _outerCompleted;

        /// <summary>A value indicating whether an inner source is active.</summary>
        private bool _innerActive;

        /// <summary>The current inner source version.</summary>
        private int _version;

        /// <summary>A value indicating whether a terminal notification has been emitted.</summary>
        private bool _done;

        /// <summary>Initializes a new instance of the <see cref="SwitchCoordinator{T}"/> class.</summary>
        /// <param name="observer">The downstream observer.</param>
        internal SwitchCoordinator(IObserver<T> observer) => _observer = observer;

        /// <summary>Releases the active subscriptions.</summary>
        public void Dispose()
        {
            _innerSlot.Dispose();
            _subscriptions.Dispose();
        }

        /// <summary>Subscribes to the outer source.</summary>
        /// <param name="sources">The outer source.</param>
        /// <returns>The coordinator that owns the subscription cleanup.</returns>
        internal SwitchCoordinator<T> Run(IObservable<IObservable<T>> sources)
        {
            _subscriptions.Add(_innerSlot);
            _subscriptions.Add(sources.Subscribe(OnSource, OnOuterError, OnOuterCompleted));
            return this;
        }

        /// <summary>Switches to a new inner source.</summary>
        /// <param name="source">The new inner source.</param>
        private void OnSource(IObservable<T> source)
        {
            int current;
            lock (_gate)
            {
                if (_done)
                {
                    return;
                }

                current = _version + 1;

                // Publish the new version so readers in gated operations observe it.
                Volatile.Write(ref _version, current);
                _innerActive = true;
            }

            _innerSlot.Create(source.Subscribe(
                value => OnNext(current, value),
                error => OnError(current, error),
                () => OnCompleted(current)));
        }

        /// <summary>Marks the outer source as complete.</summary>
        private void OnOuterCompleted()
        {
            lock (_gate)
            {
                if (_done)
                {
                    return;
                }

                _outerCompleted = true;
                TryComplete();
            }
        }

        /// <summary>Forwards an outer source error once.</summary>
        /// <param name="error">The error to forward.</param>
        private void OnOuterError(Exception error)
        {
            lock (_gate)
            {
                if (_done)
                {
                    return;
                }

                _done = true;
                _observer.OnError(error);
            }
        }

        /// <summary>Forwards an inner value when it belongs to the current source.</summary>
        /// <param name="version">The inner version.</param>
        /// <param name="value">The value to forward.</param>
        private void OnNext(int version, T value)
        {
            lock (_gate)
            {
                if (_done || version != _version)
                {
                    return;
                }

                _observer.OnNext(value);
            }
        }

        /// <summary>Forwards an inner error when it belongs to the current source.</summary>
        /// <param name="version">The inner version.</param>
        /// <param name="error">The error to forward.</param>
        private void OnError(int version, Exception error)
        {
            lock (_gate)
            {
                if (_done || version != _version)
                {
                    return;
                }

                _done = true;
                _observer.OnError(error);
            }
        }

        /// <summary>Completes an inner source when it belongs to the current source.</summary>
        /// <param name="version">The inner version.</param>
        private void OnCompleted(int version)
        {
            lock (_gate)
            {
                if (_done || version != _version)
                {
                    return;
                }

                _innerActive = false;
                TryComplete();
            }
        }

        /// <summary>Completes the observer when both outer and inner sources are complete.</summary>
        private void TryComplete()
        {
            if (_done || !_outerCompleted || _innerActive)
            {
                return;
            }

            _done = true;
            _observer.OnCompleted();
        }
    }
}
