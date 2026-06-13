// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives;

/// <summary>Internal observable helpers for operators that ReactiveUI.Primitives needs but does not expose directly.</summary>
internal static class ObservableExtensions
{
    /// <summary>Cancellation operators for an observable source sequence.</summary>
    /// <param name="source">The source observable.</param>
    /// <typeparam name="T">The source value type.</typeparam>
    extension<T>(IObservable<T> source)
    {
        /// <summary>Forwards source values until <paramref name="other"/> emits a value. Completion of <paramref name="other"/> without a value does not stop the source.</summary>
        /// <typeparam name="TOther">The cancellation value type.</typeparam>
        /// <param name="other">The observable that stops the source when it emits.</param>
        /// <returns>An observable that completes when the source completes or <paramref name="other"/> emits.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="other"/> is <see langword="null"/>.</exception>
        public IObservable<T> TakeUntil<TOther>(IObservable<TOther> other)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(other);

            return new TakeUntilSignal<T, TOther>(source, other);
        }
    }

    /// <summary>Dedicated signal for <c>TakeUntil</c> that holds its sources without a per-subscription closure.</summary>
    /// <typeparam name="T">The source value type.</typeparam>
    /// <typeparam name="TOther">The cancellation value type.</typeparam>
    private sealed class TakeUntilSignal<T, TOther> : IObservable<T>
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<T> _source;

        /// <summary>The observable that stops the source when it emits.</summary>
        private readonly IObservable<TOther> _other;

        /// <summary>Initializes a new instance of the <see cref="TakeUntilSignal{T, TOther}"/> class.</summary>
        /// <param name="source">The source observable.</param>
        /// <param name="other">The observable that stops the source when it emits.</param>
        internal TakeUntilSignal(IObservable<T> source, IObservable<TOther> other)
        {
            _source = source;
            _other = other;
        }

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            var coordinator = new Coordinator(observer);
            coordinator.Add(_other.Subscribe(new CancelWitness(coordinator)));
            if (coordinator.IsStopped)
            {
                return coordinator;
            }

            coordinator.Add(_source.Subscribe(new SourceWitness(coordinator)));
            return coordinator;
        }

        /// <summary>Coordinates serialized observer callbacks and subscription lifetime for the source and cancellation streams.</summary>
        private sealed class Coordinator : IDisposable
        {
            /// <summary>The downstream observer.</summary>
            private readonly IObserver<T> _observer;

            /// <summary>Serializes downstream observer callbacks.</summary>
            private readonly Lock _gate = new();

            /// <summary>Tracks the source and cancellation subscriptions.</summary>
            private readonly MultipleDisposable _subscriptions = [];

            /// <summary>Indicates whether the sequence has stopped (0 = running, 1 = stopped).</summary>
            private int _stopped;

            /// <summary>Initializes a new instance of the <see cref="Coordinator"/> class.</summary>
            /// <param name="observer">The downstream observer.</param>
            internal Coordinator(IObserver<T> observer) => _observer = observer;

            /// <summary>Gets a value indicating whether the sequence has stopped.</summary>
            internal bool IsStopped => Volatile.Read(ref _stopped) != 0;

            /// <inheritdoc/>
            public void Dispose() => _subscriptions.Dispose();

            /// <summary>Adds a subscription to the coordinator lifetime.</summary>
            /// <param name="subscription">The subscription to add.</param>
            internal void Add(IDisposable subscription) => _subscriptions.Add(subscription);

            /// <summary>Forwards a source value when the sequence has not stopped.</summary>
            /// <param name="value">The source value.</param>
            internal void Next(T value)
            {
                lock (_gate)
                {
                    if (!IsStopped)
                    {
                        _observer.OnNext(value);
                    }
                }
            }

            /// <summary>Completes the downstream observer once and disposes all subscriptions.</summary>
            internal void Complete()
            {
                if (Interlocked.Exchange(ref _stopped, 1) != 0)
                {
                    return;
                }

                lock (_gate)
                {
                    _observer.OnCompleted();
                }

                _subscriptions.Dispose();
            }

            /// <summary>Sends an error to the downstream observer once and disposes all subscriptions.</summary>
            /// <param name="exception">The exception to forward.</param>
            internal void Error(Exception exception)
            {
                if (Interlocked.Exchange(ref _stopped, 1) != 0)
                {
                    return;
                }

                lock (_gate)
                {
                    _observer.OnError(exception);
                }

                _subscriptions.Dispose();
            }
        }

        /// <summary>Observes the source stream and routes its notifications through the coordinator.</summary>
        private sealed class SourceWitness : IObserver<T>
        {
            /// <summary>The owning coordinator.</summary>
            private readonly Coordinator _coordinator;

            /// <summary>Initializes a new instance of the <see cref="SourceWitness"/> class.</summary>
            /// <param name="coordinator">The owning coordinator.</param>
            internal SourceWitness(Coordinator coordinator) => _coordinator = coordinator;

            /// <inheritdoc/>
            public void OnNext(T value) => _coordinator.Next(value);

            /// <inheritdoc/>
            public void OnError(Exception error) => _coordinator.Error(error);

            /// <inheritdoc/>
            public void OnCompleted() => _coordinator.Complete();
        }

        /// <summary>Observes the cancellation stream; its first value (or error) stops the source.</summary>
        private sealed class CancelWitness : IObserver<TOther>
        {
            /// <summary>The owning coordinator.</summary>
            private readonly Coordinator _coordinator;

            /// <summary>Initializes a new instance of the <see cref="CancelWitness"/> class.</summary>
            /// <param name="coordinator">The owning coordinator.</param>
            internal CancelWitness(Coordinator coordinator) => _coordinator = coordinator;

            /// <inheritdoc/>
            public void OnNext(TOther value) => _coordinator.Complete();

            /// <inheritdoc/>
            public void OnError(Exception error) => _coordinator.Error(error);

            /// <inheritdoc/>
            public void OnCompleted()
            {
                // Completion of the cancellation stream without a value does not stop the source.
            }
        }
    }
}
