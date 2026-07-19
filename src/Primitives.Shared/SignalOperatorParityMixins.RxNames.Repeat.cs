// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive;
#else
namespace ReactiveUI.Primitives;
#endif

/// <summary>System.Reactive-named repetition operators for observable sources.</summary>
public static partial class LinqExtensions
{
    /// <summary>System.Reactive-named repetition operators for an observable source sequence.</summary>
    /// <param name="source">The source sequence.</param>
    /// <typeparam name="T">The value type.</typeparam>
    extension<T>(IObservable<T> source)
    {
        /// <summary>Repeats the source sequence indefinitely.</summary>
        /// <returns>An observable sequence that repeats the source sequence indefinitely.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        public IObservable<T> Repeat()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new RepeatSourceSignal<T>(source, null);
        }

        /// <summary>Repeats the source sequence a fixed number of times.</summary>
        /// <param name="repeatCount">The number of times to repeat the source sequence.</param>
        /// <returns>An observable sequence that repeats the source sequence <paramref name="repeatCount"/> times.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="repeatCount"/> is less than zero.</exception>
        public IObservable<T> Repeat(int repeatCount)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentOutOfRangeExceptionHelper.ThrowIfNegative(repeatCount);

            return repeatCount == 0 ? ImmutableEmptySignal<T>.Instance : new RepeatSourceSignal<T>(source, repeatCount);
        }
    }

    /// <summary>Repeats a source observable by resubscribing after each successful completion.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class RepeatSourceSignal<T> : IRequireCurrentThread<T>
    {
        /// <summary>The source sequence.</summary>
        private readonly IObservable<T> _source;

        /// <summary>The number of repetitions, or <see langword="null"/> for indefinite repetition.</summary>
        private readonly int? _repeatCount;

        /// <summary>Initializes a new instance of the <see cref="RepeatSourceSignal{T}"/> class.</summary>
        /// <param name="source">The source sequence.</param>
        /// <param name="repeatCount">The number of repetitions, or <see langword="null"/> for indefinite repetition.</param>
        internal RepeatSourceSignal(IObservable<T> source, int? repeatCount)
        {
            _source = source;
            _repeatCount = repeatCount;
        }

        /// <inheritdoc/>
        public bool IsRequiredSubscribeOnCurrentThread() => true;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer) =>
            SignalSubscription.Subscribe(observer, true, SubscribeCore);

        /// <summary>Starts the repeat coordinator once the subscription lifetime has been created.</summary>
        /// <param name="observer">The downstream observer.</param>
        /// <param name="cancel">The cancellation handle owned by the subscription helper.</param>
        /// <returns>The repeat coordinator.</returns>
        private RepeatSourceCoordinator<T> SubscribeCore(IObserver<T> observer, IDisposable cancel)
        {
            RepeatSourceCoordinator<T> coordinator = new(_source, _repeatCount, new GuardedWitness<T>(observer, cancel));
            coordinator.Start();
            return coordinator;
        }
    }

    /// <summary>Coordinates sequential subscriptions for <see cref="RepeatSourceSignal{T}"/>.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class RepeatSourceCoordinator<T> : IDisposable
    {
        /// <summary>The source sequence.</summary>
        private readonly IObservable<T> _source;

        /// <summary>The downstream observer.</summary>
        private readonly IObserver<T> _observer;

        /// <summary>The configured repeat count.</summary>
        private readonly int? _repeatCount;

        /// <summary>The active source subscription or queued resubscription.</summary>
        private readonly SingleReplaceableDisposable _active = new();

        /// <summary>Guards synchronous completion while a subscription is still being assigned.</summary>
        private readonly Lock _gate = new();

        /// <summary>The remaining number of finite subscriptions.</summary>
        private int _remaining;

        /// <summary>Tracks whether a source subscription is currently being created.</summary>
        private bool _subscribing;

        /// <summary>Tracks synchronous completion before the subscription disposable is returned.</summary>
        private bool _completedWhileSubscribing;

        /// <summary>Tracks the current subscription generation.</summary>
        private int _generation;

        /// <summary>The generation currently allowed to forward notifications.</summary>
        private int _activeGeneration;

        /// <summary>Tracks disposal and terminal notification state.</summary>
        private int _disposed;

        /// <summary>Initializes a new instance of the <see cref="RepeatSourceCoordinator{T}"/> class.</summary>
        /// <param name="source">The source sequence.</param>
        /// <param name="repeatCount">The number of repetitions, or <see langword="null"/> for indefinite repetition.</param>
        /// <param name="observer">The downstream observer.</param>
        internal RepeatSourceCoordinator(IObservable<T> source, int? repeatCount, IObserver<T> observer)
        {
            _source = source;
            _observer = observer;
            _repeatCount = repeatCount;
            _remaining = repeatCount.GetValueOrDefault();
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }

            _active.Dispose();
        }

        /// <summary>Starts the repeated subscription loop.</summary>
        internal void Start() => ScheduleNext();

        /// <summary>Handles completion for the active source subscription.</summary>
        /// <param name="generation">The source subscription generation.</param>
        internal void OnCompleted(int generation)
        {
            if (!IsCurrentGeneration(generation))
            {
                return;
            }

            Start();
        }

        /// <summary>Handles failure for the active source subscription.</summary>
        /// <param name="generation">The source subscription generation.</param>
        /// <param name="error">The source error.</param>
        internal void OnError(int generation, Exception error)
        {
            if (!IsCurrentGeneration(generation) || Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }

            try
            {
                _observer.OnError(error);
            }
            finally
            {
                _active.Dispose();
            }
        }

        /// <summary>Forwards a value from the active source subscription.</summary>
        /// <param name="generation">The source subscription generation.</param>
        /// <param name="value">The source value.</param>
        internal void OnNext(int generation, T value)
        {
            if (!IsCurrentGeneration(generation))
            {
                return;
            }

            _observer.OnNext(value);
        }

        /// <summary>Completes the downstream observer once.</summary>
        private void Complete()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }

            try
            {
                _observer.OnCompleted();
            }
            finally
            {
                _active.Dispose();
            }
        }

        /// <summary>Schedules the next source subscription on the current-thread trampoline.</summary>
        private void ScheduleNext()
        {
            if (IsDisposed())
            {
                return;
            }

            lock (_gate)
            {
                _completedWhileSubscribing |= _subscribing;
            }

            var scheduled = Sequencer.CurrentThread.Schedule(SubscribeNext);
            if (ReferenceEquals(scheduled, EmptyDisposable.Instance) || IsDisposed())
            {
                return;
            }

            _active.Create(scheduled);
        }

        /// <summary>Subscribes to the source for the next repetition.</summary>
        private void SubscribeNext()
        {
            if (IsDisposed())
            {
                return;
            }

            if (_repeatCount is not null && _remaining == 0)
            {
                Complete();
                return;
            }

            if (_repeatCount is not null)
            {
                _remaining--;
            }

            var generation = Interlocked.Increment(ref _generation);
            Volatile.Write(ref _activeGeneration, generation);
            RepeatSourceObserver<T> observer = new(this, generation);
            IDisposable? subscription = null;
            var completedWhileSubscribing = false;
            lock (_gate)
            {
                _subscribing = true;
                _completedWhileSubscribing = false;
            }

            try
            {
                subscription = _source.Subscribe(observer);
            }
            finally
            {
                lock (_gate)
                {
                    completedWhileSubscribing = _completedWhileSubscribing;
                    _completedWhileSubscribing = false;
                    _subscribing = false;
                }
            }

            if (subscription is null)
            {
                return;
            }

            if (completedWhileSubscribing || IsDisposed())
            {
                subscription.Dispose();
                return;
            }

            _active.Create(subscription);
        }

        /// <summary>Gets a value indicating whether the coordinator is disposed.</summary>
        /// <returns><see langword="true"/> when the coordinator is disposed; otherwise, <see langword="false"/>.</returns>
        private bool IsDisposed() => Volatile.Read(ref _disposed) != 0;

        /// <summary>Gets a value indicating whether a notification belongs to the active generation.</summary>
        /// <param name="generation">The source subscription generation.</param>
        /// <returns><see langword="true"/> when the generation can still forward; otherwise, <see langword="false"/>.</returns>
        private bool IsCurrentGeneration(int generation) =>
            !IsDisposed() && Volatile.Read(ref _activeGeneration) == generation;
    }

    /// <summary>Per-subscription observer that suppresses duplicate terminal notifications.</summary>
    /// <param name="parent">The owning repeat coordinator.</param>
    /// <param name="generation">The source subscription generation.</param>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class RepeatSourceObserver<T>(RepeatSourceCoordinator<T> parent, int generation) : IObserver<T>
    {
        /// <summary>The owning repeat coordinator.</summary>
        private readonly RepeatSourceCoordinator<T> _parent = parent;

        /// <summary>The source subscription generation.</summary>
        private readonly int _generation = generation;

        /// <summary>Tracks whether this source subscription has already terminated.</summary>
        private int _terminated;

        /// <inheritdoc/>
        public void OnCompleted()
        {
            if (Interlocked.Exchange(ref _terminated, 1) != 0)
            {
                return;
            }

            _parent.OnCompleted(_generation);
        }

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
            if (Interlocked.Exchange(ref _terminated, 1) != 0)
            {
                return;
            }

            _parent.OnError(_generation, error);
        }

        /// <inheritdoc/>
        public void OnNext(T value)
        {
            if (Volatile.Read(ref _terminated) != 0)
            {
                return;
            }

            _parent.OnNext(_generation, value);
        }
    }
}
