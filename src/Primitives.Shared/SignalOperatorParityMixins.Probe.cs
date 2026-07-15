// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive;
#else
namespace ReactiveUI.Primitives;
#endif

/// <summary>The Probe operator: emits the most recent source value on a fixed period.</summary>
public static partial class LinqExtensions
{
    /// <summary>Sample signal with a direct subscription path.</summary>
    /// <typeparam name="T">The source value type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="period">The sample period.</param>
    /// <param name="sequencer">The sequencer used to schedule ticks.</param>
    private sealed class ProbeSignal<T>(IObservable<T> source, TimeSpan period, ISequencer sequencer) : IRequireCurrentThread<T>
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<T> _source = source;

        /// <summary>The sample period.</summary>
        private readonly TimeSpan _period = period;

        /// <summary>The sequencer used to schedule ticks.</summary>
        private readonly ISequencer _sequencer = sequencer;

        /// <inheritdoc/>
        public bool IsRequiredSubscribeOnCurrentThread() => _sequencer == Sequencer.CurrentThread;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            ProbeCoordinator<T> coordinator = new(_source, _period, _sequencer, observer);
            if (!IsRequiredSubscribeOnCurrentThread() || !CurrentThreadSequencer.IsScheduleRequired)
            {
                return coordinator.Run();
            }

            SingleDisposable subscription = new();
            _ = Sequencer.CurrentThread.Schedule(
                (subscription, coordinator),
                static (_, s) =>
                {
                    s.subscription.Create(s.coordinator.Run());
                    return EmptyDisposable.Instance;
                });
            return subscription;
        }
    }

    /// <summary>Coordinates a sampled observable sequence without the anonymous signal wrapper.</summary>
    /// <typeparam name="T">The source value type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="period">The sample period.</param>
    /// <param name="sequencer">The sequencer used to schedule ticks.</param>
    /// <param name="observer">The downstream observer.</param>
    private sealed class ProbeCoordinator<T>(IObservable<T> source, TimeSpan period, ISequencer sequencer, IObserver<T> observer) : IObserver<T>, IDisposable
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<T> _source = source;

        /// <summary>The sample period.</summary>
        private readonly TimeSpan _period = period;

        /// <summary>The sequencer used to schedule ticks.</summary>
        private readonly ISequencer _sequencer = sequencer;

        /// <summary>The downstream observer.</summary>
        private readonly IObserver<T> _observer = observer;

        /// <summary>
        /// The synchronization gate. A reentrant monitor is used because emissions are serialized
        /// while the gate is held, which a non-reentrant spin lock cannot do safely.
        /// </summary>
        private readonly Lock _gate = new();

        /// <summary>The active source subscription.</summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Usage",
            "CA2213:Disposable fields should be disposed",
            Justification =
                "Disposed via the thread-safe Interlocked.Exchange teardown in Dispose; CA2213 does not recognize disposal of a field through Interlocked.Exchange.")]
        private IDisposable? _subscription;

        /// <summary>The active timer.</summary>
        private IDisposable? _timer;

        /// <summary>A value indicating whether a sample timer is active.</summary>
        private bool _timerActive;

        /// <summary>A value indicating whether a latest value is available.</summary>
        private bool _hasLatest;

        /// <summary>The latest value.</summary>
        private T? _latest;

        /// <summary>A value indicating whether the source has completed.</summary>
        private bool _done;

        /// <summary>A value indicating whether the coordinator has been disposed.</summary>
        private int _disposed;

        /// <inheritdoc/>
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }

            // Latch the terminal state as well as the resources: a source that ignores the disposal of its
            // subscription can keep pushing, and its completion or error must not reach an observer that has
            // already unsubscribed. The gate is reentrant, so the terminal paths may reach this while holding it.
            lock (_gate)
            {
                _done = true;
            }

            Interlocked.Exchange(ref _timer, null)?.Dispose();

            Interlocked.Exchange(ref _subscription, null)?.Dispose();
        }

        /// <summary>Records the latest source value.</summary>
        /// <param name="value">The source value.</param>
        public void OnNext(T value)
        {
            bool shouldSchedule;
            lock (_gate)
            {
                if (_done)
                {
                    return;
                }

                _hasLatest = true;
                _latest = value;
                shouldSchedule = !_timerActive;
                _timerActive = true;
            }

            if (!shouldSchedule)
            {
                return;
            }

            ScheduleNext();
        }

        /// <summary>Forwards source errors and releases active resources.</summary>
        /// <param name="error">The source error.</param>
        public void OnError(Exception error)
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

            Dispose();
        }

        /// <summary>Forwards completion and releases active resources.</summary>
        public void OnCompleted()
        {
            lock (_gate)
            {
                if (_done)
                {
                    return;
                }

                _done = true;
                _observer.OnCompleted();
            }

            Dispose();
        }

        /// <summary>Starts sampling the source.</summary>
        /// <returns>The coordinator that owns the subscription cleanup.</returns>
        internal ProbeCoordinator<T> Run()
        {
            Volatile.Write(ref _subscription, _source.Subscribe(this));
            return this;
        }

        /// <summary>Schedules the next sample tick.</summary>
        private void ScheduleNext()
        {
            var timer = _sequencer.Schedule(this, _period, static (_, coordinator) => coordinator.Tick());
            if (Volatile.Read(ref _disposed) == 0)
            {
                Volatile.Write(ref _timer, timer);
                return;
            }

            timer.Dispose();
        }

        /// <summary>Handles a sample tick.</summary>
        /// <returns>An empty disposable.</returns>
        private EmptyDisposable Tick()
        {
            // Hold the gate across the emission so the sample cannot interleave with a terminal.
            lock (_gate)
            {
                if (_done || !_hasLatest)
                {
                    _timerActive = false;
                    return EmptyDisposable.Instance;
                }

                var value = _latest!;
                _hasLatest = false;
                _timerActive = false;
                _observer.OnNext(value);
            }

            return EmptyDisposable.Instance;
        }
    }
}
