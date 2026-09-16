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

/// <summary>The Probe operator: emits the most recent source value on a fixed period.</summary>
public static partial class LinqExtensions
{
    /// <summary>Coordinates a sampled observable sequence and its tick timer.</summary>
    /// <typeparam name="T">The source value type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="period">The sample period.</param>
    /// <param name="sequencer">The sequencer used to schedule ticks.</param>
    /// <param name="observer">The downstream observer.</param>
    /// <remarks>
    /// The gate only guards the latest value and the flags. Samples and terminals are queued in order under the gate and
    /// delivered by a <see cref="SerializedDelivery{T}"/> after it is released, so no lock is held while the observer runs.
    /// </remarks>
    internal sealed class ProbeCoordinator<T>(IObservable<T> source, TimeSpan period, ISequencer sequencer, IObserver<T> observer) : IObserver<T>, IDisposable
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<T> _source = source;

        /// <summary>The sample period.</summary>
        private readonly TimeSpan _period = period;

        /// <summary>The sequencer used to schedule ticks.</summary>
        private readonly ISequencer _sequencer = sequencer;

        /// <summary>The downstream observer.</summary>
        private readonly IObserver<T> _observer = observer;

        /// <summary>Guards the latest value and the flags; never held while the observer runs.</summary>
        private readonly Lock _gate = new();

        /// <summary>Serializes downstream deliveries.</summary>
        private SerializedDelivery<T> _delivery = new();

        /// <summary>The active source subscription.</summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Usage",
            "CA2213:Disposable fields should be disposed",
            Justification = "Dispose tears this field down through Interlocked.Exchange.")]
        private IDisposable? _subscription;

        /// <summary>The active timer.</summary>
        private IDisposable? _timer;

        /// <summary>A value indicating whether a sample timer is active.</summary>
        private bool _timerActive;

        /// <summary>A value indicating whether a latest value is available.</summary>
        private bool _hasLatest;

        /// <summary>The latest value.</summary>
        private T? _latest;

        /// <summary>A value indicating whether the source has terminated or the coordinator was disposed.</summary>
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

            // Notifications received after termination are ignored.
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

        /// <summary>Queues the source error behind any pending sample, then releases active resources.</summary>
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
                _ = _delivery.PostError(error);
            }

            _delivery.Flush(new PendingDrain(this));
            Dispose();
        }

        /// <summary>Queues completion behind any pending sample, then releases active resources.</summary>
        public void OnCompleted()
        {
            lock (_gate)
            {
                if (_done)
                {
                    return;
                }

                _done = true;
                _ = _delivery.PostCompleted();
            }

            _delivery.Flush(new PendingDrain(this));
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

        /// <summary>Queues the latest value as a sample under the gate, then delivers it.</summary>
        /// <returns>An empty disposable.</returns>
        private EmptyDisposable Tick()
        {
            lock (_gate)
            {
                _timerActive = false;
                if (_done || !_hasLatest)
                {
                    return EmptyDisposable.Instance;
                }

                _hasLatest = false;
                _ = _delivery.Post(_latest!);
            }

            _delivery.Flush(new PendingDrain(this));
            return EmptyDisposable.Instance;
        }

        /// <summary>Drains this coordinator's queued notifications for the delivery gate.</summary>
        /// <param name="Owner">The coordinator.</param>
        private readonly record struct PendingDrain(ProbeCoordinator<T> Owner) : IDrainTarget
        {
            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Drain() => _ = Owner._delivery.DrainTo(Owner._observer);
        }
    }
}
