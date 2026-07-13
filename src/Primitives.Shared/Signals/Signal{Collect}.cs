// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Signals;
#else
namespace ReactiveUI.Primitives.Signals;
#endif

/// <summary>Create Signals functionality.</summary>
public static partial class Signal
{
    /// <summary>Coordinates time-windowed buffering for a single subscription.</summary>
    /// <typeparam name="TSource">The source value type.</typeparam>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="timeSpan">The buffer window duration.</param>
    /// <param name="sequencer">The sequencer used to schedule flushes.</param>
    internal sealed class CollectCoordinator<TSource>(IObserver<IList<TSource>> observer, TimeSpan timeSpan, ISequencer sequencer) : IDisposable
    {
        /// <summary>The downstream observer.</summary>
        private readonly IObserver<IList<TSource>> _observer = observer;

        /// <summary>The duration of the buffer window.</summary>
        private readonly TimeSpan _timeSpan = timeSpan;

        /// <summary>The sequencer used to schedule flushes.</summary>
        private readonly ISequencer _sequencer = sequencer;

        /// <summary>Serializes access to buffered values and terminal state.</summary>
        private readonly Lock _gate = new();

        /// <summary>Tracks the source subscription and scheduled flushes.</summary>
        private readonly MultipleDisposable _disposables = [];

        /// <summary>The values collected for the current window.</summary>
        private readonly List<TSource> _values = [];

        /// <summary>Whether a flush has already been scheduled for the current window.</summary>
        private bool _flushScheduled;

        /// <summary>Whether the source has terminated.</summary>
        private bool _stopped;

        /// <inheritdoc/>
        public void Dispose() => _disposables.Dispose();

        /// <summary>Subscribes to the source and returns the coordinator as the subscription.</summary>
        /// <param name="source">The source signal.</param>
        /// <returns>The subscription that tears down source and scheduled flush work.</returns>
        internal CollectCoordinator<TSource> Subscribe(IObservable<TSource> source)
        {
            _disposables.Add(source.Subscribe(OnNext, OnError, OnCompleted));
            return this;
        }

        /// <summary>Records a value and schedules a flush for the current window when needed.</summary>
        /// <param name="value">The source value.</param>
        private void OnNext(TSource value)
        {
            if (!TryRecord(value))
            {
                return;
            }

            _disposables.Add(_sequencer.Schedule(_timeSpan, Flush));
        }

        /// <summary>Forwards a terminal error after marking the coordinator stopped.</summary>
        /// <param name="error">The source error.</param>
        private void OnError(Exception error)
        {
            MarkStopped();
            _observer.OnError(error);
        }

        /// <summary>Flushes remaining values and forwards completion.</summary>
        private void OnCompleted()
        {
            var batch = CompleteAndTakeFinalBatch();
            if (batch is { Length: > 0 })
            {
                _observer.OnNext(batch);
            }

            _observer.OnCompleted();
        }

        /// <summary>Flushes the current window if it still has buffered values.</summary>
        private void Flush()
        {
            var batch = TakeScheduledBatch();
            if (batch is not { Length: > 0 })
            {
                return;
            }

            _observer.OnNext(batch);
        }

        /// <summary>Stores a value and reports whether this value opened a new scheduled window.</summary>
        /// <param name="value">The source value.</param>
        /// <returns><see langword="true"/> when a flush should be scheduled.</returns>
        private bool TryRecord(TSource value)
        {
            lock (_gate)
            {
                if (_stopped)
                {
                    return false;
                }

                _values.Add(value);
                if (_flushScheduled)
                {
                    return false;
                }

                _flushScheduled = true;
                return true;
            }
        }

        /// <summary>Marks the coordinator as stopped.</summary>
        private void MarkStopped()
        {
            lock (_gate)
            {
                _stopped = true;
            }
        }

        /// <summary>Returns and clears values from a scheduled flush.</summary>
        /// <returns>The values to emit, or <see langword="null"/> when there is no batch.</returns>
        private TSource[]? TakeScheduledBatch()
        {
            lock (_gate)
            {
                _flushScheduled = false;
                return _values.Count == 0 || _stopped ? null : TakeValues();
            }
        }

        /// <summary>Stops the coordinator and returns the final buffered values.</summary>
        /// <returns>The final buffered values, or <see langword="null"/> when no values remain.</returns>
        private TSource[]? CompleteAndTakeFinalBatch()
        {
            lock (_gate)
            {
                _stopped = true;
                return _values.Count == 0 ? null : TakeValues();
            }
        }

        /// <summary>Copies and clears the buffered values.</summary>
        /// <returns>The copied buffered values.</returns>
        private TSource[] TakeValues()
        {
            var batch = _values.ToArray();
            _values.Clear();
            return batch;
        }
    }
}
