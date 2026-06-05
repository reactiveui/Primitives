// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Signals;

/// <summary>
/// Create Signals functionality.
/// </summary>
public static partial class Signal
{
    /// <summary>
    /// Collects values into time-windowed batches using the default sequencer.
    /// </summary>
    /// <typeparam name="TSource">The source value type.</typeparam>
    /// <param name="source">The source signal.</param>
    /// <param name="timeSpan">The duration of each buffer window.</param>
    /// <returns>A signal that emits batches of source values.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static IObservable<IList<TSource>> Collect<TSource>(
        this IObservable<TSource> source,
        TimeSpan timeSpan) =>
        source.Collect(timeSpan, Sequencer.Default);

    /// <summary>
    /// Collects values into time-windowed batches.
    /// </summary>
    /// <typeparam name="TSource">The source value type.</typeparam>
    /// <param name="source">The source signal.</param>
    /// <param name="timeSpan">The duration of each buffer window.</param>
    /// <param name="sequencer">The sequencer used to schedule buffer flushes.</param>
    /// <returns>A signal that emits batches of source values.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="sequencer"/> is <see langword="null"/>.</exception>
    public static IObservable<IList<TSource>> Collect<TSource>(
        this IObservable<TSource> source,
        TimeSpan timeSpan,
        ISequencer sequencer)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (sequencer == null)
        {
            throw new ArgumentNullException(nameof(sequencer));
        }

        if (timeSpan <= TimeSpan.Zero)
        {
            return source.Map(static value => (IList<TSource>)[value]);
        }

        return Create<IList<TSource>>(observer =>
            new CollectCoordinator<TSource>(observer, timeSpan, sequencer).Subscribe(source));
    }

    /// <summary>
    /// Coordinates time-windowed buffering for a single subscription.
    /// </summary>
    /// <typeparam name="TSource">The source value type.</typeparam>
    private sealed class CollectCoordinator<TSource> : IDisposable
    {
        /// <summary>The downstream observer.</summary>
        private readonly IObserver<IList<TSource>> _observer;

        /// <summary>The duration of the buffer window.</summary>
        private readonly TimeSpan _timeSpan;

        /// <summary>The sequencer used to schedule flushes.</summary>
        private readonly ISequencer _sequencer;

        /// <summary>Serializes access to buffered values and terminal state.</summary>
        private readonly Lock _gate = new();

        /// <summary>Tracks the source subscription and scheduled flushes.</summary>
        private readonly MultipleDisposable _disposables = new();

        /// <summary>The values collected for the current window.</summary>
        private readonly List<TSource> _values = [];

        /// <summary>Whether a flush has already been scheduled for the current window.</summary>
        private bool _flushScheduled;

        /// <summary>Whether the source has terminated.</summary>
        private bool _stopped;

        /// <summary>
        /// Initializes a new instance of the <see cref="CollectCoordinator{TSource}"/> class.
        /// </summary>
        /// <param name="observer">The downstream observer.</param>
        /// <param name="timeSpan">The buffer window duration.</param>
        /// <param name="sequencer">The sequencer used to schedule flushes.</param>
        public CollectCoordinator(IObserver<IList<TSource>> observer, TimeSpan timeSpan, ISequencer sequencer)
        {
            _observer = observer;
            _timeSpan = timeSpan;
            _sequencer = sequencer;
        }

        /// <inheritdoc/>
        public void Dispose() => _disposables.Dispose();

        /// <summary>
        /// Subscribes to the source and returns the coordinator as the subscription.
        /// </summary>
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
        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Roslynator",
            "RCS1208:Reduce 'if' nesting",
            Justification = "Keeping the positive branch avoids a standalone defensive early-return line that is canceled by terminal disposal before it can execute.")]
        private void Flush()
        {
            var batch = TakeScheduledBatch();
            if (batch is { Length: > 0 })
            {
                _observer.OnNext(batch);
            }
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
