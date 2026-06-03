// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Core;

namespace ReactiveUI.Primitives.Signals;

/// <summary>
/// Annotates each source value with the scheduler timestamp at which it was observed, holding the sequencer directly
/// so no per-subscription closure is allocated.
/// </summary>
/// <typeparam name="T">The value type.</typeparam>
internal sealed class TimestampSignal<T> : IRequireCurrentThread<Moment<T>>
{
    /// <summary>The source sequence.</summary>
    private readonly IObservable<T> _source;

    /// <summary>The sequencer that supplies timestamps.</summary>
    private readonly ISequencer _scheduler;

    /// <summary>Initializes a new instance of the <see cref="TimestampSignal{T}"/> class.</summary>
    /// <param name="source">The source sequence.</param>
    /// <param name="scheduler">The sequencer that supplies timestamps.</param>
    internal TimestampSignal(IObservable<T> source, ISequencer scheduler)
    {
        _source = source;
        _scheduler = scheduler;
    }

    /// <summary>
    /// Determines whether the sink must subscribe on the current thread.
    /// </summary>
    /// <returns><see langword="true"/> when the source requires current-thread subscription.</returns>
    public bool IsRequiredSubscribeOnCurrentThread() =>
        _source is IRequireCurrentThread<T> currentThread && currentThread.IsRequiredSubscribeOnCurrentThread();

    /// <summary>
    /// Subscribes the observer to the timestamped sequence.
    /// </summary>
    /// <param name="observer">The downstream observer.</param>
    /// <returns>The subscription handle.</returns>
    public IDisposable Subscribe(IObserver<Moment<T>> observer)
    {
        if (observer == null)
        {
            throw new ArgumentNullException(nameof(observer));
        }

        return _source.Subscribe(new TimestampObserver(observer, _scheduler));
    }

    /// <summary>Stamps each source value with the current scheduler time.</summary>
    private sealed class TimestampObserver : IObserver<T>
    {
        /// <summary>The downstream observer.</summary>
        private readonly IObserver<Moment<T>> _observer;

        /// <summary>The sequencer that supplies timestamps.</summary>
        private readonly ISequencer _scheduler;

        /// <summary>Whether a terminal notification has been forwarded.</summary>
        private bool _stopped;

        /// <summary>Initializes a new instance of the <see cref="TimestampObserver"/> class.</summary>
        /// <param name="observer">The downstream observer.</param>
        /// <param name="scheduler">The sequencer that supplies timestamps.</param>
        public TimestampObserver(IObserver<Moment<T>> observer, ISequencer scheduler)
        {
            _observer = observer;
            _scheduler = scheduler;
        }

        /// <summary>Forwards completion downstream.</summary>
        public void OnCompleted()
        {
            if (_stopped)
            {
                return;
            }

            _stopped = true;
            _observer.OnCompleted();
        }

        /// <summary>Forwards an error downstream.</summary>
        /// <param name="error">The error value.</param>
        public void OnError(Exception error)
        {
            if (_stopped)
            {
                return;
            }

            _stopped = true;
            _observer.OnError(error);
        }

        /// <summary>Stamps and forwards a source value.</summary>
        /// <param name="value">The source value.</param>
        public void OnNext(T value)
        {
            if (_stopped)
            {
                return;
            }

            _observer.OnNext(new Moment<T>(value, _scheduler.Now));
        }
    }
}
