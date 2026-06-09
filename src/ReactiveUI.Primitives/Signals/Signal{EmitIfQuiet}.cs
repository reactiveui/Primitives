// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Signals;

/// <summary>Create Signals functionality.</summary>
public static partial class Signal
{
    /// <summary>Coordinates throttled emission for a single subscription.</summary>
    /// <typeparam name="TSource">The source value type.</typeparam>
    internal sealed class EmitIfQuietCoordinator<TSource> : IDisposable
    {
        /// <summary>The downstream observer.</summary>
        private readonly IObserver<TSource> _observer;

        /// <summary>The quiet period before the latest value is emitted.</summary>
        private readonly TimeSpan _dueTime;

        /// <summary>The sequencer used to schedule delayed emissions.</summary>
        private readonly ISequencer _sequencer;

        /// <summary>Serializes access to latest value and terminal state.</summary>
        private readonly Lock _gate = new();

        /// <summary>Tracks the source subscription and scheduled delayed emissions.</summary>
        private readonly MultipleDisposable _disposables = [];

        /// <summary>The latest observed value.</summary>
        private TSource? _latest;

        /// <summary>Monotonic version used to suppress obsolete scheduled emissions.</summary>
        private long _version;

        /// <summary>Whether a latest value is pending emission.</summary>
        private bool _hasValue;

        /// <summary>Whether the source has terminated.</summary>
        private bool _stopped;

        /// <summary>Initializes a new instance of the <see cref="EmitIfQuietCoordinator{TSource}"/> class.</summary>
        /// <param name="observer">The downstream observer.</param>
        /// <param name="dueTime">The quiet period before emitting the latest value.</param>
        /// <param name="sequencer">The sequencer used to schedule delayed emissions.</param>
        public EmitIfQuietCoordinator(IObserver<TSource> observer, TimeSpan dueTime, ISequencer sequencer)
        {
            _observer = observer;
            _dueTime = dueTime;
            _sequencer = sequencer;
        }

        /// <inheritdoc/>
        public void Dispose() => _disposables.Dispose();

        /// <summary>Subscribes to the source and returns the coordinator as the subscription.</summary>
        /// <param name="source">The source signal.</param>
        /// <returns>The subscription that tears down source and scheduled throttle work.</returns>
        internal EmitIfQuietCoordinator<TSource> Subscribe(IObservable<TSource> source)
        {
            _disposables.Add(source.Subscribe(OnNext, OnError, OnCompleted));
            return this;
        }

        /// <summary>Records a latest value and schedules its delayed emission.</summary>
        /// <param name="value">The source value.</param>
        private void OnNext(TSource value)
        {
            if (!TryRecord(value, out var currentVersion))
            {
                return;
            }

            _disposables.Add(_sequencer.Schedule(_dueTime, () => EmitIfLatest(currentVersion)));
        }

        /// <summary>Forwards a terminal error after marking the coordinator stopped.</summary>
        /// <param name="error">The source error.</param>
        private void OnError(Exception error)
        {
            MarkStopped();
            _observer.OnError(error);
        }

        /// <summary>Emits a pending latest value and forwards completion.</summary>
        private void OnCompleted()
        {
            if (CompleteAndTakeLatest(out var value))
            {
                _observer.OnNext(value!);
            }

            _observer.OnCompleted();
        }

        /// <summary>Emits the latest value if the scheduled version is still current.</summary>
        /// <param name="scheduledVersion">The version captured when the emission was scheduled.</param>
        private void EmitIfLatest(long scheduledVersion)
        {
            if (!TryTakeLatest(scheduledVersion, out var value))
            {
                return;
            }

            _observer.OnNext(value!);
        }

        /// <summary>Records a latest value and returns its version.</summary>
        /// <param name="value">The source value.</param>
        /// <param name="currentVersion">The version assigned to the value.</param>
        /// <returns><see langword="true"/> when delayed emission should be scheduled.</returns>
        private bool TryRecord(TSource value, out long currentVersion)
        {
            lock (_gate)
            {
                if (_stopped)
                {
                    currentVersion = default;
                    return false;
                }

                _latest = value;
                _hasValue = true;
                currentVersion = ++_version;
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

        /// <summary>Returns and clears the pending value when the scheduled version is current.</summary>
        /// <param name="scheduledVersion">The version captured when the emission was scheduled.</param>
        /// <param name="value">The value to emit.</param>
        /// <returns><see langword="true"/> when a value should be emitted.</returns>
        private bool TryTakeLatest(long scheduledVersion, out TSource? value)
        {
            lock (_gate)
            {
                if (_stopped || !_hasValue || scheduledVersion != _version)
                {
                    value = default;
                    return false;
                }

                value = _latest;
                _hasValue = false;
                return true;
            }
        }

        /// <summary>Stops the coordinator and returns any pending latest value.</summary>
        /// <param name="value">The pending value to emit.</param>
        /// <returns><see langword="true"/> when a value should be emitted.</returns>
        private bool CompleteAndTakeLatest(out TSource? value)
        {
            lock (_gate)
            {
                _stopped = true;
                if (!_hasValue)
                {
                    value = default;
                    return false;
                }

                value = _latest;
                _hasValue = false;
                return true;
            }
        }
    }
}
