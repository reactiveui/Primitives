// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>Emits the latest source value on each trigger after the source first emits.</summary>
/// <typeparam name="T">The type of elements in the source sequence.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="trigger">The trigger observable.</param>
/// <remarks>Source completion or either error terminates the result; trigger completion is ignored.</remarks>
public sealed class SampleLatestObservable<T>(
    IObservable<T> source,
    IObservable<object> trigger) : IObservable<T>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        InvalidOperationExceptionHelper.ThrowIfNull(trigger);
        ArgumentExceptionHelper.ThrowIfNull(observer);

        SampleLatestSink sink = new(observer);
        sink.AttachSource(source.Subscribe(sink));
        if (!sink.IsDone)
        {
            sink.AttachTrigger(trigger.Subscribe(new TriggerSampleWitness(sink)));
        }

        return sink;
    }

    /// <summary>Holds the latest source value and the terminal state shared by the source and trigger observers.</summary>
    /// <param name="downstream">The downstream observer.</param>
    /// <remarks>Samples and terminals are serialized, and no lock is held while the observer runs.</remarks>
    private sealed class SampleLatestSink(IObserver<T> downstream) : IObserver<T>, IDisposable
    {
        /// <summary>Guards the latest value and the terminal flag; never held while the observer runs.</summary>
        private readonly Lock _gate = new();

        /// <summary>Serializes downstream deliveries.</summary>
        private SerializedDelivery<T> _delivery = new();

        /// <summary>Subscription to the source sequence.</summary>
        private IDisposable? _sourceSub;

        /// <summary>Subscription to the trigger sequence.</summary>
        private IDisposable? _triggerSub;

        /// <summary>The latest value from the source.</summary>
        private T? _latest;

        /// <summary>Whether the source has produced a value.</summary>
        private bool _hasValue;

        /// <summary>Whether the sequence is done.</summary>
        private bool _done;

        /// <summary>Gets a value indicating whether the sink has terminated.</summary>
        internal bool IsDone
        {
            get
            {
                lock (_gate)
                {
                    return _done;
                }
            }
        }

        /// <inheritdoc/>
        public void OnNext(T value)
        {
            lock (_gate)
            {
                if (_done)
                {
                    return;
                }

                _latest = value;
                _hasValue = true;
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => OnAnyError(error);

        /// <inheritdoc/>
        public void OnCompleted()
        {
            lock (_gate)
            {
                if (_done)
                {
                    return;
                }

                _done = true;
                _latest = default;
            }

            DisposeUpstream();
            _delivery.OnCompleted(new PendingDrain(this));
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            lock (_gate)
            {
                _done = true;
                _latest = default;
            }

            DisposeUpstream();
        }

        /// <summary>Attaches the source subscription handle.</summary>
        /// <param name="subscription">The source subscription handle.</param>
        internal void AttachSource(IDisposable? subscription)
        {
            if (subscription is not null)
            {
                SinkSubscription.Set(ref _sourceSub, subscription);
            }
        }

        /// <summary>Attaches the trigger subscription handle.</summary>
        /// <param name="subscription">The trigger subscription handle.</param>
        internal void AttachTrigger(IDisposable? subscription)
        {
            if (subscription is not null)
            {
                SinkSubscription.Set(ref _triggerSub, subscription);
            }
        }

        /// <summary>Forwards a terminal error from either source.</summary>
        /// <param name="error">The terminal error.</param>
        internal void OnAnyError(Exception error)
        {
            lock (_gate)
            {
                if (_done)
                {
                    return;
                }

                _done = true;
                _latest = default;
            }

            DisposeUpstream();
            _delivery.OnError(error, new PendingDrain(this));
        }

        /// <summary>Samples and forwards the latest source value if one is available.</summary>
        internal void OnTriggerNext()
        {
            T? value;
            bool shouldEmit;
            lock (_gate)
            {
                shouldEmit = !_done && _hasValue;
                value = _latest;
            }

            if (!shouldEmit)
            {
                return;
            }

            try
            {
                _delivery.OnNext(downstream, value!, new PendingDrain(this));
            }
            catch
            {
                Dispose();
                throw;
            }
        }

        /// <summary>Disposes both source and trigger subscriptions.</summary>
        private void DisposeUpstream()
        {
            SinkSubscription.Dispose(ref _sourceSub);
            SinkSubscription.Dispose(ref _triggerSub);
        }

        /// <summary>Delivers the queued notifications to the downstream observer.</summary>
        private void DrainPending()
        {
            try
            {
                if (_delivery.DrainTo(downstream))
                {
                    DisposeUpstream();
                }
            }
            catch
            {
                Dispose();
                throw;
            }
        }

        /// <summary>Drains this sink's queued notifications for the delivery gate.</summary>
        /// <param name="Owner">The sink.</param>
        private readonly record struct PendingDrain(SampleLatestSink Owner) : IDrainTarget
        {
            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Drain() => Owner.DrainPending();
        }
    }

    /// <summary>Observer that asks the sink to emit its latest value on each trigger notification.</summary>
    /// <param name="sink">The owning sink.</param>
    private sealed class TriggerSampleWitness(SampleLatestSink sink) : IObserver<object>
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(object value) => sink.OnTriggerNext();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => sink.OnAnyError(error);

        /// <inheritdoc/>
        public void OnCompleted()
        {
            // A completed trigger leaves the sampled sequence running.
        }
    }
}
