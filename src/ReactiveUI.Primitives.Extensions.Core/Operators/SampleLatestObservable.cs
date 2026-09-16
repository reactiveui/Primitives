// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Disposables;

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
        var sourceSub = source.Subscribe(sink.SourceWitness);
        var triggerSub = trigger.Subscribe(sink.TriggerObserver);
        return new DisposableBag(sourceSub, triggerSub, sink);
    }

    /// <summary>Holds the latest source value and the terminal state shared by the source and trigger observers.</summary>
    /// <param name="downstream">The downstream observer.</param>
    /// <remarks>Samples and terminals are serialized, and no lock is held while the observer runs.</remarks>
    private sealed class SampleLatestSink(IObserver<T> downstream) : IDisposable
    {
        /// <summary>Guards the latest value and the terminal flag; never held while the observer runs.</summary>
        private readonly Lock _gate = new();

        /// <summary>Serializes downstream deliveries.</summary>
        private SerializedDelivery<T> _delivery = new();

        /// <summary>The latest value from the source.</summary>
        private T? _latest;

        /// <summary>Whether the source has produced a value.</summary>
        private bool _hasValue;

        /// <summary>Whether the sequence is done.</summary>
        private bool _done;

        /// <summary>Gets a new observer that records source values into this sink.</summary>
        public IObserver<T> SourceWitness => new SourceSampleWitness(this);

        /// <summary>Gets a new observer that samples this sink on each trigger notification.</summary>
        public IObserver<object> TriggerObserver => new TriggerSampleWitness(this);

        /// <inheritdoc/>
        public void Dispose()
        {
            lock (_gate)
            {
                _done = true;
            }
        }

        /// <summary>Records the latest source value.</summary>
        /// <param name="value">The source value.</param>
        private void OnSourceNext(T value)
        {
            lock (_gate)
            {
                _latest = value;
                _hasValue = true;
            }
        }

        /// <summary>Forwards a terminal error from either source.</summary>
        /// <param name="error">The terminal error.</param>
        private void OnAnyError(Exception error)
        {
            lock (_gate)
            {
                if (_done)
                {
                    return;
                }

                _done = true;
            }

            _delivery.OnError(error, new PendingDrain(this));
        }

        /// <summary>Completes the downstream observer once, ignoring later notifications.</summary>
        private void OnSourceCompleted()
        {
            lock (_gate)
            {
                if (_done)
                {
                    return;
                }

                _done = true;
            }

            _delivery.OnCompleted(new PendingDrain(this));
        }

        /// <summary>Samples and forwards the latest source value if one is available.</summary>
        private void OnTriggerNext()
        {
            T? value;
            bool shouldEmit;
            lock (_gate)
            {
                shouldEmit = _hasValue;
                value = _latest;
            }

            if (!shouldEmit)
            {
                return;
            }

            _delivery.OnNext(downstream, value!, new PendingDrain(this));
        }

        /// <summary>Delivers the queued notifications to the downstream observer.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void DrainPending() => _ = _delivery.DrainTo(downstream);

        /// <summary>Drains this sink's queued notifications for the delivery gate.</summary>
        /// <param name="Owner">The sink.</param>
        private readonly record struct PendingDrain(SampleLatestSink Owner) : IDrainTarget
        {
            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Drain() => Owner.DrainPending();
        }

        /// <summary>Observer that stores each source value in the sink.</summary>
        /// <param name="sink">The owning sink.</param>
        private sealed class SourceSampleWitness(SampleLatestSink sink) : IObserver<T>
        {
            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void OnNext(T value) => sink.OnSourceNext(value);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void OnError(Exception error) => sink.OnAnyError(error);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void OnCompleted() => sink.OnSourceCompleted();
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
}
