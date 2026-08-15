// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>Samples the latest value from the source observable whenever a trigger observable emits.</summary>
/// <typeparam name="T">The type of elements in the source sequence.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="trigger">The trigger observable.</param>
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

    /// <summary>Sinks the source observable and samples it based on the trigger.</summary>
    /// <param name="downstream">The downstream observer.</param>
    private sealed class SampleLatestSink(IObserver<T> downstream) : IDisposable
    {
        /// <summary>The gate for synchronization.</summary>
        private readonly Lock _gate = new();

        /// <summary>The latest value from the source.</summary>
        private T? _latest;

        /// <summary>Whether the source has produced a value.</summary>
        private bool _hasValue;

        /// <summary>Whether the sequence is done.</summary>
        private bool _done;

        /// <summary>Gets the source observer.</summary>
        public IObserver<T> SourceWitness => new SourceSampleWitness(this);

        /// <summary>Gets the trigger observer.</summary>
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
                downstream.OnError(error);
            }
        }

        /// <summary>Forwards source completion.</summary>
        private void OnSourceCompleted()
        {
            lock (_gate)
            {
                if (_done)
                {
                    return;
                }

                _done = true;
                downstream.OnCompleted();
            }
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

            downstream.OnNext(value!);
        }

        /// <summary>Observer for source values.</summary>
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

        /// <summary>Observer for trigger values.</summary>
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
                /* Trigger completion does not affect sample */
            }
        }
    }
}
