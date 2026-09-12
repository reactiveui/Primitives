// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>
/// Emits the source's latest value each time <paramref name="trigger"/> fires, repeating it when no newer value has
/// arrived and emitting nothing until the source produces its first. An error from either sequence terminates the
/// result; the source's completion completes it, while the trigger's completion is ignored.
/// </summary>
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

    /// <summary>Holds the latest source value and the terminal state shared by the source and trigger observers.</summary>
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
                downstream.OnError(error);
            }
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
