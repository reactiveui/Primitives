// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Disposables;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Extensions.Reactive.Operators;
#else
namespace ReactiveUI.Primitives.Extensions.Operators;
#endif

/// <summary>Buffers elements until the source has been idle for the specified duration.</summary>
/// <typeparam name="T">The type of elements in the source sequence.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="idleTime">The duration of inactivity required to flush the buffer.</param>
/// <param name="scheduler">The scheduler to run the idle timer on.</param>
public sealed class BufferUntilIdleObservable<T>(
    IObservable<T> source,
    TimeSpan idleTime,
    ISequencer scheduler) : IObservable<IList<T>>
{
    /// <summary>Initializes a new instance of the <see cref="BufferUntilIdleObservable{T}"/> class using <see cref="Sequencer.Default"/>.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="idleTime">The duration of inactivity required to flush the buffer.</param>
    public BufferUntilIdleObservable(IObservable<T> source, TimeSpan idleTime)
        : this(source, idleTime, Sequencer.Default)
    {
    }

    /// <summary>Creates a buffer-until-idle sequence, defaulting a <see langword="null"/> <paramref name="scheduler"/> to <see cref="Sequencer.Default"/>.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="idleTime">The duration of inactivity required to flush the buffer.</param>
    /// <param name="scheduler">The optional scheduler to run the idle timer on.</param>
    /// <returns>A sequence of buffered lists.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static BufferUntilIdleObservable<T> Create(
        IObservable<T> source,
        TimeSpan idleTime,
        ISequencer? scheduler) =>
        new(source, idleTime, scheduler ?? Sequencer.Default);

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<IList<T>> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        InvalidOperationExceptionHelper.ThrowIfNull(scheduler);
        ArgumentExceptionHelper.ThrowIfNull(observer);

        BufferUntilIdleSink sink = new(observer, idleTime, scheduler);
        sink.SetSubscription(source.Subscribe(sink));
        return sink;
    }

    /// <summary>Sink that accumulates elements and emits the buffer once the idle timer elapses.</summary>
    /// <param name="downstream">The downstream observer.</param>
    /// <param name="idleTime">The idle time period.</param>
    /// <param name="scheduler">The sequencer that times the idle period.</param>
    /// <remarks>Buffers and terminals are queued in order under the gate and delivered after it is released.</remarks>
    private sealed class BufferUntilIdleSink(
        IObserver<IList<T>> downstream,
        TimeSpan idleTime,
        ISequencer scheduler) : IObserver<T>, IDisposable
    {
        /// <summary>Guards the buffer, the terminal state and the order notifications are queued in; never held while the observer runs.</summary>
        private readonly Lock _gate = new();

        /// <summary>The timer slot, terminal state and serialized delivery shared with the operator's handlers.</summary>
        private readonly TimerSinkState<IList<T>> _state = new(downstream);

        /// <summary>The upstream subscription handle.</summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Usage",
            "CA2213:Disposable fields should be disposed",
            Justification = "Disposed atomically via SinkSubscription.Dispose.")]
        private IDisposable? _subscription;

        /// <summary>The current buffer of elements.</summary>
        private List<T> _buffer = [];

        /// <summary>Assigns the upstream subscription.</summary>
        /// <param name="subscription">The upstream subscription.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetSubscription(IDisposable subscription) => SinkSubscription.Set(ref _subscription, subscription);

        /// <inheritdoc/>
        public void OnNext(T value)
        {
            lock (_gate)
            {
                if (_state.Done)
                {
                    return;
                }

                _buffer.Add(value);
            }

            ScheduleFlush();
        }

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
            lock (_gate)
            {
                QueueBufferLocked();
                _ = _state.QueueErrorLocked(error);
            }

            _state.Flush();
            SinkSubscription.Dispose(ref _subscription);
        }

        /// <inheritdoc/>
        public void OnCompleted()
        {
            lock (_gate)
            {
                QueueBufferLocked();
                _ = _state.QueueCompletedLocked();
            }

            _state.Flush();
            SinkSubscription.Dispose(ref _subscription);
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            lock (_gate)
            {
                _state.HandleDisposeLocked();
                _buffer = [];
            }

            SinkSubscription.Dispose(ref _subscription);
        }

        /// <summary>Replaces any pending flush with one scheduled a further idle period ahead.</summary>
        private void ScheduleFlush() =>
            _state.Timer.Disposable = scheduler.Schedule(this, idleTime, static (_, self) => self.EmitBuffer());

        /// <summary>Queues the current buffer and delivers it to the downstream observer.</summary>
        /// <returns>An empty disposable.</returns>
        private EmptyDisposable EmitBuffer()
        {
            lock (_gate)
            {
                QueueBufferLocked();
            }

            _state.Flush();
            return EmptyDisposable.Instance;
        }

        /// <summary>Queues the current buffer when it holds elements and starts a new one, while the caller holds the gate.</summary>
        private void QueueBufferLocked()
        {
            if (_buffer.Count == 0)
            {
                return;
            }

            _ = _state.QueueLocked(_buffer);
            _buffer = [];
        }
    }
}
