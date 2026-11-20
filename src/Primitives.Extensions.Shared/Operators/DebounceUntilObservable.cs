// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Extensions.Reactive.Operators;
#else
namespace ReactiveUI.Primitives.Extensions.Operators;
#endif

/// <summary>Emits a value inline when <paramref name="condition"/> holds for it, otherwise after <paramref name="debounce"/> of quiet.</summary>
/// <typeparam name="T">The type of elements in the source sequence.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="debounce">The debounce duration.</param>
/// <param name="condition">The condition to determine if an element should be emitted immediately or debounced.</param>
/// <param name="scheduler">The scheduler to use for timing.</param>
public sealed class DebounceUntilObservable<T>(
    IObservable<T> source,
    TimeSpan debounce,
    Func<T, bool> condition,
    ISequencer scheduler) : IObservable<T>
{
    /// <summary>Initializes a new instance of the <see cref="DebounceUntilObservable{T}"/> class using <see cref="Sequencer.Default"/>.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="debounce">The debounce duration.</param>
    /// <param name="condition">The condition to determine if an element should be emitted immediately or debounced.</param>
    public DebounceUntilObservable(IObservable<T> source, TimeSpan debounce, Func<T, bool> condition)
        : this(source, debounce, condition, Sequencer.Default)
    {
    }

    /// <summary>Creates a debounce-until sequence, defaulting a <see langword="null"/> <paramref name="scheduler"/> to <see cref="Sequencer.Default"/>.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="debounce">The debounce duration.</param>
    /// <param name="condition">The condition to determine if an element should be emitted immediately or debounced.</param>
    /// <param name="scheduler">Optional scheduler to use for timing.</param>
    /// <returns>A debounce-until sequence.</returns>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    public static DebounceUntilObservable<T> Create(
        IObservable<T> source,
        TimeSpan debounce,
        Func<T, bool> condition,
        ISequencer? scheduler) =>
        new(source, debounce, condition, scheduler ?? Sequencer.Default);

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        InvalidOperationExceptionHelper.ThrowIfNull(condition);
        InvalidOperationExceptionHelper.ThrowIfNull(scheduler);
        ArgumentExceptionHelper.ThrowIfNull(observer);

        DebounceUntilSink sink = new(observer, debounce, condition, scheduler);
        sink.Attach(source.Subscribe(sink));
        return sink;
    }

    /// <summary>Sink that forwards a value inline when the condition holds and otherwise after the debounce window.</summary>
    /// <param name="downstream">The downstream observer.</param>
    /// <param name="debounce">The debounce duration.</param>
    /// <param name="condition">The condition.</param>
    /// <param name="scheduler">The sequencer that times the debounce window.</param>
    /// <remarks>
    /// Notifications are queued in order under the gate and delivered after it is released, so neither the observer nor the
    /// condition runs while the gate is held.
    /// </remarks>
    private sealed class DebounceUntilSink(
        IObserver<T> downstream,
        TimeSpan debounce,
        Func<T, bool> condition,
        ISequencer scheduler) : IObserver<T>, IDisposable
    {
        /// <summary>Guards the terminal state and the order notifications are queued in.</summary>
        private readonly Lock _gate = new();

        /// <summary>The timer slot, terminal state and serialized delivery shared with the operator's handlers.</summary>
        private readonly TimerSinkState<T> _state = new(downstream);

        /// <summary>Upstream subscription handle.</summary>
        private IDisposable? _subscription;

        /// <inheritdoc/>
        public void OnNext(T value)
        {
            if (!condition(value))
            {
                _state.Timer.Disposable = scheduler.Schedule(
                    (Sink: this, Value: value),
                    debounce,
                    static state => state.Sink.EmitDebounced(state.Value));
                return;
            }

            lock (_gate)
            {
                _state.Timer.Disposable = null;
                _ = _state.QueueLocked(value);
            }

            _state.Flush();
        }

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
            lock (_gate)
            {
                _ = _state.QueueErrorLocked(error);
            }

            SinkSubscription.Dispose(ref _subscription);
            _state.Flush();
        }

        /// <inheritdoc/>
        public void OnCompleted()
        {
            lock (_gate)
            {
                _ = _state.QueueCompletedLocked();
            }

            SinkSubscription.Dispose(ref _subscription);
            _state.Flush();
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            lock (_gate)
            {
                _state.HandleDisposeLocked();
            }

            SinkSubscription.Dispose(ref _subscription);
        }

        /// <summary>Attaches the upstream subscription handle.</summary>
        /// <param name="subscription">The upstream subscription handle.</param>
        internal void Attach(IDisposable? subscription)
        {
            if (subscription is not null)
            {
                SinkSubscription.Set(ref _subscription, subscription);
            }
        }

        /// <summary>Queues and delivers the debounced value unless the sink has terminated.</summary>
        /// <param name="value">The debounced value.</param>
        private void EmitDebounced(T value)
        {
            lock (_gate)
            {
                _ = _state.QueueLocked(value);
            }

            _state.Flush();
        }
    }
}
