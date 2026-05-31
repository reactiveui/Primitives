// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>
/// Buffers elements and emits them when the stream has been idle for a specified duration. Backs both
/// the <c>BufferUntilIdle</c> and <c>BufferUntilInactive</c> public operators.
/// </summary>
/// <typeparam name="T">The type of elements in the source sequence.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="idleTime">The duration of inactivity required to flush the buffer.</param>
/// <param name="scheduler">The scheduler to run the idle timer on.</param>
internal sealed class BufferUntilIdleObservable<T>(
    IObservable<T> source,
    TimeSpan idleTime,
    ISequencer scheduler) : IObservable<IList<T>>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<IList<T>> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        InvalidOperationExceptionHelper.ThrowIfNull(scheduler);
        ArgumentExceptionHelper.ThrowIfNull(observer);

        var sink = new BufferUntilIdleSink(observer, idleTime, scheduler);
        var subscription = source.Subscribe(sink);
        return new DisposableBag(subscription, sink);
    }

    /// <summary>
    /// Sink that manages the buffer and idle timer. Composes <see cref="TimerSinkState{T}"/> for
    /// the shared gate / timer / done-flag plumbing so this class only carries the buffer logic.
    /// </summary>
    /// <param name="downstream">The downstream observer.</param>
    /// <param name="idleTime">The idle time period.</param>
    /// <param name="scheduler">The scheduler.</param>
    private sealed class BufferUntilIdleSink(
        IObserver<IList<T>> downstream,
        TimeSpan idleTime,
        ISequencer scheduler) : IObserver<T>, IDisposable
    {
        /// <summary>Shared gate / timer / done-flag plumbing.</summary>
        private readonly TimerSinkState<IList<T>> _state = new(downstream);

        /// <summary>The current buffer of elements.</summary>
        private List<T> _buffer = [];

        /// <inheritdoc/>
        public void OnNext(T value)
        {
            lock (_state.Gate)
            {
                if (_state.Done)
                {
                    return;
                }

                _buffer.Add(value);
                ScheduleFlush();
            }
        }

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
            Flush();
            _state.HandleError(error);
        }

        /// <inheritdoc/>
        public void OnCompleted()
        {
            Flush();
            _state.HandleCompleted();
        }

        /// <inheritdoc/>
        public void Dispose() => _state.HandleDispose();

        /// <summary>Schedules a flush after the idle period.</summary>
        private void ScheduleFlush() => _state.Timer.Disposable = scheduler.Schedule(idleTime, Flush);

        /// <summary>Flushes the current buffer to the downstream observer.</summary>
        private void Flush()
        {
            List<T>? toEmit = null;
            lock (_state.Gate)
            {
                if (_buffer.Count > 0)
                {
                    toEmit = _buffer;
                    _buffer = [];
                }
            }

            if (toEmit is null)
            {
                return;
            }

            downstream.OnNext(toEmit);
        }
    }
}
