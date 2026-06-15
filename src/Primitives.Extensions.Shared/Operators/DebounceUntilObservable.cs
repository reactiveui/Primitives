// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Extensions.Reactive.Operators;
#else
namespace ReactiveUI.Primitives.Extensions.Operators;
#endif

/// <summary>Debounces a sequence until a condition becomes true for an element.</summary>
/// <typeparam name="T">The type of elements in the source sequence.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="debounce">The debounce duration.</param>
/// <param name="condition">The condition to determine if an element should be emitted immediately or debounced.</param>
/// <param name="scheduler">The scheduler to use for timing.</param>
internal sealed class DebounceUntilObservable<T>(
    IObservable<T> source,
    TimeSpan debounce,
    Func<T, bool> condition,
    ISequencer scheduler) : IObservable<T>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        InvalidOperationExceptionHelper.ThrowIfNull(condition);
        InvalidOperationExceptionHelper.ThrowIfNull(scheduler);
        ArgumentExceptionHelper.ThrowIfNull(observer);

        DebounceUntilSink sink = new(observer, debounce, condition, scheduler);
        var subscription = source.Subscribe(sink);
        return new DisposableBag(subscription, sink);
    }

    /// <summary>
    /// Sink for the debounce-until observable. Composes <see cref="TimerSinkState{T}"/> for the
    /// shared gate / timer / done-flag plumbing so this class only carries the OnNext logic.
    /// </summary>
    /// <param name="downstream">The downstream observer.</param>
    /// <param name="debounce">The debounce duration.</param>
    /// <param name="condition">The condition.</param>
    /// <param name="scheduler">The scheduler.</param>
    private sealed class DebounceUntilSink(
        IObserver<T> downstream,
        TimeSpan debounce,
        Func<T, bool> condition,
        ISequencer scheduler) : IObserver<T>, IDisposable
    {
        /// <summary>The gate protecting state transitions and downstream notification.</summary>
        private readonly Lock _gate = new();

        /// <summary>Shared timer / done-flag plumbing.</summary>
        private readonly TimerSinkState<T> _state = new(downstream);

        /// <inheritdoc/>
        public void OnNext(T value)
        {
            lock (_gate)
            {
                if (_state.Done)
                {
                    return;
                }

                if (condition(value))
                {
                    _state.Timer.Disposable = null;
                    downstream.OnNext(value);
                }
                else
                {
                    _state.Timer.Disposable = scheduler.Schedule(
                        (Sink: this, Value: value),
                        debounce,
                        static state => state.Sink.EmitDebounced(state.Value));
                }
            }
        }

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
            lock (_gate)
            {
                _state.HandleErrorLocked(error);
            }
        }

        /// <inheritdoc/>
        public void OnCompleted()
        {
            lock (_gate)
            {
                _state.HandleCompletedLocked();
            }
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            lock (_gate)
            {
                _state.HandleDisposeLocked();
            }
        }

        /// <summary>Emits a debounced value when the sink is still active.</summary>
        /// <param name="value">The debounced value.</param>
        private void EmitDebounced(T value)
        {
            lock (_gate)
            {
                if (!_state.Done)
                {
                    downstream.OnNext(value);
                }
            }
        }
    }
}
