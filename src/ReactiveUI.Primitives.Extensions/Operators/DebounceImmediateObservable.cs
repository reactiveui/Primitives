// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>Debounces a sequence but emits the first value immediately.</summary>
/// <typeparam name="T">The type of elements in the source sequence.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="dueTime">The debounce duration.</param>
/// <param name="scheduler">The scheduler to use for timing.</param>
internal sealed class DebounceImmediateObservable<T>(
    IObservable<T> source,
    TimeSpan dueTime,
    ISequencer scheduler) : IObservable<T>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        InvalidOperationExceptionHelper.ThrowIfNull(scheduler);
        ArgumentExceptionHelper.ThrowIfNull(observer);

        var sink = new DebounceImmediateSink(observer, dueTime, scheduler);
        var subscription = source.Subscribe(sink);
        return new DisposableBag(subscription, sink);
    }

    /// <summary>Sink for the debounce immediate observable.</summary>
    /// <param name="downstream">The downstream observer.</param>
    /// <param name="dueTime">The debounce duration.</param>
    /// <param name="scheduler">The scheduler to use for timing.</param>
    private sealed class DebounceImmediateSink(
        IObserver<T> downstream,
        TimeSpan dueTime,
        ISequencer scheduler) : IObserver<T>, IDisposable
    {
        /// <summary>The gate for thread safety.</summary>
        private readonly Lock _gate = new();

        /// <summary>The timer for debouncing.</summary>
        private readonly SwapDisposable _timer = new();

        /// <summary>Whether the first value has been emitted.</summary>
        private bool _isFirst = true;

        /// <summary>The last value received.</summary>
        private T? _lastValue;

        /// <summary>Whether a value is pending.</summary>
        private bool _hasValue;

        /// <summary>Whether the sequence is done.</summary>
        private bool _done;

        /// <inheritdoc/>
        public void OnNext(T value)
        {
            lock (_gate)
            {
                if (_done)
                {
                    return;
                }

                if (_isFirst)
                {
                    _isFirst = false;
                    downstream.OnNext(value);
                    return;
                }

                _lastValue = value;
                _hasValue = true;
                _timer.Disposable = scheduler.Schedule(dueTime, Emit);
            }
        }

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
            lock (_gate)
            {
                if (_done)
                {
                    return;
                }

                _done = true;
                Emit();
                _timer.Dispose();
                downstream.OnError(error);
            }
        }

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
                Emit();
                _timer.Dispose();
                downstream.OnCompleted();
            }
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            lock (_gate)
            {
                _done = true;
                _timer.Dispose();
            }
        }

        /// <summary>Emits the last value if any.</summary>
        private void Emit()
        {
            T? toEmit;
            bool shouldEmit;

            lock (_gate)
            {
                shouldEmit = _hasValue;
                toEmit = _lastValue;
                _hasValue = false;
            }

            if (!shouldEmit)
            {
                return;
            }

            downstream.OnNext(toEmit!);
        }
    }
}
