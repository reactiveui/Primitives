// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>Queues asynchronous projections with bounded concurrency and emits results in completion order.</summary>
/// <typeparam name = "TSource">The type of elements in the source sequence.</typeparam>
/// <typeparam name = "TResult">The type of the result of the asynchronous operation.</typeparam>
/// <param name = "source">The source observable.</param>
/// <param name = "selector">The asynchronous projection function.</param>
/// <param name = "maxConcurrency">The maximum number of concurrent operations.</param>
/// <remarks>Selector failure terminates immediately; source completion waits for queued projections to finish.</remarks>
public sealed class SelectAsyncConcurrentObservable<TSource, TResult>(IObservable<TSource> source, Func<TSource, Task<TResult>> selector, int maxConcurrency) : IObservable<TResult>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<TResult> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        InvalidOperationExceptionHelper.ThrowIfNull(selector);
        ArgumentExceptionHelper.ThrowIfNull(observer);
        SelectAsyncConcurrentSink sink = new(observer, selector, maxConcurrency);
        var sub = source.Subscribe(sink);
        return new DisposableBag(sub, sink);
    }

    /// <summary>Processes source values and owns the subscription state.</summary>
    /// <param name = "downstream">The downstream observer.</param>
    /// <param name = "selector">The asynchronous operation.</param>
    /// <param name = "maxConcurrency">The maximum concurrency.</param>
    /// <remarks>The gate only guards the queue and the counters; the selector and the observer run without it held.</remarks>
    internal sealed class SelectAsyncConcurrentSink(IObserver<TResult> downstream, Func<TSource, Task<TResult>> selector, int maxConcurrency) : IObserver<TSource>, IDisposable
    {
        /// <summary>Guards the queue and the counters; never held while the selector or the observer runs.</summary>
        private readonly Lock _gate = new();

        /// <summary>Queue of values to process.</summary>
        private readonly Queue<TSource> _queue = new();

        /// <summary>Serializes downstream deliveries.</summary>
        private SerializedDelivery<TResult> _delivery = new();

        /// <summary>The number of currently running async operations.</summary>
        private int _running;

        /// <summary>Whether the source has completed.</summary>
        private bool _done;

        /// <summary>Whether the sink has been disposed.</summary>
        private bool _disposed;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(TSource value) => _ = OnNextAsync(value);

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
            lock (_gate)
            {
                if (_done || _disposed)
                {
                    return;
                }

                _done = true;
                _ = _delivery.PostError(error);
            }

            Flush();
        }

        /// <inheritdoc/>
        public void OnCompleted()
        {
            lock (_gate)
            {
                if (_done || _disposed)
                {
                    return;
                }

                _done = true;
                if (_running == 0 && _queue.Count == 0)
                {
                    _ = _delivery.PostCompleted();
                }
            }

            Flush();
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            lock (_gate)
            {
                _disposed = true;
            }
        }

        /// <summary>Queues the value and starts as many operations as the concurrency limit allows.</summary>
        /// <param name = "value">The source value.</param>
        /// <returns>The processing task, or a completed task when no work starts.</returns>
        internal Task OnNextAsync(TSource value)
        {
            lock (_gate)
            {
                if (_done || _disposed)
                {
                    return Task.CompletedTask;
                }

                _queue.Enqueue(value);
            }

            return StartQueued();
        }

        /// <summary>Starts queued values up to the concurrency limit, invoking the selector outside the gate.</summary>
        /// <returns>The last operation started, or a completed task when the queue cannot advance.</returns>
        private Task StartQueued()
        {
            var processing = Task.CompletedTask;
            while (TryTakeNext(out var value))
            {
                processing = ProcessAsync(value);
            }

            return processing;
        }

        /// <summary>Claims the next queued value when a concurrency slot is free.</summary>
        /// <param name="value">The claimed value.</param>
        /// <returns><see langword="true"/> when a value was claimed.</returns>
        private bool TryTakeNext(out TSource value)
        {
            lock (_gate)
            {
                if (_running >= maxConcurrency || _queue.Count == 0)
                {
                    value = default!;
                    return false;
                }

                value = _queue.Dequeue();
                _running++;
                return true;
            }
        }

        /// <summary>Awaits the selector, emits its result, then starts queued work or completes the sequence.</summary>
        /// <param name = "value">The value to project.</param>
        /// <returns>A task representing the operation.</returns>
        private async Task ProcessAsync(TSource value)
        {
            try
            {
                var result = await selector(value).ConfigureAwait(false);
                lock (_gate)
                {
                    if (!_disposed)
                    {
                        _ = _delivery.Post(result);
                    }
                }

                Flush();
            }
            catch (Exception ex)
            {
                lock (_gate)
                {
                    if (!_disposed)
                    {
                        _done = true;
                        _ = _delivery.PostError(ex);
                    }
                }

                Flush();
            }
            finally
            {
                var startNext = false;
                lock (_gate)
                {
                    _running--;
                    if (!_disposed)
                    {
                        if (_done && _running == 0 && _queue.Count == 0)
                        {
                            _ = _delivery.PostCompleted();
                        }
                        else
                        {
                            startNext = true;
                        }
                    }
                }

                Flush();
                if (startNext)
                {
                    _ = StartQueued();
                }
            }
        }

        /// <summary>Delivers the queued notifications on the calling thread, or hands them to the thread already delivering.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void Flush() => _delivery.Flush(new PendingDrain(this));

        /// <summary>Delivers the queued notifications to the downstream observer.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void DrainPending() => _ = _delivery.DrainTo(downstream);

        /// <summary>Drains this sink's queued notifications for the delivery gate.</summary>
        /// <param name="Owner">The sink.</param>
        private readonly record struct PendingDrain(SelectAsyncConcurrentSink Owner) : IDrainTarget
        {
            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Drain() => Owner.DrainPending();
        }
    }
}
