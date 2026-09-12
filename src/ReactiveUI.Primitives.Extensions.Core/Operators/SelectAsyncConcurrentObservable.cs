// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>
/// Projects each element through an asynchronous selector with at most <paramref name="maxConcurrency"/> operations in
/// flight and queues the rest, so results arrive in completion order rather than source order. The first selector
/// failure terminates the sequence, and the source's completion is held back until the queue drains.
/// </summary>
/// <typeparam name = "TSource">The type of elements in the source sequence.</typeparam>
/// <typeparam name = "TResult">The type of the result of the asynchronous operation.</typeparam>
/// <param name = "source">The source observable.</param>
/// <param name = "selector">The asynchronous projection function.</param>
/// <param name = "maxConcurrency">The maximum number of concurrent operations.</param>
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
    internal sealed class SelectAsyncConcurrentSink(IObserver<TResult> downstream, Func<TSource, Task<TResult>> selector, int maxConcurrency) : IObserver<TSource>, IDisposable
    {
        /// <summary>The gate for state access.</summary>
        private readonly Lock _gate = new();

        /// <summary>Queue of values to process.</summary>
        private readonly Queue<TSource> _queue = new();

        /// <summary>The number of currently running async operations.</summary>
        private int _running;

        /// <summary>Whether the source has completed.</summary>
        private bool _done;

        /// <summary>Whether the sink has been disposed.</summary>
        private bool _disposed;

        /// <inheritdoc/>
        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
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
                downstream.OnError(error);
            }
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
                    downstream.OnCompleted();
                }
            }
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
            Task processing;
            lock (_gate)
            {
                if (_done || _disposed)
                {
                    return Task.CompletedTask;
                }

                _queue.Enqueue(value);
                processing = TryProcessNext();
            }

            return processing;
        }

        /// <summary>Attempts to process the next value in the queue.</summary>
        /// <returns>The last operation started, or a completed task when the queue cannot advance.</returns>
        private Task TryProcessNext()
        {
            var processing = Task.CompletedTask;
            while (_running < maxConcurrency && _queue.Count > 0)
            {
                var value = _queue.Dequeue();
                _running++;
                processing = ProcessAsync(value);
            }

            return processing;
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
                        downstream.OnNext(result);
                    }
                }
            }
            catch (Exception ex)
            {
                lock (_gate)
                {
                    if (!_disposed)
                    {
                        _done = true;
                        downstream.OnError(ex);
                    }
                }
            }
            finally
            {
                lock (_gate)
                {
                    _running--;
                    if (!_disposed)
                    {
                        if (_done && _running == 0 && _queue.Count == 0)
                        {
                            downstream.OnCompleted();
                        }
                        else
                        {
                            _ = TryProcessNext();
                        }
                    }
                }
            }
        }
    }
}
