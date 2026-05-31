// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>
/// Projects each element to an asynchronous operation with limited concurrency.
/// </summary>
/// <typeparam name="TSource">The type of elements in the source sequence.</typeparam>
/// <typeparam name="TResult">The type of the result of the asynchronous operation.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="selector">The asynchronous projection function.</param>
/// <param name="maxConcurrency">The maximum number of concurrent operations.</param>
internal sealed class SelectAsyncConcurrentObservable<TSource, TResult>(
    IObservable<TSource> source,
    Func<TSource, Task<TResult>> selector,
    int maxConcurrency) : IObservable<TResult>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<TResult> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        InvalidOperationExceptionHelper.ThrowIfNull(selector);
        ArgumentExceptionHelper.ThrowIfNull(observer);

        var sink = new SelectAsyncConcurrentSink(observer, selector, maxConcurrency);
        var sub = source.Subscribe(sink);
        return new DisposableBag(sub, sink);
    }

    /// <summary>
    /// Sink that manages concurrent async projection.
    /// </summary>
    /// <param name="downstream">The downstream observer.</param>
    /// <param name="selector">The async selector.</param>
    /// <param name="maxConcurrency">The maximum concurrency.</param>
    private sealed class SelectAsyncConcurrentSink(
        IObserver<TResult> downstream,
        Func<TSource, Task<TResult>> selector,
        int maxConcurrency) : IObserver<TSource>, IDisposable
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
        public void OnNext(TSource value)
        {
            lock (_gate)
            {
                if (_done || _disposed)
                {
                    return;
                }

                _queue.Enqueue(value);
                TryProcessNext();
            }
        }

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

        /// <summary>Attempts to process the next value in the queue.</summary>
        private void TryProcessNext()
        {
            while (_running < maxConcurrency && _queue.Count > 0)
            {
                var value = _queue.Dequeue();
                _running++;
                _ = ProcessAsync(value);
            }
        }

        /// <summary>Processes the async operation.</summary>
        /// <param name="value">The value to project.</param>
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
                            TryProcessNext();
                        }
                    }
                }
            }
        }
    }
}
