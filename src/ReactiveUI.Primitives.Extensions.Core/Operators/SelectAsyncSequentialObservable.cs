// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>
/// Projects each element through an asynchronous selector one operation at a time, queueing values that arrive while an
/// operation runs so results keep source order. The first selector failure terminates the sequence, and the source's
/// completion waits for the queue to drain.
/// </summary>
/// <typeparam name = "TSource">The type of elements in the source sequence.</typeparam>
/// <typeparam name = "TResult">The type of the result of the asynchronous operation.</typeparam>
/// <param name = "source">The source observable.</param>
/// <param name = "selector">The asynchronous projection function.</param>
public sealed class SelectAsyncSequentialObservable<TSource, TResult>(IObservable<TSource> source, Func<TSource, Task<TResult>> selector) : IObservable<TResult>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<TResult> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        InvalidOperationExceptionHelper.ThrowIfNull(selector);
        ArgumentExceptionHelper.ThrowIfNull(observer);
        SelectAsyncSequentialSink sink = new(observer, selector);
        var sub = source.Subscribe(sink);
        return new DisposableBag(sub, sink);
    }

    /// <summary>Processes source values and owns the subscription state.</summary>
    /// <param name = "downstream">The downstream observer.</param>
    /// <param name = "selector">The asynchronous operation.</param>
    internal sealed class SelectAsyncSequentialSink(IObserver<TResult> downstream, Func<TSource, Task<TResult>> selector) : IObserver<TSource>, IDisposable
    {
        /// <summary>The gate for state access.</summary>
        private readonly Lock _gate = new();

        /// <summary>Queue of values to process.</summary>
        private readonly Queue<TSource> _queue = new();

        /// <summary>Whether an async operation is currently in progress.</summary>
        private bool _isProcessing;

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
                if (!_isProcessing)
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

        /// <summary>Queues the value and starts the drain loop when no operation is running.</summary>
        /// <param name = "value">The source value.</param>
        /// <returns>The processing task, or a completed task when no work starts.</returns>
        internal Task OnNextAsync(TSource value)
        {
            var processing = Task.CompletedTask;
            lock (_gate)
            {
                if (_done || _disposed)
                {
                    return Task.CompletedTask;
                }

                _queue.Enqueue(value);
                if (!_isProcessing)
                {
                    _isProcessing = true;
                    processing = ProcessNextAsync();
                }
            }

            return processing;
        }

        /// <summary>Projects queued values one at a time, completing the sequence when the queue empties after the source finishes.</summary>
        /// <returns>A task representing the operation.</returns>
        private async Task ProcessNextAsync()
        {
            while (true)
            {
                TSource value;
                lock (_gate)
                {
                    if (_disposed || _queue.Count == 0)
                    {
                        _isProcessing = false;
                        if (_done && !_disposed)
                        {
                            downstream.OnCompleted();
                        }

                        return;
                    }

                    value = _queue.Dequeue();
                }

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

                        _isProcessing = false;
                        return;
                    }
                }
            }
        }
    }
}
