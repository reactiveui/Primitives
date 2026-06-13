// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>Projects each element to an asynchronous operation, preserving order and handling sequential execution.</summary>
/// <typeparam name="TSource">The type of elements in the source sequence.</typeparam>
/// <typeparam name="TResult">The type of the result of the asynchronous operation.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="selector">The asynchronous projection function.</param>
internal sealed class SelectAsyncSequentialObservable<TSource, TResult>(
    IObservable<TSource> source,
    Func<TSource, Task<TResult>> selector) : IObservable<TResult>
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

    /// <summary>Sink that manages sequential async projection.</summary>
    /// <param name="downstream">The downstream observer.</param>
    /// <param name="selector">The async selector.</param>
    private sealed class SelectAsyncSequentialSink(
        IObserver<TResult> downstream,
        Func<TSource, Task<TResult>> selector) : IObserver<TSource>, IDisposable
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
        public void OnNext(TSource value)
        {
            lock (_gate)
            {
                if (_done || _disposed)
                {
                    return;
                }

                _queue.Enqueue(value);
                if (!_isProcessing)
                {
                    _isProcessing = true;
                    _ = ProcessNextAsync();
                }
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

        /// <summary>Processes the next value in the queue.</summary>
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
