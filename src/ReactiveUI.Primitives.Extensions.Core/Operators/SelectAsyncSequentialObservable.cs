// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>Queues asynchronous projections and emits results in source order.</summary>
/// <typeparam name = "TSource">The type of elements in the source sequence.</typeparam>
/// <typeparam name = "TResult">The type of the result of the asynchronous operation.</typeparam>
/// <remarks>Selector failure terminates immediately; source completion waits for queued projections to finish.</remarks>
[System.Diagnostics.DebuggerDisplay("SelectAsyncSequential<{typeof(TSource).Name,nq},{typeof(TResult).Name,nq}>")]
public sealed class SelectAsyncSequentialObservable<TSource, TResult> : IObservable<TResult>
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<TSource> _source;

    /// <summary>The asynchronous projection, given a token that fires when the subscription is disposed.</summary>
    private readonly Func<TSource, CancellationToken, Task<TResult>>? _selector;

    /// <summary>Initializes a new instance of the <see cref="SelectAsyncSequentialObservable{TSource, TResult}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="selector">The asynchronous projection function.</param>
    public SelectAsyncSequentialObservable(IObservable<TSource> source, Func<TSource, Task<TResult>> selector)
    {
        _source = source;
        _selector = selector is null ? null : (value, _) => selector(value);
    }

    /// <summary>Initializes a new instance of the <see cref="SelectAsyncSequentialObservable{TSource, TResult}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="selector">The asynchronous projection function. Its token fires when the subscription is disposed.</param>
    public SelectAsyncSequentialObservable(IObservable<TSource> source, Func<TSource, CancellationToken, Task<TResult>> selector)
    {
        _source = source;
        _selector = selector;
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<TResult> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(_source);
        InvalidOperationExceptionHelper.ThrowIfNull(_selector);
        ArgumentExceptionHelper.ThrowIfNull(observer);
        SelectAsyncSequentialSink sink = new(observer, _selector);
        var sub = _source.Subscribe(sink);
        return new DisposableBag(sub, sink);
    }

    /// <summary>Processes source values and owns the subscription state.</summary>
    /// <param name = "downstream">The downstream observer.</param>
    /// <param name = "selector">The asynchronous operation.</param>
    /// <remarks>The gate only guards the queue and the flags; the selector and the observer run without it held.</remarks>
    internal sealed class SelectAsyncSequentialSink(IObserver<TResult> downstream, Func<TSource, CancellationToken, Task<TResult>> selector) : IObserver<TSource>, IDisposable
    {
        /// <summary>Guards the queue and the flags; never held while the selector or the observer runs.</summary>
        private readonly Lock _gate = new();

        /// <summary>Cancelled when the subscription is disposed, so an in-flight projection can stop.</summary>
        private readonly CancellationTokenSource _cancellation = new();

        /// <summary>Queue of values to process.</summary>
        private readonly Queue<TSource> _queue = new();

        /// <summary>Serializes downstream deliveries.</summary>
        private SerializedDelivery<TResult> _delivery = new();

        /// <summary>Whether an async operation is currently in progress.</summary>
        private bool _isProcessing;

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
                if (!_isProcessing)
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

            _cancellation.Cancel();
            _cancellation.Dispose();
        }

        /// <summary>Queues the value and starts the drain loop when no operation is running.</summary>
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
                if (_isProcessing)
                {
                    return Task.CompletedTask;
                }

                _isProcessing = true;
            }

            return ProcessNextAsync();
        }

        /// <summary>Projects queued values one at a time, completing the sequence when the queue empties after the source finishes.</summary>
        /// <returns>A task representing the operation.</returns>
        private async Task ProcessNextAsync()
        {
            while (TryTakeNext(out var value))
            {
                try
                {
                    var result = await selector(value, _cancellation.Token).ConfigureAwait(false);
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

                        _isProcessing = false;
                    }

                    Flush();
                    return;
                }
            }

            Flush();
        }

        /// <summary>Takes the next queued value, or ends the drain and queues completion once the source has finished.</summary>
        /// <param name="value">The taken value.</param>
        /// <returns><see langword="true"/> when a value was taken.</returns>
        private bool TryTakeNext(out TSource value)
        {
            lock (_gate)
            {
                if (_disposed || _queue.Count == 0)
                {
                    _isProcessing = false;
                    if (_done && !_disposed)
                    {
                        _ = _delivery.PostCompleted();
                    }

                    value = default!;
                    return false;
                }

                value = _queue.Dequeue();
                return true;
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
        private readonly record struct PendingDrain(SelectAsyncSequentialSink Owner) : IDrainTarget
        {
            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Drain() => Owner.DrainPending();
        }
    }
}
