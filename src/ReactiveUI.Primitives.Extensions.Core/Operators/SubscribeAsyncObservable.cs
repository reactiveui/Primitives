// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>Queues source values and invokes the asynchronous handler one value at a time.</summary>
/// <typeparam name="T">The type of elements in the source sequence.</typeparam>
/// <remarks>
/// Handler failure invokes the error callback and stops processing. Completion waits for queued values; disposal drops them and unsubscribes.
/// </remarks>
[System.Diagnostics.DebuggerDisplay("SubscribeAsyncObservable: Queued = {_queue.Count}, Processing = {_isProcessing}, Done = {_done}")]
public sealed class SubscribeAsyncObservable<T> : IDisposable
{
    /// <summary>The gate for state access.</summary>
    private readonly Lock _gate = new();

    /// <summary>Queue of values to process.</summary>
    private readonly Queue<T> _queue = new();

    /// <summary>The subscription to the source sequence.</summary>
    private readonly IDisposable _subscription;

    /// <summary>The asynchronous element handler.</summary>
    private readonly Func<T, ValueTask> _onNext;

    /// <summary>The error handler.</summary>
    private readonly Action<Exception>? _onError;

    /// <summary>The completion handler.</summary>
    private readonly Action? _onCompleted;

    /// <summary>Whether an async operation is currently in progress.</summary>
    private bool _isProcessing;

    /// <summary>Whether the source has completed.</summary>
    private bool _done;

    /// <summary>Whether the sink has been disposed.</summary>
    private bool _disposed;

    /// <summary>Initializes a new instance of the <see cref="SubscribeAsyncObservable{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="onNext">The asynchronous element handler.</param>
    /// <param name="onError">The error handler.</param>
    /// <param name="onCompleted">The completion handler.</param>
    public SubscribeAsyncObservable(
        IObservable<T> source,
        Func<T, ValueTask> onNext,
        Action<Exception>? onError,
        Action? onCompleted)
    {
        _onNext = onNext;
        _onError = onError;
        _onCompleted = onCompleted;
        _subscription = source.SubscribeCallbacks(OnNext, OnError, OnCompleted);
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        lock (_gate)
        {
            _disposed = true;
            _subscription.Dispose();
        }
    }

    /// <summary>Queues a source value and returns the operation started by it.</summary>
    /// <param name="value">The source value.</param>
    /// <returns>The processing task, or a completed task if no work starts.</returns>
    internal Task OnNextAsync(T value)
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

    /// <summary>Queues a source value.</summary>
    /// <param name="value">The source value.</param>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    private void OnNext(T value) => _ = OnNextAsync(value);

    /// <summary>Routes a source error to the error callback and stops processing the queue.</summary>
    /// <param name="error">The error that occurred.</param>
    private void OnError(Exception error)
    {
        lock (_gate)
        {
            if (_done || _disposed)
            {
                return;
            }

            _done = true;
            _onError?.Invoke(error);
        }
    }

    /// <summary>Marks the source finished and runs the completion callback when no handler is in flight.</summary>
    private void OnCompleted()
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
                _onCompleted?.Invoke();
            }
        }
    }

    /// <summary>Runs the handler for queued values in turn, invoking the completion callback when the queue empties after the source finishes.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    private async Task ProcessNextAsync()
    {
        while (true)
        {
            T value;
            lock (_gate)
            {
                if (_disposed || _queue.Count == 0)
                {
                    _isProcessing = false;
                    if (_done && !_disposed)
                    {
                        _onCompleted?.Invoke();
                    }

                    return;
                }

                value = _queue.Dequeue();
            }

            try
            {
                await _onNext(value).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                lock (_gate)
                {
                    if (!_disposed)
                    {
                        _done = true;
                        _onError?.Invoke(ex);
                    }

                    _isProcessing = false;
                    return;
                }
            }
        }
    }
}
