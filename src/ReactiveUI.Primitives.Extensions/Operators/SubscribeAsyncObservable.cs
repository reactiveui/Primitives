// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>Subscribes to an observable sequence and executes an asynchronous handler for each element.</summary>
/// <typeparam name="T">The type of elements in the source sequence.</typeparam>
internal sealed class SubscribeAsyncObservable<T> : IDisposable
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
        Action<Exception>? onError = null,
        Action? onCompleted = null)
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

    /// <summary>Called when a new value is emitted by the source.</summary>
    /// <param name="value">The value emitted by the source.</param>
    private void OnNext(T value)
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

    /// <summary>Called when an error occurs in the source.</summary>
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

    /// <summary>Called when the source completes.</summary>
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

    /// <summary>Processes the next value in the queue.</summary>
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
