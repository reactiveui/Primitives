// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>Projects each element to an asynchronous operation, but only the result of the latest operation is emitted.</summary>
/// <typeparam name="TSource">The type of elements in the source sequence.</typeparam>
/// <typeparam name="TResult">The type of the result of the asynchronous operation.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="selector">The asynchronous projection function.</param>
internal sealed class SelectLatestAsyncObservable<TSource, TResult>(
    IObservable<TSource> source,
    Func<TSource, Task<TResult>> selector) : IObservable<TResult>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<TResult> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        InvalidOperationExceptionHelper.ThrowIfNull(selector);
        ArgumentExceptionHelper.ThrowIfNull(observer);

        var sink = new SelectLatestAsyncSink(observer, selector);
        var sub = source.Subscribe(sink);
        return new DisposableBag(sub, sink);
    }

    /// <summary>Sink that manages the latest async projection.</summary>
    /// <param name="downstream">The downstream observer.</param>
    /// <param name="selector">The async selector.</param>
    private sealed class SelectLatestAsyncSink(
        IObserver<TResult> downstream,
        Func<TSource, Task<TResult>> selector) : IObserver<TSource>, IDisposable
    {
        /// <summary>The gate for state access.</summary>
        private readonly Lock _gate = new();

        /// <summary>The current operation ID to track latest.</summary>
        private long _currentId;

        /// <summary>Whether the source has completed (no more values will arrive).</summary>
        private bool _sourceCompleted;

        /// <summary>Whether downstream completion has been signalled.</summary>
        private bool _completionSignalled;

        /// <summary>The latest in-flight projection task, used to delay completion until it finishes.</summary>
        private Task? _latestTask;

        /// <summary>Whether the sink has been disposed.</summary>
        private bool _disposed;

        /// <inheritdoc/>
        public void OnNext(TSource value)
        {
            long id;
            lock (_gate)
            {
                if (_sourceCompleted || _disposed)
                {
                    return;
                }

                id = ++_currentId;
            }

            var task = ProcessAsync(value, id);
            lock (_gate)
            {
                _latestTask = task;
            }
        }

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
            lock (_gate)
            {
                if (_sourceCompleted || _disposed)
                {
                    return;
                }

                _sourceCompleted = true;
                _completionSignalled = true;
                downstream.OnError(error);
            }
        }

        /// <inheritdoc/>
        public void OnCompleted()
        {
            Task? toAwait;
            lock (_gate)
            {
                if (_sourceCompleted || _disposed)
                {
                    return;
                }

                _sourceCompleted = true;
                toAwait = _latestTask;
            }

            if (toAwait?.IsCompleted != false)
            {
                SignalCompleted();
                return;
            }

            _ = toAwait.ContinueWith(static (_, s) => ((SelectLatestAsyncSink)s!).SignalCompleted(), this, TaskScheduler.Default);
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            lock (_gate)
            {
                _disposed = true;
            }
        }

        /// <summary>Signals downstream completion exactly once after the latest projection has finished.</summary>
        private void SignalCompleted()
        {
            lock (_gate)
            {
                if (_disposed || _completionSignalled)
                {
                    return;
                }

                _completionSignalled = true;
                downstream.OnCompleted();
            }
        }

        /// <summary>Processes the async operation and checks for latest ID.</summary>
        /// <param name="value">The value to project.</param>
        /// <param name="id">The ID of this operation.</param>
        /// <returns>A task representing the operation.</returns>
        private async Task ProcessAsync(TSource value, long id)
        {
            try
            {
                var result = await selector(value).ConfigureAwait(false);
                bool sourceDone;
                lock (_gate)
                {
                    if (_disposed || id != _currentId)
                    {
                        return;
                    }

                    downstream.OnNext(result);
                    sourceDone = _sourceCompleted;
                }

                if (sourceDone)
                {
                    SignalCompleted();
                }
            }
            catch (Exception ex)
            {
                lock (_gate)
                {
                    if (!_disposed && id == _currentId && !_completionSignalled)
                    {
                        _sourceCompleted = true;
                        _completionSignalled = true;
                        downstream.OnError(ex);
                    }
                }
            }
        }
    }
}
