// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>Wraps elements in a synchronization context that waits for a disposal signal before proceeding to the next element.</summary>
/// <typeparam name="T">The type of elements in the source sequence.</typeparam>
/// <param name="source">The source observable.</param>
public sealed class SynchronizeAsyncObservable<T>(IObservable<T> source) : IObservable<(T Value, IDisposable Sync)>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<(T Value, IDisposable Sync)> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(observer);

        SynchronizeAsyncSink sink = new(observer);
        var sub = source.Subscribe(sink);
        return new DisposableBag(sub, sink);
    }

    /// <summary>The sink for the <see cref="SynchronizeAsyncObservable{T}"/>.</summary>
    /// <param name="downstream">The downstream observer.</param>
    internal sealed class SynchronizeAsyncSink(IObserver<(T Value, IDisposable Sync)> downstream) : IObserver<T>, IDisposable
    {
        /// <summary>The gate for state access.</summary>
        private readonly Lock _gate = new();

        /// <summary>Whether the sink has completed.</summary>
        private bool _done;

        /// <summary>Whether the sink has been disposed.</summary>
        private bool _disposed;

        /// <inheritdoc/>
        /// <param name="value">The value.</param>
        public void OnNext(T value)
        {
            lock (_gate)
            {
                if (_done || _disposed)
                {
                    return;
                }
            }

            _ = ProcessAsync(value);
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
                downstream.OnCompleted();
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

        /// <summary>Delivers the value and waits for the consumer to dispose its acknowledgement signal.</summary>
        /// <param name="value">The value to process.</param>
        /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
        private Task ProcessAsync(T value)
        {
            SyncSignal signal = new();
            downstream.OnNext((value, signal));
            return signal.WaitForDisposeAsync();
        }

        /// <summary>Releases one emission when the consumer disposes it, allocating completion state only for an asynchronous wait.</summary>
        internal sealed class SyncSignal : IDisposable
        {
            /// <summary>The lazily-created completion source; only allocated on the slow path.</summary>
            private TaskCompletionSource<bool>? _tcs;

            /// <summary>Latches to <c>1</c> on the first dispose so signalling is idempotent.</summary>
            private int _disposed;

            /// <inheritdoc/>
            public void Dispose()
            {
                if (Interlocked.Exchange(ref _disposed, 1) != 0)
                {
                    return;
                }

                Volatile.Read(ref _tcs)?.TrySetResult(true);
            }

            /// <summary>Returns the disposal task; the producer must call this exactly once per emission.</summary>
            /// <returns>A completed task if the consumer already disposed; otherwise the lazily-allocated TCS task.</returns>
            internal Task WaitForDisposeAsync()
            {
                if (Volatile.Read(ref _disposed) == 1)
                {
                    return Task.CompletedTask;
                }

                TaskCompletionSource<bool> tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
                Volatile.Write(ref _tcs, tcs);
                CompleteIfDisposedRaced(tcs);
                return tcs.Task;
            }

            /// <summary>Completes the acknowledgement task when disposal overlaps its publication.</summary>
            /// <param name="tcs">The completion source published for this signal.</param>
            internal void CompleteIfDisposedRaced(TaskCompletionSource<bool> tcs)
            {
                if (Volatile.Read(ref _disposed) != 1)
                {
                    return;
                }

                _ = tcs.TrySetResult(true);
            }
        }
    }
}
