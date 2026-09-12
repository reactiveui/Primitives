// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>
/// Forwards each source value paired with a fresh disposable handle the consumer disposes to acknowledge it. Each value
/// carries its own handle, and a handle left undisposed leaves only its own acknowledgement wait outstanding.
/// </summary>
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

    /// <summary>Observer that pairs each value with a new acknowledgement handle and forwards the pair downstream.</summary>
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

        /// <summary>Delivers the value with a fresh acknowledgement signal and returns the task that the signal's disposal completes.</summary>
        /// <param name="value">The value to process.</param>
        /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
        private Task ProcessAsync(T value)
        {
            SyncSignal signal = new();
            downstream.OnNext((value, signal));
            return signal.WaitForDisposeAsync();
        }

        /// <summary>Acknowledgement handle for one emission whose disposal completes that emission's wait task.</summary>
        internal sealed class SyncSignal : IDisposable
        {
            /// <summary>The completion source, created only when the wait cannot finish synchronously.</summary>
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
            /// <returns>A completed task when the handle has been disposed; otherwise the task that its disposal completes.</returns>
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
