// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>Emits only the latest asynchronous projection, discarding superseded results and errors.</summary>
/// <typeparam name = "TSource">The type of elements in the source sequence.</typeparam>
/// <typeparam name = "TResult">The type of the result of the asynchronous operation.</typeparam>
/// <param name = "source">The source observable.</param>
/// <param name = "selector">The asynchronous projection function.</param>
/// <remarks>Superseded operations continue running; source completion waits for the latest projection.</remarks>
public sealed class SelectLatestAsyncObservable<TSource, TResult>(IObservable<TSource> source, Func<TSource, Task<TResult>> selector) : IObservable<TResult>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<TResult> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        InvalidOperationExceptionHelper.ThrowIfNull(selector);
        ArgumentExceptionHelper.ThrowIfNull(observer);
        SelectLatestAsyncSink sink = new(observer, selector);
        var sub = source.Subscribe(sink);
        return new DisposableBag(sub, sink);
    }

    /// <summary>Processes source values and owns the subscription state.</summary>
    /// <param name = "downstream">The downstream observer.</param>
    /// <param name = "selector">The asynchronous operation.</param>
    /// <remarks>Results and terminals are queued in order under the gate and delivered after it is released.</remarks>
    internal sealed class SelectLatestAsyncSink(IObserver<TResult> downstream, Func<TSource, Task<TResult>> selector) : IObserver<TSource>, IDisposable
    {
        /// <summary>Guards the projection bookkeeping and the order notifications are queued in; never held while the observer runs.</summary>
        private readonly Lock _gate = new();

        /// <summary>Serializes downstream deliveries.</summary>
        private SerializedDelivery<TResult> _delivery = new();

        /// <summary>Identifier of the most recent projection; a result carrying an older identifier is dropped.</summary>
        private long _currentId;

        /// <summary>Whether the source has completed (no more values will arrive).</summary>
        private bool _sourceCompleted;

        /// <summary>Whether downstream completion has been signalled.</summary>
        private bool _completionSignalled;

        /// <summary>The latest in-flight projection task, whose completion gates the downstream completion.</summary>
        private Task? _latestTask;

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
                if (_sourceCompleted || _disposed)
                {
                    return;
                }

                _sourceCompleted = true;
                _completionSignalled = true;
                _ = _delivery.PostError(error);
            }

            Flush();
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

            RegisterCompletion(toAwait, this);
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            lock (_gate)
            {
                _disposed = true;
            }
        }

        /// <summary>Starts a projection for the value and records it as the latest, superseding any in-flight one.</summary>
        /// <param name = "value">The source value.</param>
        /// <returns>The processing task, or a completed task when no work starts.</returns>
        internal Task OnNextAsync(TSource value)
        {
            long id;
            lock (_gate)
            {
                if (_sourceCompleted || _disposed)
                {
                    return Task.CompletedTask;
                }

                id = ++_currentId;
            }

            var task = ProcessAsync(value, id);
            lock (_gate)
            {
                _latestTask = task;
            }

            return task;
        }

        /// <summary>Signals downstream completion exactly once after the latest projection has finished.</summary>
        internal void SignalCompleted()
        {
            lock (_gate)
            {
                if (_disposed || _completionSignalled)
                {
                    return;
                }

                _completionSignalled = true;
                _ = _delivery.PostCompleted();
            }

            Flush();
        }

        /// <summary>Registers completion delivery for the pending projection.</summary>
        /// <param name="task">The pending projection.</param>
        /// <param name="sink">The completion recipient.</param>
        [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void RegisterCompletion(Task task, SelectLatestAsyncSink sink) =>
            _ = task.ContinueWith(static (_, state) => ((SelectLatestAsyncSink)state!).SignalCompleted(), sink, TaskScheduler.Default);

        /// <summary>Awaits the selector and emits or faults only while this operation is the latest one.</summary>
        /// <param name = "value">The value to project.</param>
        /// <param name = "id">The ID of this operation.</param>
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

                    _ = _delivery.Post(result);
                    sourceDone = _sourceCompleted;
                }

                Flush();
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
                        _ = _delivery.PostError(ex);
                    }
                }

                Flush();
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
        private readonly record struct PendingDrain(SelectLatestAsyncSink Owner) : IDrainTarget
        {
            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Drain() => Owner.DrainPending();
        }
    }
}
