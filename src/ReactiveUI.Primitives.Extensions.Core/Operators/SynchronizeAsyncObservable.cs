// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>Pairs each source value with an independent disposable acknowledgement handle.</summary>
/// <typeparam name="T">The type of elements in the source sequence.</typeparam>
/// <param name="source">The source observable.</param>
/// <remarks>The handles are independent: the producer does not wait on one, and one value's handle does not gate the next.
/// An undisposed handle leaves only its own acknowledgement pending, so a subscriber that ignores the handle still receives
/// every value and the terminal notification.</remarks>
public sealed class SynchronizeAsyncObservable<T>(IObservable<T> source) : IObservable<(T Value, IDisposable Sync)>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<(T Value, IDisposable Sync)> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(observer);

        SynchronizeAsyncSink sink = new(observer);
        sink.Attach(source.Subscribe(sink));
        return sink;
    }

    /// <summary>Observer that pairs each value with a new acknowledgement handle and forwards the pair downstream.</summary>
    /// <param name="downstream">The downstream observer.</param>
    /// <remarks>Deliveries are serialized, and no lock is held while the observer runs.</remarks>
    internal sealed class SynchronizeAsyncSink(IObserver<(T Value, IDisposable Sync)> downstream) : IObserver<T>, IDisposable
    {
        /// <summary>Guards the flags; never held while the observer runs.</summary>
        private readonly Lock _gate = new();

        /// <summary>Serializes downstream deliveries.</summary>
        private SerializedDelivery<(T Value, IDisposable Sync)> _delivery = new();

        /// <summary>Upstream subscription handle.</summary>
        private IDisposable? _subscription;

        /// <summary>Whether the sink has completed.</summary>
        private bool _done;

        /// <summary>Whether the sink has been disposed.</summary>
        private bool _disposed;

        /// <inheritdoc/>
        /// <param name="value">The value to forward.</param>
        public void OnNext(T value)
        {
            lock (_gate)
            {
                if (_done || _disposed)
                {
                    return;
                }
            }

            try
            {
                _delivery.OnNext(downstream, (value, new SyncSignal()), new PendingDrain(this));
            }
            catch
            {
                Dispose();
                throw;
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
            }

            SinkSubscription.Dispose(ref _subscription);
            _delivery.OnError(error, new PendingDrain(this));
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
            }

            SinkSubscription.Dispose(ref _subscription);
            _delivery.OnCompleted(new PendingDrain(this));
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            lock (_gate)
            {
                _disposed = true;
            }

            SinkSubscription.Dispose(ref _subscription);
        }

        /// <summary>Attaches the upstream subscription handle.</summary>
        /// <param name="subscription">The upstream subscription handle.</param>
        internal void Attach(IDisposable? subscription)
        {
            if (subscription is not null)
            {
                SinkSubscription.Set(ref _subscription, subscription);
            }
        }

        /// <summary>Delivers the queued notifications to the downstream observer.</summary>
        private void DrainPending()
        {
            try
            {
                if (_delivery.DrainTo(downstream))
                {
                    SinkSubscription.Dispose(ref _subscription);
                }
            }
            catch
            {
                Dispose();
                throw;
            }
        }

        /// <summary>Drains this sink's queued notifications for the delivery gate.</summary>
        /// <param name="Owner">The sink.</param>
        private readonly record struct PendingDrain(SynchronizeAsyncSink Owner) : IDrainTarget
        {
            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Drain() => Owner.DrainPending();
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
