// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>Emits the larger or smaller latest value after both sources have emitted.</summary>
/// <typeparam name="T">The value type.</typeparam>
/// <param name="left">The first source.</param>
/// <param name="right">The second source.</param>
/// <param name="emitMaximum"><c>true</c> to emit the maximum; <c>false</c> to emit the minimum.</param>
/// <remarks>An error terminates immediately; successful completion waits for both sources unless one completes without emitting.</remarks>
[System.Diagnostics.DebuggerDisplay("BinaryMinMaxObservable: Left = {_left}, Right = {_right}")]
public sealed class BinaryMinMaxObservable<T>(IObservable<T> left, IObservable<T> right, bool emitMaximum) : IObservable<T>
    where T : struct, IComparable<T>
{
    /// <summary>The first source.</summary>
    private readonly IObservable<T> _left = InvalidOperationExceptionHelper.Check(left);

    /// <summary>The second source.</summary>
    private readonly IObservable<T> _right = InvalidOperationExceptionHelper.Check(right);

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        Sink sink = new(observer, emitMaximum);
        sink.AttachLeft(_left.Subscribe(sink));
        if (!sink.IsDone)
        {
            sink.AttachRight(_right.Subscribe(new RightWitness(sink)));
        }

        return sink;
    }

    /// <summary>Holds latest values and terminal state for two sources.</summary>
    /// <param name="downstream">The downstream observer.</param>
    /// <param name="emitMaximum"><c>true</c> for max; <c>false</c> for min.</param>
    /// <remarks>Emissions and terminals are queued in order under the gate and delivered after it is released.</remarks>
    private sealed class Sink(IObserver<T> downstream, bool emitMaximum) : IObserver<T>, IDisposable
    {
        /// <summary>Guards the latest values, the flags and the order notifications are queued in; never held while the observer runs.</summary>
        private readonly Lock _gate = new();

        /// <summary>Serializes downstream deliveries.</summary>
        private SerializedDelivery<T> _delivery = new();

        /// <summary>Left source subscription handle.</summary>
        private IDisposable? _leftSub;

        /// <summary>Right source subscription handle.</summary>
        private IDisposable? _rightSub;

        /// <summary>The latest left value.</summary>
        private T _leftValue;

        /// <summary>The latest right value.</summary>
        private T _rightValue;

        /// <summary>Whether the left source has produced a value.</summary>
        private bool _hasLeft;

        /// <summary>Whether the right source has produced a value.</summary>
        private bool _hasRight;

        /// <summary>Latch set to one when the left source completes.</summary>
        private int _leftCompleted;

        /// <summary>Latch set to one when the right source completes.</summary>
        private int _rightCompleted;

        /// <summary>Whether the sink is terminal.</summary>
        private bool _isDone;

        /// <summary>Gets a value indicating whether the sink has terminated.</summary>
        internal bool IsDone
        {
            get
            {
                lock (_gate)
                {
                    return _isDone;
                }
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(T value) => OnNext(true, value);

        /// <summary>Records one side's latest value and emits the winning comparison once both sides have a value.</summary>
        /// <param name="isLeft"><c>true</c> for the left source.</param>
        /// <param name="value">That side's latest value.</param>
        public void OnNext(bool isLeft, T value)
        {
            lock (_gate)
            {
                if (_isDone)
                {
                    return;
                }

                if (isLeft)
                {
                    _leftValue = value;
                    _hasLeft = true;
                }
                else
                {
                    _rightValue = value;
                    _hasRight = true;
                }

                if (!_hasLeft || !_hasRight)
                {
                    return;
                }

                var compare = _leftValue.CompareTo(_rightValue);
                var useLeft = emitMaximum ? compare >= 0 : compare <= 0;
                _ = _delivery.Post(useLeft ? _leftValue : _rightValue);
            }

            Flush();
        }

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
            lock (_gate)
            {
                if (_isDone)
                {
                    return;
                }

                _isDone = true;
                _ = _delivery.PostError(error);
            }

            DisposeUpstream();
            Flush();
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted() => OnCompleted(true);

        /// <summary>Records one side's completion, completing downstream when both sides finish or when this side never emitted.</summary>
        /// <param name="isLeft"><c>true</c> for the left source.</param>
        public void OnCompleted(bool isLeft)
        {
            bool terminated;
            lock (_gate)
            {
                RecordCompletionLocked(isLeft);
                terminated = _isDone;
            }

            if (terminated)
            {
                DisposeUpstream();
            }

            Flush();
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            lock (_gate)
            {
                _isDone = true;
            }

            DisposeUpstream();
        }

        /// <summary>Attaches the left subscription handle.</summary>
        /// <param name="subscription">The left subscription handle.</param>
        internal void AttachLeft(IDisposable? subscription)
        {
            if (subscription is not null)
            {
                SinkSubscription.Set(ref _leftSub, subscription);
            }
        }

        /// <summary>Attaches the right subscription handle.</summary>
        /// <param name="subscription">The right subscription handle.</param>
        internal void AttachRight(IDisposable? subscription)
        {
            if (subscription is not null)
            {
                SinkSubscription.Set(ref _rightSub, subscription);
            }
        }

        /// <summary>Records one side's completion and queues completion when the sequence is finished, while the caller holds the gate.</summary>
        /// <param name="isLeft"><c>true</c> for the left source.</param>
        private void RecordCompletionLocked(bool isLeft)
        {
            if (_isDone)
            {
                return;
            }

            if (isLeft)
            {
                if (Interlocked.Exchange(ref _leftCompleted, 1) != 0)
                {
                    return;
                }

                if (!_hasLeft)
                {
                    Complete();
                    return;
                }
            }
            else
            {
                if (Interlocked.Exchange(ref _rightCompleted, 1) != 0)
                {
                    return;
                }

                if (!_hasRight)
                {
                    Complete();
                    return;
                }
            }

            if (Volatile.Read(ref _leftCompleted) != 0 && Volatile.Read(ref _rightCompleted) != 0)
            {
                Complete();
            }
        }

        /// <summary>Marks the sink terminal and queues completion.</summary>
        private void Complete()
        {
            _isDone = true;
            _ = _delivery.PostCompleted();
        }

        /// <summary>Disposes both upstream subscriptions.</summary>
        private void DisposeUpstream()
        {
            SinkSubscription.Dispose(ref _leftSub);
            SinkSubscription.Dispose(ref _rightSub);
        }

        /// <summary>Delivers the queued notifications on the calling thread, or hands them to the thread already delivering.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void Flush() => _delivery.Flush(new PendingDrain(this));

        /// <summary>Delivers the queued notifications to the downstream observer.</summary>
        private void DrainPending()
        {
            try
            {
                if (_delivery.DrainTo(downstream))
                {
                    DisposeUpstream();
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
        private readonly record struct PendingDrain(Sink Owner) : IDrainTarget
        {
            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Drain() => Owner.DrainPending();
        }
    }

    /// <summary>Observer that forwards notifications from the right source to the shared sink.</summary>
    /// <param name="sink">The shared sink.</param>
    private sealed class RightWitness(Sink sink) : IObserver<T>
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(T value) => sink.OnNext(false, value);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => sink.OnError(error);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted() => sink.OnCompleted(false);
    }
}
