// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Extensions;
#if REACTIVE_SHIM
using ReactiveUI.Primitives.Reactive.Internal;
#else
using ReactiveUI.Primitives.Internal;
#endif

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive;
#else
namespace ReactiveUI.Primitives;
#endif

/// <summary>Coordinator helpers for multi-source signal operators.</summary>
public static partial class LinqExtensions
{
    /// <summary>Range-specialized WithLatest (Latch): emits each left range value paired with the right range's final value.</summary>
    /// <typeparam name="TResult">The result value type.</typeparam>
    /// <param name="left">The left source range.</param>
    /// <param name="right">The right source range.</param>
    /// <param name="selector">The result projection.</param>
    private sealed class RangeWithLatestSignal<TResult>(RangeSignal left, RangeSignal right, Func<int, int, TResult> selector) : IObservable<TResult>
    {
        /// <summary>The left source range.</summary>
        private readonly RangeSignal _left = left;

        /// <summary>The right source range (its final value is the latched value).</summary>
        private readonly RangeSignal _right = right;

        /// <summary>The result projection.</summary>
        private readonly Func<int, int, TResult> _selector = selector;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<TResult> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            var rightValue = _right.Start + _right.Count - 1;
            for (var i = 0; i < _left.Count; i++)
            {
                observer.OnNext(_selector(_left.Start + i, rightValue));
            }

            observer.OnCompleted();
            return EmptyDisposable.Instance;
        }
    }

    /// <summary>Dedicated signal for <c>Race</c>, handing each subscription to a coordinator.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class RaceSignal<T> : IObservable<T>
    {
        /// <summary>The candidate sources.</summary>
        private readonly IObservable<IObservable<T>> _sources;

        /// <summary>Initializes a new instance of the <see cref="RaceSignal{T}"/> class.</summary>
        /// <param name="sources">The candidate sources.</param>
        internal RaceSignal(IObservable<IObservable<T>> sources) => _sources = sources;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            return new RaceCoordinator<T>(observer).Run(_sources);
        }
    }

    /// <summary>Dedicated signal for <c>Zip</c>, holding the two sources and the projection.</summary>
    /// <typeparam name="TLeft">The left value type.</typeparam>
    /// <typeparam name="TRight">The right value type.</typeparam>
    /// <typeparam name="TResult">The result value type.</typeparam>
    /// <param name="left">The left source.</param>
    /// <param name="right">The right source.</param>
    /// <param name="selector">The projection function.</param>
    private sealed class ZipSignal<TLeft, TRight, TResult>(IObservable<TLeft> left, IObservable<TRight> right, Func<TLeft, TRight, TResult> selector) : IObservable<TResult>
    {
        /// <summary>The left source.</summary>
        private readonly IObservable<TLeft> _left = left;

        /// <summary>The right source.</summary>
        private readonly IObservable<TRight> _right = right;

        /// <summary>The projection function.</summary>
        private readonly Func<TLeft, TRight, TResult> _selector = selector;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<TResult> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            return new PairWitness<TLeft, TRight, TResult>(observer, _selector).Run(_left, _right);
        }
    }

    /// <summary>Dedicated signal for the two-source <c>CombineLatest</c> path.</summary>
    /// <typeparam name="TLeft">The left value type.</typeparam>
    /// <typeparam name="TRight">The right value type.</typeparam>
    /// <typeparam name="TResult">The result value type.</typeparam>
    /// <param name="left">The left source.</param>
    /// <param name="right">The right source.</param>
    /// <param name="selector">The projection function.</param>
    private sealed class CombineLatestSignal<TLeft, TRight, TResult>(
        IObservable<TLeft> left,
        IObservable<TRight> right,
        Func<TLeft, TRight, TResult> selector) : IObservable<TResult>
    {
        /// <summary>The left source.</summary>
        private readonly IObservable<TLeft> _left = left;

        /// <summary>The right source.</summary>
        private readonly IObservable<TRight> _right = right;

        /// <summary>The projection function.</summary>
        private readonly Func<TLeft, TRight, TResult> _selector = selector;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<TResult> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            return new CombineLatestCoordinator<TLeft, TRight, TResult>(observer, _selector).Run(_left, _right);
        }
    }

    /// <summary>Dedicated signal for <c>Blend</c> (concurrent merge of inner sources).</summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class BlendSignal<T> : IObservable<T>
    {
        /// <summary>The outer sequence of inner sources.</summary>
        private readonly IObservable<IObservable<T>> _sources;

        /// <summary>Initializes a new instance of the <see cref="BlendSignal{T}"/> class.</summary>
        /// <param name="sources">The outer sequence of inner sources.</param>
        internal BlendSignal(IObservable<IObservable<T>> sources) => _sources = sources;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            return new BlendCoordinator<T>(observer).Run(_sources);
        }
    }

    /// <summary>Coordinates concurrent merging of inner sources for <c>Blend</c>.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <remarks>
    /// Deliveries are serialized by a <see cref="SerializedDelivery{T}"/>, so no lock is held while the downstream observer
    /// runs; values that arrive while another thread is delivering are delivered in arrival order.
    /// </remarks>
    private sealed class BlendCoordinator<T> : IDisposable
    {
        /// <summary>Active subscriptions.</summary>
        private readonly MultipleDisposable _pocket = [];

        /// <summary>The downstream observer.</summary>
        private readonly IObserver<T> _observer;

        /// <summary>Serializes downstream deliveries.</summary>
        private SerializedDelivery<T> _delivery = new();

        /// <summary>Whether the outer source completed, as 0 or 1.</summary>
        private int _outerCompleted;

        /// <summary>The number of active inner sources.</summary>
        private int _active;

        /// <summary>Initializes a new instance of the <see cref="BlendCoordinator{T}"/> class.</summary>
        /// <param name="observer">The downstream observer.</param>
        internal BlendCoordinator(IObserver<T> observer) => _observer = observer;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose() => _pocket.Dispose();

        /// <summary>Subscribes to the outer source.</summary>
        /// <param name="sources">The outer sequence of inner sources.</param>
        /// <returns>The subscription cleanup.</returns>
        internal BlendCoordinator<T> Run(IObservable<IObservable<T>> sources)
        {
            _pocket.Add(sources.Subscribe(OnSource, OnAnyError, OnOuterCompleted));
            return this;
        }

        /// <summary>Subscribes a new inner source concurrently.</summary>
        /// <param name="source">The inner source.</param>
        private void OnSource(IObservable<T> source)
        {
            if (source is null)
            {
                OnAnyError(new InvalidOperationException("Blend source contained null."));
                return;
            }

            _ = Interlocked.Increment(ref _active);
            _pocket.Add(source.Subscribe(OnInnerNext, OnAnyError, OnInnerCompleted));
        }

        /// <summary>Forwards an inner value, directly when nothing else is delivering.</summary>
        /// <param name="value">The value to forward.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void OnInnerNext(T value) => _delivery.OnNext(_observer, value, new PendingDrain(this));

        /// <summary>Forwards the first terminal error and suppresses later notifications.</summary>
        /// <param name="error">The error to forward.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void OnAnyError(Exception error) => _delivery.OnError(error, new PendingDrain(this));

        /// <summary>Decrements the active count and attempts completion.</summary>
        private void OnInnerCompleted()
        {
            _ = Interlocked.Decrement(ref _active);
            TryComplete();
        }

        /// <summary>Marks the outer source complete and attempts completion.</summary>
        private void OnOuterCompleted()
        {
            Volatile.Write(ref _outerCompleted, 1);
            TryComplete();
        }

        /// <summary>Delivers completion once the outer and all inners are done.</summary>
        private void TryComplete()
        {
            if (Volatile.Read(ref _outerCompleted) == 0 || Volatile.Read(ref _active) != 0)
            {
                return;
            }

            _delivery.OnCompleted(new PendingDrain(this));
        }

        /// <summary>Drains this coordinator's queued notifications for the delivery gate.</summary>
        /// <param name="Owner">The coordinator.</param>
        private readonly record struct PendingDrain(BlendCoordinator<T> Owner) : IDrainTarget
        {
            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Drain() => _ = Owner._delivery.DrainTo(Owner._observer);
        }
    }

    /// <summary>Dedicated signal for the general <c>Latch</c> (WithLatest) path.</summary>
    /// <typeparam name="TLeft">The left value type.</typeparam>
    /// <typeparam name="TRight">The right value type.</typeparam>
    /// <typeparam name="TResult">The result value type.</typeparam>
    /// <param name="left">The left source.</param>
    /// <param name="right">The right source.</param>
    /// <param name="selector">The projection function.</param>
    private sealed class LatchSignal<TLeft, TRight, TResult>(IObservable<TLeft> left, IObservable<TRight> right, Func<TLeft, TRight, TResult> selector) : IObservable<TResult>
    {
        /// <summary>The left (driving) source.</summary>
        private readonly IObservable<TLeft> _left = left;

        /// <summary>The right (latched) source.</summary>
        private readonly IObservable<TRight> _right = right;

        /// <summary>The projection function.</summary>
        private readonly Func<TLeft, TRight, TResult> _selector = selector;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<TResult> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            return new LatchCoordinator<TLeft, TRight, TResult>(observer, _selector).Run(_left, _right);
        }
    }

    /// <summary>Coordinates the general WithLatest projection for <c>Latch</c>.</summary>
    /// <typeparam name="TLeft">The left value type.</typeparam>
    /// <typeparam name="TRight">The right value type.</typeparam>
    /// <typeparam name="TResult">The result value type.</typeparam>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="selector">The projection function.</param>
    private sealed class LatchCoordinator<TLeft, TRight, TResult>(IObserver<TResult> observer, Func<TLeft, TRight, TResult> selector)
    {
        /// <summary>Guards the latest-right state.</summary>
        private readonly Lock _gate = new();

        /// <summary>The downstream observer.</summary>
        private readonly IObserver<TResult> _observer = observer;

        /// <summary>The projection function.</summary>
        private readonly Func<TLeft, TRight, TResult> _selector = selector;

        /// <summary>A value indicating whether the right source has produced a value.</summary>
        private bool _hasRight;

        /// <summary>The latest right value.</summary>
        private TRight? _latestRight;

        /// <summary>Subscribes to both sources.</summary>
        /// <param name="left">The left source.</param>
        /// <param name="right">The right source.</param>
        /// <returns>The subscription cleanup.</returns>
        internal MultipleDisposable Run(IObservable<TLeft> left, IObservable<TRight> right) =>
            new(
                right.Subscribe(OnRightNext, _observer.OnError, NoOp),
                left.Subscribe(OnLeftNext, _observer.OnError, _observer.OnCompleted));

        /// <summary>No-op completion handler for the right (latched) source.</summary>
        private static void NoOp()
        {
            // The right source's completion does not terminate the latch; only the left source does.
        }

        /// <summary>Stores the latest right value.</summary>
        /// <param name="value">The right value.</param>
        private void OnRightNext(TRight value)
        {
            lock (_gate)
            {
                _hasRight = true;
                _latestRight = value;
            }
        }

        /// <summary>Projects a left value with the latest right value when available.</summary>
        /// <param name="value">The left value.</param>
        private void OnLeftNext(TLeft value)
        {
            TRight rightValue;
            lock (_gate)
            {
                if (!_hasRight)
                {
                    return;
                }

                rightValue = _latestRight!;
            }

            _observer.OnNext(_selector(value, rightValue));
        }
    }

    /// <summary>Coordinates race subscriptions and forwards only the winning source.</summary>
    /// <typeparam name="T">The source value type.</typeparam>
    private sealed class RaceCoordinator<T> : IDisposable
    {
        /// <summary>The shared race-arm bookkeeping.</summary>
        private readonly RaceArms<T> _arms;

        /// <summary>Initializes a new instance of the <see cref="RaceCoordinator{T}"/> class.</summary>
        /// <param name="observer">The downstream observer.</param>
        internal RaceCoordinator(IObserver<T> observer) => _arms = new(observer);

        /// <summary>Releases the active subscriptions.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose() => _arms.Dispose();

        /// <summary>Starts observing the candidate source streams.</summary>
        /// <param name="sources">The candidate source streams.</param>
        /// <returns>The coordinator that owns the subscription cleanup.</returns>
        internal RaceCoordinator<T> Run(IObservable<IObservable<T>> sources)
        {
            _arms.Add(sources.Subscribe(_arms.OnSource, _arms.OnOuterError, OnOuterCompleted));
            return this;
        }

        /// <summary>Handles completion of the outer sequence.</summary>
        private static void OnOuterCompleted()
        {
            // Only the winning source determines completion.
        }
    }

    /// <summary>Coordinates a two-source combine-latest operation.</summary>
    /// <typeparam name="TLeft">The left value type.</typeparam>
    /// <typeparam name="TRight">The right value type.</typeparam>
    /// <typeparam name="TResult">The result value type.</typeparam>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="selector">The projection function.</param>
    private sealed class CombineLatestCoordinator<TLeft, TRight, TResult>(IObserver<TResult> observer, Func<TLeft, TRight, TResult> selector) : IDrainTarget
    {
        /// <summary>The downstream observer.</summary>
        private readonly IObserver<TResult> _observer = observer;

        /// <summary>The projection function.</summary>
        private readonly Func<TLeft, TRight, TResult> _selector = selector;

        /// <summary>Serializes downstream deliveries, so no lock is held while the projection or the observer runs.</summary>
        private DeliveryGateState _delivery;

        /// <summary>Updates and the terminal notification queued while another thread delivers.</summary>
        private PendingNotifications<Update> _pending = new();

        /// <summary>Whether the left source completed, as 0 or 1.</summary>
        private int _leftDone;

        /// <summary>Whether the right source completed, as 0 or 1.</summary>
        private int _rightDone;

        /// <summary>A value indicating whether the left source has produced a value.</summary>
        private bool _hasLeft;

        /// <summary>A value indicating whether the right source has produced a value.</summary>
        private bool _hasRight;

        /// <summary>The latest left value.</summary>
        private TLeft? _latestLeft;

        /// <summary>The latest right value.</summary>
        private TRight? _latestRight;

        /// <inheritdoc/>
        public void Drain()
        {
            while (true)
            {
                switch (_pending.TakeNext(out var update, out var error))
                {
                    case PendingDelivery.Value:
                    {
                        Apply(update);
                        break;
                    }

                    case PendingDelivery.Terminal when error is null:
                    {
                        _observer.OnCompleted();
                        return;
                    }

                    case PendingDelivery.Terminal:
                    {
                        _observer.OnError(error);
                        return;
                    }

                    default:
                    {
                        return;
                    }
                }
            }
        }

        /// <summary>Subscribes to both combine-latest sources.</summary>
        /// <param name="left">The left source.</param>
        /// <param name="right">The right source.</param>
        /// <returns>The subscription cleanup.</returns>
        internal MultipleDisposable Run(IObservable<TLeft> left, IObservable<TRight> right) =>
            new(
                left.Subscribe(OnLeftNext, OnError, OnLeftCompleted),
                right.Subscribe(OnRightNext, OnError, OnRightCompleted));

        /// <summary>Records a left value directly when nothing else is delivering, otherwise queues it for the delivering thread.</summary>
        /// <param name="value">The left value.</param>
        private void OnLeftNext(TLeft value)
        {
            if (_pending.HasItems || !DeliveryGate.TryEnter(ref _delivery))
            {
                Queue(new(IsLeft: true, value, default!));
                return;
            }

            try
            {
                if (!_pending.IsTerminated)
                {
                    _latestLeft = value;
                    _hasLeft = true;
                    EmitLatest();
                }
            }
            catch
            {
                _ = DeliveryGate.Reset(ref _delivery);
                throw;
            }

            DeliveryGate.Exit(ref _delivery, this);
        }

        /// <summary>Records a right value directly when nothing else is delivering, otherwise queues it for the delivering thread.</summary>
        /// <param name="value">The right value.</param>
        private void OnRightNext(TRight value)
        {
            if (_pending.HasItems || !DeliveryGate.TryEnter(ref _delivery))
            {
                Queue(new(IsLeft: false, default!, value));
                return;
            }

            try
            {
                if (!_pending.IsTerminated)
                {
                    _latestRight = value;
                    _hasRight = true;
                    EmitLatest();
                }
            }
            catch
            {
                _ = DeliveryGate.Reset(ref _delivery);
                throw;
            }

            DeliveryGate.Exit(ref _delivery, this);
        }

        /// <summary>Queues an update for the delivering thread and signals it.</summary>
        /// <param name="update">The update.</param>
        private void Queue(Update update)
        {
            if (!_pending.TryEnqueue(update))
            {
                return;
            }

            DeliveryGate.Signal(ref _delivery, this);
        }

        /// <summary>Records an update and emits the projection once both sources have a value.</summary>
        /// <param name="update">The update.</param>
        private void Apply(in Update update)
        {
            if (update.IsLeft)
            {
                _latestLeft = update.Left;
                _hasLeft = true;
            }
            else
            {
                _latestRight = update.Right;
                _hasRight = true;
            }

            EmitLatest();
        }

        /// <summary>Emits the projection of the latest values once both sources have produced one.</summary>
        private void EmitLatest()
        {
            if (!_hasLeft || !_hasRight)
            {
                return;
            }

            _observer.OnNext(_selector(_latestLeft!, _latestRight!));
        }

        /// <summary>Requests the first error as the terminal notification.</summary>
        /// <param name="error">The error.</param>
        private void OnError(Exception error)
        {
            if (!_pending.TryRequestTerminal(error))
            {
                return;
            }

            DeliveryGate.Signal(ref _delivery, this);
        }

        /// <summary>Marks the left source as complete.</summary>
        private void OnLeftCompleted()
        {
            Volatile.Write(ref _leftDone, 1);
            TryComplete();
        }

        /// <summary>Marks the right source as complete.</summary>
        private void OnRightCompleted()
        {
            Volatile.Write(ref _rightDone, 1);
            TryComplete();
        }

        /// <summary>Requests completion once both sources have completed.</summary>
        private void TryComplete()
        {
            if (Volatile.Read(ref _leftDone) == 0 || Volatile.Read(ref _rightDone) == 0 || !_pending.TryRequestTerminal(null))
            {
                return;
            }

            DeliveryGate.Signal(ref _delivery, this);
        }

        /// <summary>A value from one side, queued while another thread delivers.</summary>
        /// <param name="IsLeft">Whether the value came from the left source.</param>
        /// <param name="Left">The left value, when <paramref name="IsLeft"/> is set.</param>
        /// <param name="Right">The right value, when <paramref name="IsLeft"/> is clear.</param>
        private readonly record struct Update(bool IsLeft, TLeft Left, TRight Right);
    }
}
