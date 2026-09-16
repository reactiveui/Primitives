// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
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

/// <summary>Coordinator helpers for multi-source combine-latest signal operators.</summary>
public static partial class LinqExtensions
{
    /// <summary>The element-type-agnostic view of a latest-value slot, so the coordinator can hold them all.</summary>
    private interface ICombineLatestSlot
    {
        /// <summary>Subscribes the slot to the source it holds the latest value of.</summary>
        /// <returns>The source subscription.</returns>
        IDisposable Subscribe();

        /// <summary>Applies the oldest value this slot queued while another thread was delivering.</summary>
        void ApplyQueued();
    }

    /// <summary>Observable implementation for generated multi-source combine-latest overloads.</summary>
    /// <typeparam name="TResult">The projected result type.</typeparam>
    /// <param name="connect">
    /// Creates one typed slot per source against a fresh coordinator and returns the projection that reads them.
    /// </param>
    private sealed partial class CombineLatestSignal<TResult>(
        Func<CombineLatestCoordinator<TResult>, Func<TResult>> connect) : IObservable<TResult>
    {
        /// <summary>Creates this subscription's typed slots and the projection that reads them.</summary>
        private readonly Func<CombineLatestCoordinator<TResult>, Func<TResult>> _connect = connect;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<TResult> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            CombineLatestCoordinator<TResult> coordinator = new(observer);
            return coordinator.Run(_connect(coordinator));
        }

        /// <summary>Creates an arity-3 combine-latest signal.</summary>
        /// <typeparam name="T1">The first source element type.</typeparam>
        /// <typeparam name="T2">The second source element type.</typeparam>
        /// <typeparam name="T3">The third source element type.</typeparam>
        /// <param name="source">The first source observable.</param>
        /// <param name="source2">The second source observable.</param>
        /// <param name="source3">The third source observable.</param>
        /// <param name="selector">The selector that combines latest values from all sources.</param>
        /// <returns>The combine-latest signal.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static CombineLatestSignal<TResult> Create<T1, T2, T3>(
            IObservable<T1> source,
            IObservable<T2> source2,
            IObservable<T3> source3,
            Func<T1, T2, T3, TResult> selector) =>
            new(coordinator =>
            {
                var slot = coordinator.Attach(source);
                var slot2 = coordinator.Attach(source2);
                var slot3 = coordinator.Attach(source3);

                return () => selector(
                    slot.Value,
                    slot2.Value,
                    slot3.Value);
            });

        /// <summary>Creates an arity-4 combine-latest signal.</summary>
        /// <typeparam name="T1">The first source element type.</typeparam>
        /// <typeparam name="T2">The second source element type.</typeparam>
        /// <typeparam name="T3">The third source element type.</typeparam>
        /// <typeparam name="T4">The fourth source element type.</typeparam>
        /// <param name="source">The first source observable.</param>
        /// <param name="source2">The second source observable.</param>
        /// <param name="source3">The third source observable.</param>
        /// <param name="source4">The fourth source observable.</param>
        /// <param name="selector">The selector that combines latest values from all sources.</param>
        /// <returns>The combine-latest signal.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static CombineLatestSignal<TResult> Create<T1, T2, T3, T4>(
            IObservable<T1> source,
            IObservable<T2> source2,
            IObservable<T3> source3,
            IObservable<T4> source4,
            Func<T1, T2, T3, T4, TResult> selector) =>
            new(coordinator =>
            {
                var slot = coordinator.Attach(source);
                var slot2 = coordinator.Attach(source2);
                var slot3 = coordinator.Attach(source3);
                var slot4 = coordinator.Attach(source4);

                return () => selector(
                    slot.Value,
                    slot2.Value,
                    slot3.Value,
                    slot4.Value);
            });

        /// <summary>Creates an arity-5 combine-latest signal.</summary>
        /// <typeparam name="T1">The first source element type.</typeparam>
        /// <typeparam name="T2">The second source element type.</typeparam>
        /// <typeparam name="T3">The third source element type.</typeparam>
        /// <typeparam name="T4">The fourth source element type.</typeparam>
        /// <typeparam name="T5">The fifth source element type.</typeparam>
        /// <param name="source">The first source observable.</param>
        /// <param name="source2">The second source observable.</param>
        /// <param name="source3">The third source observable.</param>
        /// <param name="source4">The fourth source observable.</param>
        /// <param name="source5">The fifth source observable.</param>
        /// <param name="selector">The selector that combines latest values from all sources.</param>
        /// <returns>The combine-latest signal.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static CombineLatestSignal<TResult> Create<T1, T2, T3, T4, T5>(
            IObservable<T1> source,
            IObservable<T2> source2,
            IObservable<T3> source3,
            IObservable<T4> source4,
            IObservable<T5> source5,
            Func<T1, T2, T3, T4, T5, TResult> selector) =>
            new(coordinator =>
            {
                var slot = coordinator.Attach(source);
                var slot2 = coordinator.Attach(source2);
                var slot3 = coordinator.Attach(source3);
                var slot4 = coordinator.Attach(source4);
                var slot5 = coordinator.Attach(source5);

                return () => selector(
                    slot.Value,
                    slot2.Value,
                    slot3.Value,
                    slot4.Value,
                    slot5.Value);
            });

        /// <summary>Creates an arity-6 combine-latest signal.</summary>
        /// <typeparam name="T1">The first source element type.</typeparam>
        /// <typeparam name="T2">The second source element type.</typeparam>
        /// <typeparam name="T3">The third source element type.</typeparam>
        /// <typeparam name="T4">The fourth source element type.</typeparam>
        /// <typeparam name="T5">The fifth source element type.</typeparam>
        /// <typeparam name="T6">The sixth source element type.</typeparam>
        /// <param name="source">The first source observable.</param>
        /// <param name="source2">The second source observable.</param>
        /// <param name="source3">The third source observable.</param>
        /// <param name="source4">The fourth source observable.</param>
        /// <param name="source5">The fifth source observable.</param>
        /// <param name="source6">The sixth source observable.</param>
        /// <param name="selector">The selector that combines latest values from all sources.</param>
        /// <returns>The combine-latest signal.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static CombineLatestSignal<TResult> Create<T1, T2, T3, T4, T5, T6>(
            IObservable<T1> source,
            IObservable<T2> source2,
            IObservable<T3> source3,
            IObservable<T4> source4,
            IObservable<T5> source5,
            IObservable<T6> source6,
            Func<T1, T2, T3, T4, T5, T6, TResult> selector) =>
            new(coordinator =>
            {
                var slot = coordinator.Attach(source);
                var slot2 = coordinator.Attach(source2);
                var slot3 = coordinator.Attach(source3);
                var slot4 = coordinator.Attach(source4);
                var slot5 = coordinator.Attach(source5);
                var slot6 = coordinator.Attach(source6);

                return () => selector(
                    slot.Value,
                    slot2.Value,
                    slot3.Value,
                    slot4.Value,
                    slot5.Value,
                    slot6.Value);
            });

        /// <summary>Creates an arity-7 combine-latest signal.</summary>
        /// <typeparam name="T1">The first source element type.</typeparam>
        /// <typeparam name="T2">The second source element type.</typeparam>
        /// <typeparam name="T3">The third source element type.</typeparam>
        /// <typeparam name="T4">The fourth source element type.</typeparam>
        /// <typeparam name="T5">The fifth source element type.</typeparam>
        /// <typeparam name="T6">The sixth source element type.</typeparam>
        /// <typeparam name="T7">The seventh source element type.</typeparam>
        /// <param name="source">The first source observable.</param>
        /// <param name="source2">The second source observable.</param>
        /// <param name="source3">The third source observable.</param>
        /// <param name="source4">The fourth source observable.</param>
        /// <param name="source5">The fifth source observable.</param>
        /// <param name="source6">The sixth source observable.</param>
        /// <param name="source7">The seventh source observable.</param>
        /// <param name="selector">The selector that combines latest values from all sources.</param>
        /// <returns>The combine-latest signal.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [SuppressMessage(
            "Maintainability",
            "SST1472:Signatures should not declare too many parameters",
            Justification = "An arity-N combinator takes one observable per source.")]
        internal static CombineLatestSignal<TResult> Create<T1, T2, T3, T4, T5, T6, T7>(
            IObservable<T1> source,
            IObservable<T2> source2,
            IObservable<T3> source3,
            IObservable<T4> source4,
            IObservable<T5> source5,
            IObservable<T6> source6,
            IObservable<T7> source7,
            Func<T1, T2, T3, T4, T5, T6, T7, TResult> selector) =>
            new(coordinator =>
            {
                var slot = coordinator.Attach(source);
                var slot2 = coordinator.Attach(source2);
                var slot3 = coordinator.Attach(source3);
                var slot4 = coordinator.Attach(source4);
                var slot5 = coordinator.Attach(source5);
                var slot6 = coordinator.Attach(source6);
                var slot7 = coordinator.Attach(source7);

                return () => selector(
                    slot.Value,
                    slot2.Value,
                    slot3.Value,
                    slot4.Value,
                    slot5.Value,
                    slot6.Value,
                    slot7.Value);
            });

        /// <summary>Creates an arity-8 combine-latest signal.</summary>
        /// <typeparam name="T1">The first source element type.</typeparam>
        /// <typeparam name="T2">The second source element type.</typeparam>
        /// <typeparam name="T3">The third source element type.</typeparam>
        /// <typeparam name="T4">The fourth source element type.</typeparam>
        /// <typeparam name="T5">The fifth source element type.</typeparam>
        /// <typeparam name="T6">The sixth source element type.</typeparam>
        /// <typeparam name="T7">The seventh source element type.</typeparam>
        /// <typeparam name="T8">The eighth source element type.</typeparam>
        /// <param name="source">The first source observable.</param>
        /// <param name="source2">The second source observable.</param>
        /// <param name="source3">The third source observable.</param>
        /// <param name="source4">The fourth source observable.</param>
        /// <param name="source5">The fifth source observable.</param>
        /// <param name="source6">The sixth source observable.</param>
        /// <param name="source7">The seventh source observable.</param>
        /// <param name="source8">The eighth source observable.</param>
        /// <param name="selector">The selector that combines latest values from all sources.</param>
        /// <returns>The combine-latest signal.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [SuppressMessage(
            "Maintainability",
            "SST1472:Signatures should not declare too many parameters",
            Justification = "An arity-N combinator takes one observable per source.")]
        internal static CombineLatestSignal<TResult> Create<T1, T2, T3, T4, T5, T6, T7, T8>(
            IObservable<T1> source,
            IObservable<T2> source2,
            IObservable<T3> source3,
            IObservable<T4> source4,
            IObservable<T5> source5,
            IObservable<T6> source6,
            IObservable<T7> source7,
            IObservable<T8> source8,
            Func<T1, T2, T3, T4, T5, T6, T7, T8, TResult> selector) =>
            new(coordinator =>
            {
                var slot = coordinator.Attach(source);
                var slot2 = coordinator.Attach(source2);
                var slot3 = coordinator.Attach(source3);
                var slot4 = coordinator.Attach(source4);
                var slot5 = coordinator.Attach(source5);
                var slot6 = coordinator.Attach(source6);
                var slot7 = coordinator.Attach(source7);
                var slot8 = coordinator.Attach(source8);

                return () => selector(
                    slot.Value,
                    slot2.Value,
                    slot3.Value,
                    slot4.Value,
                    slot5.Value,
                    slot6.Value,
                    slot7.Value,
                    slot8.Value);
            });

        /// <summary>Creates an arity-9 combine-latest signal.</summary>
        /// <typeparam name="T1">The first source element type.</typeparam>
        /// <typeparam name="T2">The second source element type.</typeparam>
        /// <typeparam name="T3">The third source element type.</typeparam>
        /// <typeparam name="T4">The fourth source element type.</typeparam>
        /// <typeparam name="T5">The fifth source element type.</typeparam>
        /// <typeparam name="T6">The sixth source element type.</typeparam>
        /// <typeparam name="T7">The seventh source element type.</typeparam>
        /// <typeparam name="T8">The eighth source element type.</typeparam>
        /// <typeparam name="T9">The ninth source element type.</typeparam>
        /// <param name="source">The first source observable.</param>
        /// <param name="source2">The second source observable.</param>
        /// <param name="source3">The third source observable.</param>
        /// <param name="source4">The fourth source observable.</param>
        /// <param name="source5">The fifth source observable.</param>
        /// <param name="source6">The sixth source observable.</param>
        /// <param name="source7">The seventh source observable.</param>
        /// <param name="source8">The eighth source observable.</param>
        /// <param name="source9">The ninth source observable.</param>
        /// <param name="selector">The selector that combines latest values from all sources.</param>
        /// <returns>The combine-latest signal.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [SuppressMessage(
            "Maintainability",
            "SST1472:Signatures should not declare too many parameters",
            Justification = "An arity-N combinator takes one observable per source.")]
        internal static CombineLatestSignal<TResult> Create<T1, T2, T3, T4, T5, T6, T7, T8, T9>(
            IObservable<T1> source,
            IObservable<T2> source2,
            IObservable<T3> source3,
            IObservable<T4> source4,
            IObservable<T5> source5,
            IObservable<T6> source6,
            IObservable<T7> source7,
            IObservable<T8> source8,
            IObservable<T9> source9,
            Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, TResult> selector) =>
            new(coordinator =>
            {
                var slot = coordinator.Attach(source);
                var slot2 = coordinator.Attach(source2);
                var slot3 = coordinator.Attach(source3);
                var slot4 = coordinator.Attach(source4);
                var slot5 = coordinator.Attach(source5);
                var slot6 = coordinator.Attach(source6);
                var slot7 = coordinator.Attach(source7);
                var slot8 = coordinator.Attach(source8);
                var slot9 = coordinator.Attach(source9);

                return () => selector(
                    slot.Value,
                    slot2.Value,
                    slot3.Value,
                    slot4.Value,
                    slot5.Value,
                    slot6.Value,
                    slot7.Value,
                    slot8.Value,
                    slot9.Value);
            });
    }

    /// <summary>Holds the latest value of one source in a field of that source's own type and observes it directly.</summary>
    /// <typeparam name="TResult">The projected result type.</typeparam>
    /// <typeparam name="T">The source element type.</typeparam>
    /// <param name="coordinator">The coordinator that serializes this slot against its siblings.</param>
    /// <param name="source">The source observable.</param>
    [System.Diagnostics.DebuggerDisplay("CombineLatestSlot: Value = {Value}, HasValue = {HasValue}")]
    private sealed class CombineLatestSlot<TResult, T>(
        CombineLatestCoordinator<TResult> coordinator,
        IObservable<T> source) : ICombineLatestSlot, IObserver<T>
    {
        /// <summary>Values this source produced while another thread was delivering, in order; created on first contention.</summary>
        private ConcurrentQueue<T>? _queued;

        /// <summary>Whether this source has completed, as 0 or 1.</summary>
        private int _completed;

        /// <summary>Gets the latest value this source produced, valid once every slot has one.</summary>
        internal T Value { get; private set; } = default!;

        /// <summary>Gets or sets a value indicating whether the source has produced a value; touched only while the delivery gate is held.</summary>
        internal bool HasValue { get; set; }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(T value) => coordinator.OnNext(this, value);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => coordinator.OnError(error);

        /// <inheritdoc/>
        public void OnCompleted()
        {
            if (Interlocked.Exchange(ref _completed, 1) != 0)
            {
                return;
            }

            coordinator.OnCompleted();
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IDisposable Subscribe() => source.Subscribe(this);

        /// <inheritdoc/>
        public void ApplyQueued()
        {
            _ = Volatile.Read(ref _queued)!.TryDequeue(out var value);
            coordinator.Apply(this, value!);
        }

        /// <summary>Queues a value for the delivering thread; the coordinator records the slot's turn separately.</summary>
        /// <param name="value">The value the source produced.</param>
        internal void Queue(T value)
        {
            _ = Interlocked.CompareExchange(ref _queued, new(), null);
            Volatile.Read(ref _queued)!.Enqueue(value);
        }

        /// <summary>Records the latest value while the coordinator's delivery gate is held.</summary>
        /// <param name="value">The value the source produced.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Accept(T value) => Value = value;
    }

    /// <summary>Coordinates latest values, completion, and errors for a multi-source combine-latest subscription.</summary>
    /// <typeparam name="TResult">The projected result type.</typeparam>
    private sealed class CombineLatestCoordinator<TResult> : IDisposable, IDrainTarget
    {
        /// <summary>The downstream observer.</summary>
        private readonly IObserver<TResult> _observer;

        /// <summary>The typed latest-value slot for each source, in source order.</summary>
        private readonly List<ICombineLatestSlot> _slots = [];

        /// <summary>The active source subscriptions.</summary>
        private readonly MultipleDisposable _subscriptions = [];

        /// <summary>Serializes downstream deliveries, so no lock is held while the projection or the observer runs.</summary>
        private DeliveryGateState _delivery;

        /// <summary>The slots with a queued value, in arrival order, and the terminal notification.</summary>
        private PendingNotifications<ICombineLatestSlot> _pending = new();

        /// <summary>The projection over this subscription's slots.</summary>
        private Func<TResult> _project = null!;

        /// <summary>The number of sources yet to produce their first value.</summary>
        private int _missingValues;

        /// <summary>The number of sources that have not completed.</summary>
        private int _remainingCompletions;

        /// <summary>Initializes a new instance of the <see cref="CombineLatestCoordinator{TResult}"/> class.</summary>
        /// <param name="observer">The downstream observer.</param>
        internal CombineLatestCoordinator(IObserver<TResult> observer) => _observer = observer;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose() => _subscriptions.Dispose();

        /// <inheritdoc/>
        public void Drain()
        {
            while (true)
            {
                switch (_pending.TakeNext(out var slot, out var error))
                {
                    case PendingDelivery.Value:
                    {
                        slot.ApplyQueued();
                        break;
                    }

                    case PendingDelivery.Terminal when error is null:
                    {
                        _observer.OnCompleted();
                        _subscriptions.Dispose();
                        return;
                    }

                    case PendingDelivery.Terminal:
                    {
                        _observer.OnError(error);
                        _subscriptions.Dispose();
                        return;
                    }

                    default:
                    {
                        return;
                    }
                }
            }
        }

        /// <summary>Creates the next source's typed slot, without subscribing to it yet.</summary>
        /// <typeparam name="T">The source element type.</typeparam>
        /// <param name="source">The source observable.</param>
        /// <returns>The slot that will hold the source's latest value.</returns>
        internal CombineLatestSlot<TResult, T> Attach<T>(IObservable<T> source)
        {
            CombineLatestSlot<TResult, T> slot = new(this, source);
            _slots.Add(slot);
            return slot;
        }

        /// <summary>Subscribes to every attached source and returns this coordinator as the subscription.</summary>
        /// <param name="project">The projection over the slots created by <see cref="Attach{T}"/>.</param>
        /// <returns>This coordinator.</returns>
        internal CombineLatestCoordinator<TResult> Run(Func<TResult> project)
        {
            _project = project;
            _missingValues = _slots.Count;
            Volatile.Write(ref _remainingCompletions, _slots.Count);
            try
            {
                for (var i = 0; i < _slots.Count; i++)
                {
                    _subscriptions.Add(_slots[i].Subscribe());
                }
            }
            catch
            {
                _subscriptions.Dispose();
                throw;
            }

            return this;
        }

        /// <summary>Applies a source value directly when nothing else is delivering, otherwise queues it for the delivering thread.</summary>
        /// <typeparam name="T">The source element type.</typeparam>
        /// <param name="slot">The slot that holds the source's latest value.</param>
        /// <param name="value">The source value.</param>
        internal void OnNext<T>(CombineLatestSlot<TResult, T> slot, T value)
        {
            if (!_pending.HasItems && DeliveryGate.TryEnter(ref _delivery))
            {
                DeliverEntered(slot, value);
                return;
            }

            slot.Queue(value);
            if (!_pending.TryEnqueue(slot))
            {
                return;
            }

            DeliveryGate.Signal(ref _delivery, this);
        }

        /// <summary>Records a latest source value and emits a projected value once every source has produced one.</summary>
        /// <typeparam name="T">The source element type.</typeparam>
        /// <param name="slot">The slot that holds the source's latest value.</param>
        /// <param name="value">The source value.</param>
        internal void Apply<T>(CombineLatestSlot<TResult, T> slot, T value)
        {
            slot.Accept(value);
            if (!slot.HasValue)
            {
                slot.HasValue = true;
                _missingValues--;
            }

            if (_missingValues != 0)
            {
                return;
            }

            _observer.OnNext(_project());
        }

        /// <summary>Requests the first error as the terminal notification.</summary>
        /// <param name="error">The source error.</param>
        internal void OnError(Exception error)
        {
            if (!_pending.TryRequestTerminal(error))
            {
                return;
            }

            DeliveryGate.Signal(ref _delivery, this);
        }

        /// <summary>Requests completion once every source has completed.</summary>
        internal void OnCompleted()
        {
            if (Interlocked.Decrement(ref _remainingCompletions) != 0 || !_pending.TryRequestTerminal(null))
            {
                return;
            }

            DeliveryGate.Signal(ref _delivery, this);
        }

        /// <summary>Applies a source value on a gate this thread entered while nothing was queued.</summary>
        /// <typeparam name="T">The source element type.</typeparam>
        /// <param name="slot">The slot that holds the source's latest value.</param>
        /// <param name="value">The source value.</param>
        private void DeliverEntered<T>(CombineLatestSlot<TResult, T> slot, T value)
        {
            try
            {
                if (!_pending.IsTerminated)
                {
                    Apply(slot, value);
                }
            }
            catch
            {
                _ = DeliveryGate.Reset(ref _delivery);
                throw;
            }

            DeliveryGate.Exit(ref _delivery, this);
        }
    }
}
