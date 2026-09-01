// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive;
#else
namespace ReactiveUI.Primitives;
#endif

/// <summary>Coordinator helpers for multi-source combine-latest signal operators.</summary>
public static partial class LinqExtensions
{
    /// <summary>The element-type-agnostic view of a latest-value slot, so the coordinator can hold them all.</summary>
    private abstract class CombineLatestSlot
    {
        /// <summary>Subscribes the slot to the source it holds the latest value of.</summary>
        /// <returns>The source subscription.</returns>
        internal abstract IDisposable Subscribe();
    }

    /// <summary>Observable implementation for generated multi-source combine-latest overloads.</summary>
    /// <typeparam name="TResult">The projected result type.</typeparam>
    /// <param name="connect">
    /// Creates one typed slot per source against a fresh coordinator and returns the projection that reads
    /// them. Running per subscription is what keeps every latest value in a field of its own source's type.
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
            Justification = "An arity-N combinator takes one observable per source; a parameter object would erase the element type each source contributes to the selector.")]
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
            Justification = "An arity-N combinator takes one observable per source; a parameter object would erase the element type each source contributes to the selector.")]
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
            Justification = "An arity-N combinator takes one observable per source; a parameter object would erase the element type each source contributes to the selector.")]
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

    /// <summary>
    /// Holds the latest value of one source in a field of that source's own type, and observes the source
    /// directly so a subscription costs one object per source rather than a closure and a delegate per callback.
    /// </summary>
    /// <typeparam name="TResult">The projected result type.</typeparam>
    /// <typeparam name="T">The source element type.</typeparam>
    /// <param name="coordinator">The coordinator that serializes this slot against its siblings.</param>
    /// <param name="source">The source observable.</param>
    /// <param name="index">The source index.</param>
    [System.Diagnostics.DebuggerDisplay("CombineLatestSlot: Value = {Value}")]
    private sealed class CombineLatestSlot<TResult, T>(
        CombineLatestCoordinator<TResult> coordinator,
        IObservable<T> source,
        int index) : CombineLatestSlot, IObserver<T>
    {
        /// <summary>Gets the latest value this source produced, valid once every slot has one.</summary>
        internal T Value { get; private set; } = default!;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(T value) => coordinator.OnNext(index, this, value);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => coordinator.OnError(error);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted() => coordinator.OnCompleted(index);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal override IDisposable Subscribe() => source.Subscribe(this);

        /// <summary>Records the latest value. Called by the coordinator while it holds the serialization gate.</summary>
        /// <param name="value">The value the source produced.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Accept(T value) => Value = value;
    }

    /// <summary>Coordinates latest values, completion, and errors for a multi-source combine-latest subscription.</summary>
    /// <typeparam name="TResult">The projected result type.</typeparam>
    private sealed class CombineLatestCoordinator<TResult> : IDisposable
    {
        /// <summary>The number of flag slots each source occupies: one for its value, one for its completion.</summary>
        private const int FlagsPerSource = 2;

        /// <summary>Serializes notifications across all sources.</summary>
        private readonly Lock _gate = new();

        /// <summary>The downstream observer.</summary>
        private readonly IObserver<TResult> _observer;

        /// <summary>The typed latest-value slot for each source, in source order.</summary>
        private readonly List<CombineLatestSlot> _slots = [];

        /// <summary>The active source subscriptions.</summary>
        private readonly MultipleDisposable _subscriptions = [];

        /// <summary>
        /// One flag per source twice over: the first half records whether a source has produced a value, the
        /// second whether it has completed. A single array keeps both counters' state in one allocation and
        /// stays correct for the collection overloads, whose source count has no upper bound.
        /// </summary>
        private bool[] _flags = [];

        /// <summary>The projection over this subscription's slots.</summary>
        private Func<TResult> _project = null!;

        /// <summary>The number of sources still waiting for their first value.</summary>
        private int _missingValues;

        /// <summary>The number of sources that have not completed.</summary>
        private int _remainingCompletions;

        /// <summary>Whether a terminal notification has already been forwarded.</summary>
        private bool _completed;

        /// <summary>Initializes a new instance of the <see cref="CombineLatestCoordinator{TResult}"/> class.</summary>
        /// <param name="observer">The downstream observer.</param>
        internal CombineLatestCoordinator(IObserver<TResult> observer) => _observer = observer;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose() => _subscriptions.Dispose();

        /// <summary>Creates the next source's typed slot, without subscribing to it yet.</summary>
        /// <typeparam name="T">The source element type.</typeparam>
        /// <param name="source">The source observable.</param>
        /// <returns>The slot that will hold the source's latest value.</returns>
        internal CombineLatestSlot<TResult, T> Attach<T>(IObservable<T> source)
        {
            CombineLatestSlot<TResult, T> slot = new(this, source, _slots.Count);
            _slots.Add(slot);
            return slot;
        }

        /// <summary>Subscribes to every attached source and returns this coordinator as the subscription.</summary>
        /// <param name="project">The projection over the slots created by <see cref="Attach{T}"/>.</param>
        /// <returns>This coordinator.</returns>
        /// <remarks>
        /// The projection is installed before the first subscription, so a source that produces a value inside
        /// its own subscribe call still finds somewhere to project into.
        /// </remarks>
        internal CombineLatestCoordinator<TResult> Run(Func<TResult> project)
        {
            _project = project;
            _flags = new bool[_slots.Count * FlagsPerSource];
            _missingValues = _slots.Count;
            _remainingCompletions = _slots.Count;
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

        /// <summary>Records a latest source value and emits a projected value once every source has produced one.</summary>
        /// <typeparam name="T">The source element type.</typeparam>
        /// <param name="index">The source index.</param>
        /// <param name="slot">The slot that holds the source's latest value.</param>
        /// <param name="value">The source value.</param>
        internal void OnNext<T>(int index, CombineLatestSlot<TResult, T> slot, T value)
        {
            lock (_gate)
            {
                if (_completed)
                {
                    return;
                }

                slot.Accept(value);
                if (!_flags[index])
                {
                    _flags[index] = true;
                    _missingValues--;
                }

                if (_missingValues == 0)
                {
                    _observer.OnNext(_project());
                }
            }
        }

        /// <summary>Forwards an error and disposes all source subscriptions.</summary>
        /// <param name="error">The source error.</param>
        internal void OnError(Exception error)
        {
            lock (_gate)
            {
                if (_completed)
                {
                    return;
                }

                _completed = true;
                _observer.OnError(error);
            }

            _subscriptions.Dispose();
        }

        /// <summary>Tracks source completion and completes downstream after every source completes.</summary>
        /// <param name="index">The source index.</param>
        internal void OnCompleted(int index)
        {
            var done = _slots.Count + index;
            lock (_gate)
            {
                if (_completed || _flags[done])
                {
                    return;
                }

                _flags[done] = true;
                _remainingCompletions--;
                if (_remainingCompletions != 0)
                {
                    return;
                }

                _completed = true;
                _observer.OnCompleted();
            }

            _subscriptions.Dispose();
        }
    }
}
