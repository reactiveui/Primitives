// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Extensions;

/// <summary>Emits the initial value followed by each accumulation result, terminating if the accumulator throws.</summary>
/// <typeparam name="TSource">The type of elements in the source sequence.</typeparam>
/// <typeparam name="TAccumulate">The type of the accumulated value.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="initial">The initial accumulated value.</param>
/// <param name="accumulator">The accumulator function.</param>
public sealed class ScanWithInitialObservable<TSource, TAccumulate>(
    IObservable<TSource> source,
    TAccumulate initial,
    Func<TAccumulate, TSource, TAccumulate> accumulator) : IObservable<TAccumulate>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<TAccumulate> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        InvalidOperationExceptionHelper.ThrowIfNull(accumulator);
        ArgumentExceptionHelper.ThrowIfNull(observer);

        ScanWithInitialSink sink = new(observer, initial, accumulator);
        sink.Initialize();
        return source.Subscribe(sink);
    }

    /// <summary>Observer that keeps the running accumulation and emits it after each element.</summary>
    /// <param name="downstream">The observer to forward elements to.</param>
    /// <param name="initial">The initial accumulated value.</param>
    /// <param name="accumulator">The accumulator function.</param>
    /// <remarks>
    /// The accumulation is advanced by the source's serialized notifications; deliveries are serialized, and the accumulator
    /// and the observer run without a lock held.
    /// </remarks>
    private sealed class ScanWithInitialSink(
        IObserver<TAccumulate> downstream,
        TAccumulate initial,
        Func<TAccumulate, TSource, TAccumulate> accumulator) : IObserver<TSource>
    {
        /// <summary>Serializes downstream deliveries.</summary>
        private SerializedDelivery<TAccumulate> _delivery = new();

        /// <summary>The current accumulated value.</summary>
        private TAccumulate _current = initial;

        /// <summary>Whether the sink has finished.</summary>
        private bool _done;

        /// <summary>Emits the seed accumulation downstream, which the caller does before subscribing the source.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Initialize() => _delivery.OnNext(downstream, _current, new PendingDrain(this));

        /// <inheritdoc/>
        public void OnNext(TSource value)
        {
            if (Volatile.Read(ref _done))
            {
                return;
            }

            TAccumulate current;
            try
            {
                current = accumulator(_current, value);
            }
            catch (Exception ex)
            {
                Volatile.Write(ref _done, true);
                _delivery.OnError(ex, new PendingDrain(this));
                return;
            }

            _current = current;
            _delivery.OnNext(downstream, current, new PendingDrain(this));
        }

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
            Volatile.Write(ref _done, true);
            _delivery.OnError(error, new PendingDrain(this));
        }

        /// <inheritdoc/>
        public void OnCompleted()
        {
            Volatile.Write(ref _done, true);
            _delivery.OnCompleted(new PendingDrain(this));
        }

        /// <summary>Delivers the queued notifications to the downstream observer.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void DrainPending() => _ = _delivery.DrainTo(downstream);

        /// <summary>Drains this observer's queued notifications for the delivery gate.</summary>
        /// <param name="Owner">The observer.</param>
        private readonly record struct PendingDrain(ScanWithInitialSink Owner) : IDrainTarget
        {
            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Drain() => Owner.DrainPending();
        }
    }
}
