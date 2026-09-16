// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Extensions;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive;
#else
namespace ReactiveUI.Primitives;
#endif

/// <summary>Concurrently merges sources and suppresses values equal to the last forwarded value.</summary>
public static partial class LinqExtensions
{
    /// <summary>Concurrently merges sources and suppresses adjacent duplicate values using the default equality comparer.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="sources">The sources to merge.</param>
    /// <returns>An observable of the distinct merged values.</returns>
    /// <remarks>The first source error terminates the result; successful completion waits for every source.</remarks>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservable<T> BlendUnique<T>(params IObservable<T>[] sources) =>
        BlendUnique(sources, null);

    /// <summary>
    /// Concurrently merges the supplied sources and forwards only values that differ from the last forwarded
    /// value, using the supplied comparer (or the default when <see langword="null"/>).
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="sources">The sources to merge.</param>
    /// <param name="comparer">The equality comparer used to suppress duplicates, or <see langword="null"/> for the default.</param>
    /// <returns>An observable of the distinct merged values.</returns>
    public static IObservable<T> BlendUnique<T>(IObservable<T>[] sources, IEqualityComparer<T>? comparer)
    {
        ArgumentExceptionHelper.ThrowIfNull(sources);

        for (var i = 0; i < sources.Length; i++)
        {
            ArgumentExceptionHelper.ThrowIfNull(sources[i]);
        }

        return new BlendUniqueSignal<T>(sources, comparer ?? EqualityComparer<T>.Default);
    }

    /// <summary>A fused merge + distinct-until-changed observable over a fixed set of sources.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="sources">The sources to merge.</param>
    /// <param name="comparer">The equality comparer used to suppress duplicates.</param>
    private sealed class BlendUniqueSignal<T>(IObservable<T>[] sources, IEqualityComparer<T> comparer) : IObservable<T>
    {
        /// <summary>The sources to merge.</summary>
        private readonly IObservable<T>[] _sources = sources;

        /// <summary>The equality comparer used to suppress duplicates.</summary>
        private readonly IEqualityComparer<T> _comparer = comparer;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            BlendUniqueSink<T> sink = new(observer, _comparer);
            sink.Run(_sources);
            return sink;
        }
    }

    /// <summary>Forwards distinct merged values downstream and tears down every source on dispose.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="downstream">The downstream observer.</param>
    /// <param name="comparer">The equality comparer used to suppress duplicates.</param>
    /// <remarks>
    /// Merged values pass through a <see cref="SerializedDelivery{T}"/> whose observer is this sink, so the comparison and
    /// the downstream observer run serialized without any lock held. Values that arrive while another thread is delivering
    /// are compared and delivered in arrival order.
    /// </remarks>
    private sealed class BlendUniqueSink<T>(IObserver<T> downstream, IEqualityComparer<T> comparer) : IDisposable, IObserver<T>
    {
        /// <summary>The per-source subscriptions, torn down on dispose.</summary>
        private readonly MultipleDisposable _pocket = [];

        /// <summary>The downstream observer.</summary>
        private readonly IObserver<T> _downstream = downstream;

        /// <summary>The equality comparer used to suppress duplicates.</summary>
        private readonly IEqualityComparer<T> _comparer = comparer;

        /// <summary>Serializes the comparison and downstream deliveries.</summary>
        private SerializedDelivery<T> _delivery = new();

        /// <summary>The most recently forwarded value (valid only once <see cref="_hasLast"/> is set); touched only inside a delivery.</summary>
        private T _last = default!;

        /// <summary>Whether a value has been forwarded yet; touched only inside a delivery.</summary>
        private bool _hasLast;

        /// <summary>The number of sources that have not yet completed.</summary>
        private int _active;

        /// <summary>Subscribes to every merged source.</summary>
        /// <param name="sources">The sources to merge.</param>
        public void Run(IObservable<T>[] sources)
        {
            if (sources.Length == 0)
            {
                _delivery.OnCompleted(new PendingDrain(this));
                return;
            }

            Volatile.Write(ref _active, sources.Length);
            for (var i = 0; i < sources.Length; i++)
            {
                _pocket.Add(sources[i].Subscribe(new Element(this)));
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose() => _pocket.Dispose();

        /// <summary>Forwards a delivered value downstream when it differs from the last forwarded one.</summary>
        /// <param name="value">The merged value.</param>
        void IObserver<T>.OnNext(T value)
        {
            if (_hasLast && _comparer.Equals(_last, value))
            {
                return;
            }

            _last = value;
            _hasLast = true;
            _downstream.OnNext(value);
        }

        /// <summary>Forwards the delivered terminal error downstream.</summary>
        /// <param name="error">The error.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void IObserver<T>.OnError(Exception error) => _downstream.OnError(error);

        /// <summary>Forwards the delivered completion downstream.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void IObserver<T>.OnCompleted() => _downstream.OnCompleted();

        /// <summary>Queues a merged value for comparison, delivering it directly when nothing else is delivering.</summary>
        /// <param name="value">The merged value.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void Forward(T value) => _delivery.OnNext(this, value, new PendingDrain(this));

        /// <summary>Forwards the first terminal error and suppresses later notifications.</summary>
        /// <param name="error">The error.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ForwardError(Exception error) => _delivery.OnError(error, new PendingDrain(this));

        /// <summary>Decrements the active count and completes once every source has completed.</summary>
        private void Complete()
        {
            if (Interlocked.Decrement(ref _active) != 0)
            {
                return;
            }

            _delivery.OnCompleted(new PendingDrain(this));
        }

        /// <summary>Drains this sink's queued notifications for the delivery gate.</summary>
        /// <param name="Owner">The sink.</param>
        private readonly record struct PendingDrain(BlendUniqueSink<T> Owner) : IDrainTarget
        {
            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Drain() => _ = Owner._delivery.DrainTo(Owner);
        }

        /// <summary>Observes a single merged source.</summary>
        /// <param name="parent">The owning sink.</param>
        private sealed class Element(BlendUniqueSink<T> parent) : IObserver<T>
        {
            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void OnNext(T value) => parent.Forward(value);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void OnError(Exception error) => parent.ForwardError(error);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void OnCompleted() => parent.Complete();
        }
    }
}
