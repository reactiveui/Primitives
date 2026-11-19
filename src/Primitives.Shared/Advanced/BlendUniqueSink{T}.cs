// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Extensions;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Forwards distinct merged values downstream and tears down every source on dispose.</summary>
/// <typeparam name="T">The element type.</typeparam>
/// <param name="downstream">The downstream observer.</param>
/// <param name="comparer">The equality comparer used to suppress duplicates.</param>
/// <remarks>
/// Merged values pass through a <see cref="SerializedDelivery{T}"/> whose observer is this sink, so the comparison and
/// the downstream observer run serialized without any lock held. Values that arrive while another thread is delivering
/// are compared and delivered in arrival order.
/// </remarks>
[System.Diagnostics.DebuggerDisplay("BlendUniqueSink: Pocket = {_pocket}")]
public sealed class BlendUniqueSink<T>(IObserver<T> downstream, IEqualityComparer<T> comparer) : IDisposable, IObserver<T>
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
        SinkDelivery.Next(_downstream, value, this);
    }

    /// <summary>Forwards the delivered terminal error downstream and tears down remaining sources.</summary>
    /// <param name="error">The error.</param>
    void IObserver<T>.OnError(Exception error)
    {
        try
        {
            _downstream.OnError(error);
        }
        finally
        {
            Dispose();
        }
    }

    /// <summary>Forwards the delivered completion downstream and tears down sources.</summary>
    void IObserver<T>.OnCompleted()
    {
        try
        {
            _downstream.OnCompleted();
        }
        finally
        {
            Dispose();
        }
    }

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
        /// <summary>Non-zero after this source emits a terminal notification.</summary>
        private int _done;

        /// <inheritdoc/>
        public void OnNext(T value)
        {
            if (Volatile.Read(ref _done) == 0)
            {
                parent.Forward(value);
            }
        }

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
            if (Interlocked.Exchange(ref _done, 1) == 0)
            {
                parent.ForwardError(error);
            }
        }

        /// <inheritdoc/>
        public void OnCompleted()
        {
            if (Interlocked.Exchange(ref _done, 1) == 0)
            {
                parent.Complete();
            }
        }
    }
}
