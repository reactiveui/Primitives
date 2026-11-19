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

/// <summary>Mediates concurrent merging for <see cref="BlendSignal{T}"/> and <see cref="EnumerableBlendSignal{T}"/>.</summary>
/// <typeparam name="T">The value type.</typeparam>
/// <remarks>
/// Deliveries are serialized by a <see cref="SerializedDelivery{T}"/>, so no lock is held while the downstream observer
/// runs. Values that arrive while another thread is delivering are queued and delivered in arrival order; a value raised
/// by the delivering thread itself is delivered after the observer returns.
/// </remarks>
[System.Diagnostics.DebuggerDisplay("BlendWitness: Participants = {_participants}, OuterCompleted = {_outerCompleted}")]
public sealed class BlendWitness<T> : IDisposable
{
    /// <summary>Serializes downstream deliveries.</summary>
    private SerializedDelivery<T> _delivery = new();

    /// <summary>Remaining active participants (1 for the outer source plus 1 per active inner source).</summary>
    private int _participants = 1;

    /// <summary>Whether the outer source completed, as 0 or 1.</summary>
    private int _outerCompleted;

    /// <summary>Initializes a new instance of the <see cref="BlendWitness{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    public BlendWitness(IObserver<T> observer) => Observer = observer;

    /// <summary>Gets the active subscriptions.</summary>
    private MultipleDisposable Subscriptions { get; } = [];

    /// <summary>Gets the downstream observer.</summary>
    private IObserver<T> Observer { get; }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose() => Subscriptions.Dispose();

    /// <summary>Starts merging an outer observable of inner sources.</summary>
    /// <param name="sources">The outer source.</param>
    /// <returns>The observer that owns the subscriptions.</returns>
    public BlendWitness<T> Run(IObservable<IObservable<T>> sources)
    {
        OuterArm outer = new(this);
        Subscriptions.Add(outer);
        outer.Attach(sources.Subscribe(outer));
        return this;
    }

    /// <summary>Starts merging enumerable sources.</summary>
    /// <param name="sources">The sources to merge.</param>
    /// <returns>The observer that owns the subscriptions.</returns>
    public BlendWitness<T> Run(IEnumerable<IObservable<T>> sources)
    {
        foreach (var source in sources)
        {
            OnSource(source);
        }

        OnOuterCompleted();
        return this;
    }

    /// <summary>Subscribes to a new inner source.</summary>
    /// <param name="source">The inner source.</param>
    private void OnSource(IObservable<T> source)
    {
        if (source is null)
        {
            OnAnyError(new InvalidOperationException("Blend source contained null."));
            return;
        }

        if (_delivery.IsTerminated)
        {
            return;
        }

        _ = Interlocked.Increment(ref _participants);
        InnerArm arm = new(this);
        Subscriptions.Add(arm);
        arm.Attach(source.Subscribe(arm));
    }

    /// <summary>Forwards an inner value, directly when nothing else is delivering.</summary>
    /// <param name="value">The value to forward.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void OnInnerNext(T value) => _delivery.OnNext(Observer, value, new PendingDrain(this));

    /// <summary>Forwards the first terminal error.</summary>
    /// <param name="error">The error to forward.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void OnAnyError(Exception error) => _delivery.OnError(error, new PendingDrain(this));

    /// <summary>Marks one inner source complete and detaches its subscription handle.</summary>
    /// <param name="arm">The completed inner arm.</param>
    private void OnInnerCompleted(InnerArm arm)
    {
        _ = Subscriptions.Remove(arm);
        arm.Dispose();
        if (Interlocked.Decrement(ref _participants) == 0)
        {
            _delivery.OnCompleted(new PendingDrain(this));
        }
    }

    /// <summary>Marks the outer source complete.</summary>
    private void OnOuterCompleted()
    {
        if (Interlocked.Exchange(ref _outerCompleted, 1) != 0)
        {
            return;
        }

        if (Interlocked.Decrement(ref _participants) == 0)
        {
            _delivery.OnCompleted(new PendingDrain(this));
        }
    }

    /// <summary>Drains this witness's queued notifications for the delivery gate.</summary>
    /// <param name="Owner">The witness.</param>
    private readonly record struct PendingDrain(BlendWitness<T> Owner) : IDrainTarget
    {
        /// <inheritdoc/>
        public void Drain()
        {
            if (Owner._delivery.DrainTo(Owner.Observer))
            {
                Owner.Dispose();
            }
        }
    }

    /// <summary>Observes the outer sequence of inner sources.</summary>
    /// <param name="parent">The owning witness.</param>
    private sealed class OuterArm(BlendWitness<T> parent) : IObserver<IObservable<T>>, IDisposable
    {
        /// <summary>The outer subscription.</summary>
        private IDisposable? _subscription;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(IObservable<T> value) => parent.OnSource(value);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => parent.OnAnyError(error);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted() => parent.OnOuterCompleted();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose() => SinkSubscription.Dispose(ref _subscription);

        /// <summary>Assigns the outer subscription.</summary>
        /// <param name="subscription">The outer subscription.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Attach(IDisposable subscription) => SinkSubscription.Set(ref _subscription, subscription);
    }

    /// <summary>Receives the notifications of one source and passes them to the witness.</summary>
    /// <param name="parent">The witness.</param>
    private sealed class InnerArm(BlendWitness<T> parent) : IObserver<T>, IDisposable
    {
        /// <summary>The inner subscription.</summary>
        private IDisposable? _subscription;

        /// <summary>Set to 1 once this inner source has completed.</summary>
        private int _completed;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(T value) => parent.OnInnerNext(value);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => parent.OnAnyError(error);

        /// <inheritdoc/>
        public void OnCompleted()
        {
            if (Interlocked.Exchange(ref _completed, 1) != 0)
            {
                return;
            }

            parent.OnInnerCompleted(this);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose() => SinkSubscription.Dispose(ref _subscription);

        /// <summary>Assigns the inner subscription.</summary>
        /// <param name="subscription">The inner subscription.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Attach(IDisposable subscription) => SinkSubscription.Set(ref _subscription, subscription);
    }
}
