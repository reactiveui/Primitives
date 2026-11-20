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

/// <summary>Mediates sequential concatenation for <see cref="ChainSignal{T}"/>.</summary>
/// <typeparam name="T">The value type.</typeparam>
/// <remarks>
/// The gate only guards the queue and flags. Deliveries are serialized by a <see cref="SerializedDelivery{T}"/> and the
/// next inner source is subscribed after the gate is released, so no lock is held while the observer or an inner source
/// runs. A terminal notification raised before <see cref="Dispose"/> is still delivered; nothing raised after it is.
/// </remarks>
[System.Diagnostics.DebuggerDisplay("ChainWitness: IsActive = {IsActive}, IsOuterCompleted = {IsOuterCompleted}")]
public sealed class ChainWitness<T> : IDisposable
{
    /// <summary>Guards the queue and flags; never held while user code runs.</summary>
    private readonly Lock _gate = new();

    /// <summary>Serializes downstream deliveries.</summary>
    private SerializedDelivery<T> _delivery = new();

    /// <summary>Whether a terminal notification has been queued or the witness disposed; written under the gate.</summary>
    private bool _done;

    /// <summary>Whether an inner subscription is currently inside <c>Subscribe</c>, guarded by <see cref="_gate"/>.</summary>
    private bool _subscribing;

    /// <summary>Whether the inner source being subscribed completed synchronously, guarded by <see cref="_gate"/>.</summary>
    private bool _completedSynchronously;

    /// <summary>The active inner subscription, guarded by <see cref="_gate"/>.</summary>
    private IDisposable? _activeInner;

    /// <summary>The observer every inner source is subscribed with; the sources run one at a time, so one serves them all.</summary>
    private Arm? _arm;

    /// <summary>Initializes a new instance of the <see cref="ChainWitness{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    public ChainWitness(IObserver<T> observer) => Observer = observer;

    /// <summary>Gets queued sources awaiting subscription.</summary>
    private Queue<IObservable<T>?> Queue { get; } = new();

    /// <summary>Gets active subscriptions.</summary>
    private MultipleDisposable Subscriptions { get; } = [];

    /// <summary>Gets the downstream observer.</summary>
    private IObserver<T> Observer { get; }

    /// <summary>Gets or sets a value indicating whether an inner source is active.</summary>
    private bool IsActive { get; set; }

    /// <summary>Gets or sets a value indicating whether the outer source completed.</summary>
    private bool IsOuterCompleted { get; set; }

    /// <inheritdoc/>
    public void Dispose()
    {
        IDisposable? inner;
        lock (_gate)
        {
            Volatile.Write(ref _done, true);
            Queue.Clear();
            inner = _activeInner;
            _activeInner = null;
        }

        inner?.Dispose();
        Subscriptions.Dispose();
    }

    /// <summary>Starts concatenating an outer observable of sources.</summary>
    /// <param name="sources">The outer source.</param>
    /// <returns>The observer that owns the subscriptions.</returns>
    public ChainWitness<T> Run(IObservable<IObservable<T>> sources)
    {
        OuterArm outer = new(this);
        Subscriptions.Add(outer);
        outer.Attach(sources.Subscribe(outer));
        return this;
    }

    /// <summary>Starts concatenating enumerable sources.</summary>
    /// <param name="sources">The sources to concatenate.</param>
    /// <returns>The observer that owns the subscriptions.</returns>
    public ChainWitness<T> Run(IEnumerable<IObservable<T>> sources)
    {
        lock (_gate)
        {
            foreach (var source in sources)
            {
                Queue.Enqueue(source);
            }

            IsOuterCompleted = true;
        }

        Drain();
        return this;
    }

    /// <summary>Starts concatenating two fixed sources.</summary>
    /// <param name="first">The first source.</param>
    /// <param name="second">The second source.</param>
    /// <returns>The observer that owns the subscriptions.</returns>
    public ChainWitness<T> Run(IObservable<T> first, IObservable<T> second)
    {
        lock (_gate)
        {
            Queue.Enqueue(first);
            Queue.Enqueue(second);
            IsOuterCompleted = true;
        }

        Drain();
        return this;
    }

    /// <summary>Queues a new inner source.</summary>
    /// <param name="source">The inner source.</param>
    private void OnSource(IObservable<T> source)
    {
        if (source is null)
        {
            OnError(new InvalidOperationException("Chain source contained null."));
            return;
        }

        lock (_gate)
        {
            if (_done)
            {
                return;
            }

            Queue.Enqueue(source);
        }

        Drain();
    }

    /// <summary>Marks the outer source complete.</summary>
    private void OnOuterCompleted()
    {
        lock (_gate)
        {
            IsOuterCompleted = true;
        }

        Drain();
    }

    /// <summary>Marks the active inner source complete.</summary>
    private void OnInnerCompleted()
    {
        IDisposable? finished = null;
        lock (_gate)
        {
            if (_done)
            {
                return;
            }

            if (_subscribing)
            {
                _completedSynchronously = true;
                return;
            }

            finished = _activeInner;
            _activeInner = null;
            IsActive = false;
        }

        finished?.Dispose();
        Drain();
    }

    /// <summary>Forwards an inner value unless the witness has terminated or been disposed.</summary>
    /// <param name="value">The value to forward.</param>
    private void OnInnerNext(T value)
    {
        if (Volatile.Read(ref _done))
        {
            return;
        }

        _delivery.OnNext(Observer, value, new PendingDrain(this));
    }

    /// <summary>Queues the first terminal error and stops subscribing further sources.</summary>
    /// <param name="error">The error to forward.</param>
    private void OnError(Exception error)
    {
        lock (_gate)
        {
            if (_done)
            {
                return;
            }

            Volatile.Write(ref _done, true);
            Queue.Clear();
            _ = _delivery.PostError(error);
        }

        _delivery.Flush(new PendingDrain(this));
    }

    /// <summary>Subscribes queued sources iteratively so synchronous sequences run in constant stack space.</summary>
    private void Drain()
    {
        while (TryAcquireNext(out var next, out var flushCompletion))
        {
            if (flushCompletion)
            {
                _delivery.Flush(new PendingDrain(this));
                return;
            }

            if (!SubscribeAndAdvance(next))
            {
                return;
            }
        }
    }

    /// <summary>Acquires the next queued source or posts completion when all sources have finished.</summary>
    /// <param name="next">The next source to subscribe, when available.</param>
    /// <param name="flushCompletion"><see langword="true"/> when completion was posted and must be flushed.</param>
    /// <returns><see langword="true"/> when the drain loop has work to perform.</returns>
    private bool TryAcquireNext(out IObservable<T>? next, out bool flushCompletion)
    {
        next = null;
        flushCompletion = false;
        lock (_gate)
        {
            if (_done)
            {
                Queue.Clear();
                return false;
            }

            if (IsActive)
            {
                return false;
            }

            if (Queue.Count > 0)
            {
                IsActive = true;
                _subscribing = true;
                _completedSynchronously = false;
                next = Queue.Dequeue();
                return true;
            }

            if (!IsOuterCompleted)
            {
                return false;
            }

            Volatile.Write(ref _done, true);
            _ = _delivery.PostCompleted();
            flushCompletion = true;
            return true;
        }
    }

    /// <summary>Subscribes an inner source and returns whether it completed synchronously so the loop should continue.</summary>
    /// <param name="next">The inner source to subscribe.</param>
    /// <returns><see langword="true"/> when the inner source completed synchronously and the drain loop should continue.</returns>
    private bool SubscribeAndAdvance(IObservable<T>? next)
    {
        if (next is null)
        {
            lock (_gate)
            {
                _subscribing = false;
                IsActive = false;
            }

            OnError(new InvalidOperationException("Chain source contained null."));
            return false;
        }

        IDisposable subscription;
        try
        {
            subscription = next.Subscribe(_arm ??= new(this));
        }
        catch (Exception error) when (!FatalExceptionHelper.IsFatal(error))
        {
            lock (_gate)
            {
                _subscribing = false;
                IsActive = false;
            }

            OnError(error);
            return false;
        }

        var loop = false;
        var disposeImmediately = false;
        lock (_gate)
        {
            _subscribing = false;
            if (_done)
            {
                disposeImmediately = true;
            }
            else if (_completedSynchronously)
            {
                _completedSynchronously = false;
                IsActive = false;
                disposeImmediately = true;
                loop = true;
            }
            else
            {
                _activeInner = subscription;
            }
        }

        if (disposeImmediately)
        {
            subscription.Dispose();
        }

        return loop;
    }

    /// <summary>Drains this witness's queued notifications for the delivery gate.</summary>
    /// <param name="Owner">The witness.</param>
    private readonly record struct PendingDrain(ChainWitness<T> Owner) : IDrainTarget
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
    /// <param name="parent">The witness.</param>
    private sealed class OuterArm(ChainWitness<T> parent) : IObserver<IObservable<T>>, IDisposable
    {
        /// <summary>The outer subscription.</summary>
        private IDisposable? _subscription;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(IObservable<T> value) => parent.OnSource(value);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => parent.OnError(error);

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

    /// <summary>Receives the notifications of whichever inner source is running and passes them to the witness.</summary>
    /// <param name="parent">The witness.</param>
    private sealed class Arm(ChainWitness<T> parent) : IObserver<T>
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(T value) => parent.OnInnerNext(value);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => parent.OnError(error);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted() => parent.OnInnerCompleted();
    }
}
