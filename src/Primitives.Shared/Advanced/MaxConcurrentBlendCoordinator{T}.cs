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

/// <summary>Coordinates bounded-concurrency merging of enumerable observable sources.</summary>
/// <typeparam name="T">The value type.</typeparam>
/// <remarks>
/// Deliveries are serialized by a <see cref="SerializedDelivery{T}"/>, and the source enumerable is read by one thread at
/// a time through a request counter, so no lock is held while the observer or the enumerable runs. A thread that asks
/// for the next source while another thread is enumerating hands the request over and returns.
/// </remarks>
[System.Diagnostics.DebuggerDisplay("MaxConcurrentBlendCoordinator: Active = {_active}, EnumerationCompleted = {_enumerationCompleted}, Done = {_done}")]
public sealed class MaxConcurrentBlendCoordinator<T> : IDisposable
{
    /// <summary>Active subscriptions.</summary>
    private readonly MultipleDisposable _subscriptions = [];

    /// <summary>The downstream observer.</summary>
    private readonly IObserver<T> _observer;

    /// <summary>Serializes downstream deliveries.</summary>
    private SerializedDelivery<T> _delivery = new();

    /// <summary>The source enumerator.</summary>
    private IEnumerator<IObservable<T>>? _enumerator;

    /// <summary>The number of source requests not yet served; the thread that raises it from zero serves them.</summary>
    private long _requests;

    /// <summary>Remaining active participants (1 for enumeration plus 1 per active inner).</summary>
    private int _active = 1;

    /// <summary>Whether all enumerable sources have been consumed, as 0 or 1.</summary>
    private int _enumerationCompleted;

    /// <summary>Whether a terminal notification has been requested, as 0 or 1.</summary>
    private int _done;

    /// <summary>Initializes a new instance of the <see cref="MaxConcurrentBlendCoordinator{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    public MaxConcurrentBlendCoordinator(IObserver<T> observer) => _observer = observer;

    /// <inheritdoc/>
    public void Dispose()
    {
        Volatile.Write(ref _done, 1);
        var enumerator = Interlocked.Exchange(ref _enumerator, null);
        enumerator?.Dispose();
        _subscriptions.Dispose();
    }

    /// <summary>Starts bounded-concurrency merging.</summary>
    /// <param name="sources">The enumerable sources.</param>
    /// <param name="maxConcurrent">The maximum number of active inner subscriptions.</param>
    /// <returns>The subscription cleanup.</returns>
    public MaxConcurrentBlendCoordinator<T> Run(IEnumerable<IObservable<T>> sources, int maxConcurrent)
    {
        Volatile.Write(ref _enumerator, sources.GetEnumerator());
        RequestSources(maxConcurrent);
        return this;
    }

    /// <summary>Subscribes up to the requested number of sources, or hands the request to the thread already enumerating.</summary>
    /// <param name="count">The number of sources to request.</param>
    private void RequestSources(long count)
    {
        if (Interlocked.Add(ref _requests, count) != count)
        {
            return;
        }

        var missed = count;
        do
        {
            var remaining = missed;
            while (remaining > 0 && SubscribeNext())
            {
                remaining--;
            }

            missed = Interlocked.Add(ref _requests, -missed);
        }
        while (missed != 0);
    }

    /// <summary>Subscribes to the next enumerable source when one is available; runs only on the thread serving requests.</summary>
    /// <returns><see langword="true"/> when a new inner source was subscribed.</returns>
    private bool SubscribeNext()
    {
        var enumerator = Volatile.Read(ref _enumerator);
        if (Volatile.Read(ref _done) != 0)
        {
            DisposeEnumerator();
            return false;
        }

        if (Volatile.Read(ref _enumerationCompleted) != 0)
        {
            return false;
        }

        IObservable<T>? next;
        try
        {
            if (enumerator?.MoveNext() != true)
            {
                DisposeEnumerator();
                if (Interlocked.Exchange(ref _enumerationCompleted, 1) == 0
                    && Interlocked.Decrement(ref _active) == 0)
                {
                    Volatile.Write(ref _done, 1);
                    _delivery.OnCompleted(new PendingDrain(this));
                }

                return false;
            }

            next = enumerator.Current;
        }
        catch (Exception error) when (!FatalExceptionHelper.IsFatal(error))
        {
            OnAnyError(error);
            return false;
        }

        if (next is null)
        {
            OnAnyError(new InvalidOperationException("Blend source contained null."));
            return false;
        }

        _ = Interlocked.Increment(ref _active);
        Arm inner = new(this);
        _subscriptions.Add(inner);
        inner.Attach(next.Subscribe(inner));
        return true;
    }

    /// <summary>Forwards an inner value, directly when nothing else is delivering.</summary>
    /// <param name="value">The value to forward.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void OnInnerNext(T value) => _delivery.OnNext(_observer, value, new PendingDrain(this));

    /// <summary>Forwards the first terminal error; the subscriptions are released once it is delivered.</summary>
    /// <param name="error">The error to forward.</param>
    private void OnAnyError(Exception error)
    {
        Volatile.Write(ref _done, 1);
        _delivery.OnError(error, new PendingDrain(this));
    }

    /// <summary>Completes one inner source and starts another if possible.</summary>
    /// <param name="inner">The completed inner arm.</param>
    private void OnInnerCompleted(Arm inner)
    {
        _ = _subscriptions.Remove(inner);
        inner.Dispose();
        if (Volatile.Read(ref _done) != 0)
        {
            return;
        }

        if (Interlocked.Decrement(ref _active) == 0)
        {
            Volatile.Write(ref _done, 1);
            _delivery.OnCompleted(new PendingDrain(this));
            return;
        }

        RequestSources(1);
    }

    /// <summary>Releases the subscriptions after the terminal notification, and the enumerator on the thread serving requests.</summary>
    private void ReleaseAfterTerminal()
    {
        _subscriptions.Dispose();
        RequestSources(1);
    }

    /// <summary>Disposes the enumerable source exactly once.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void DisposeEnumerator() => Interlocked.Exchange(ref _enumerator, null)?.Dispose();

    /// <summary>Drains this coordinator's queued notifications for the delivery gate.</summary>
    /// <param name="Owner">The coordinator.</param>
    private readonly record struct PendingDrain(MaxConcurrentBlendCoordinator<T> Owner) : IDrainTarget
    {
        /// <inheritdoc/>
        public void Drain()
        {
            if (!Owner._delivery.DrainTo(Owner._observer))
            {
                return;
            }

            Owner.ReleaseAfterTerminal();
        }
    }

    /// <summary>Observes one inner source and detaches itself once completed.</summary>
    /// <param name="parent">The coordinator.</param>
    private sealed class Arm(MaxConcurrentBlendCoordinator<T> parent) : IObserver<T>, IDisposable
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
