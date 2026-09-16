// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Extensions;
using ReactiveUI.Primitives.Internal;

namespace ReactiveUI.Primitives.Advanced;

/// <summary>
/// Serialized, non-blocking delivery to one downstream observer, held as a mutable field by the operator or subject that
/// delivers through it.
/// </summary>
/// <typeparam name="T">The value type.</typeparam>
/// <remarks>
/// A notification is delivered directly when nothing else is delivering; otherwise it is queued and delivered in arrival
/// order by the thread holding the <see cref="DeliveryGate"/>, so no lock is held while the observer runs. The first
/// terminal notification wins and nothing follows it. Keep the field non-readonly and call it in place, since a copy is a
/// separate queue. The owner passes a drain whose <see cref="IDrainTarget.Drain"/> calls <see cref="DrainTo"/> with the
/// same observer. <see cref="Post"/>, <see cref="PostError"/> and <see cref="PostCompleted"/> queue without delivering, so
/// an owner can fix the order under its own lock and call <see cref="Flush{TDrain}"/> after releasing it.
/// <see cref="Start{TReader, TDrain}"/> reads and delivers an initial value inside the delivery, so a value raised while the
/// initial value is being read is delivered after it.
/// </remarks>
[DebuggerDisplay("SerializedDelivery: Reentrant = {_reentrant}, {_pending}")]
public record struct SerializedDelivery<T>
{
    /// <summary>Whether a value raised on the delivering thread is delivered inside the running delivery.</summary>
    private readonly bool _reentrant;

    /// <summary>Serializes deliveries to the downstream observer.</summary>
    private DeliveryGateState _gate;

    /// <summary>Values and the terminal notification waiting for delivery.</summary>
    private PendingNotifications<T> _pending;

    /// <summary>Whether the delivering thread is reading the initial value; a value it raises meanwhile is queued, not nested.</summary>
    private bool _readingInitial;

    /// <summary>Initializes a new instance of the <see cref="SerializedDelivery{T}"/> struct.</summary>
    public SerializedDelivery() => _pending = new();

    /// <summary>Initializes a new instance of the <see cref="SerializedDelivery{T}"/> struct.</summary>
    /// <param name="reentrant">
    /// Whether a value raised on the delivering thread is delivered inside the running delivery, before the observer
    /// returns, instead of after it; values raised on other threads are still queued.
    /// </param>
    public SerializedDelivery(bool reentrant)
        : this() => _reentrant = reentrant;

    /// <summary>Gets a value indicating whether the terminal notification has been taken for delivery or delivery was stopped.</summary>
    public readonly bool IsTerminated
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => _pending.IsTerminated;
    }

    /// <summary>Delivers a value directly when nothing else is delivering, otherwise queues it behind the running delivery.</summary>
    /// <typeparam name="TDrain">The drain type.</typeparam>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="value">The value.</param>
    /// <param name="drain">Drains queued notifications into the same observer.</param>
    public void OnNext<TDrain>(IObserver<T> observer, T value, TDrain drain)
        where TDrain : IDrainTarget
    {
        if ((!_pending.HasItems && DeliveryGate.TryEnter(ref _gate)) || (_reentrant && !_readingInitial && _gate.TryEnterNested()))
        {
            DeliverEntered(observer, value, drain);
            return;
        }

        if (!_pending.TryEnqueue(value))
        {
            return;
        }

        DeliveryGate.Signal(ref _gate, drain);
    }

    /// <summary>Reads the initial value inside the delivery and delivers it before any value raised during the read.</summary>
    /// <typeparam name="TReader">The reader type.</typeparam>
    /// <typeparam name="TDrain">The drain type.</typeparam>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="reader">Reads the initial value.</param>
    /// <param name="drain">Drains notifications queued while the initial value was read and delivered.</param>
    /// <remarks>
    /// When another delivery is running or values are already queued, the value is read at once and queued behind them.
    /// A reader or observer failure propagates to the caller and leaves the delivery able to deliver the next value.
    /// </remarks>
    public void Start<TReader, TDrain>(IObserver<T> observer, TReader reader, TDrain drain)
        where TReader : ICurrentValueReader<T>
        where TDrain : IDrainTarget
    {
        if (_pending.HasItems || !DeliveryGate.TryEnter(ref _gate))
        {
            OnNext(observer, reader.Read(), drain);
            return;
        }

        try
        {
            if (!_pending.IsTerminated)
            {
                observer.OnNext(ReadInitial(reader));
            }
        }
        catch
        {
            _ = DeliveryGate.Reset(ref _gate);
            throw;
        }

        DeliveryGate.Exit(ref _gate, drain);
    }

    /// <summary>Delivers an error as the terminal notification, after any queued values.</summary>
    /// <typeparam name="TDrain">The drain type.</typeparam>
    /// <param name="error">The error.</param>
    /// <param name="drain">Drains queued notifications into the downstream observer.</param>
    /// <exception cref="ArgumentNullException"><paramref name="error"/> is <see langword="null"/>.</exception>
    public void OnError<TDrain>(Exception error, TDrain drain)
        where TDrain : IDrainTarget
    {
        if (!PostError(error))
        {
            return;
        }

        DeliveryGate.Signal(ref _gate, drain);
    }

    /// <summary>Delivers completion as the terminal notification, after any queued values.</summary>
    /// <typeparam name="TDrain">The drain type.</typeparam>
    /// <param name="drain">Drains queued notifications into the downstream observer.</param>
    public void OnCompleted<TDrain>(TDrain drain)
        where TDrain : IDrainTarget
    {
        if (!PostCompleted())
        {
            return;
        }

        DeliveryGate.Signal(ref _gate, drain);
    }

    /// <summary>Queues a value without delivering it.</summary>
    /// <param name="value">The value to queue.</param>
    /// <returns><see langword="true"/> when the value was queued; <see langword="false"/> once a terminal notification is queued.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Post(T value) => _pending.TryEnqueue(value);

    /// <summary>Queues an error as the terminal notification without delivering it.</summary>
    /// <param name="error">The error.</param>
    /// <returns><see langword="true"/> when this is the first terminal notification.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="error"/> is <see langword="null"/>.</exception>
    public bool PostError(Exception error)
    {
        ArgumentExceptionHelper.ThrowIfNull(error);
        return _pending.TryRequestTerminal(error);
    }

    /// <summary>Queues completion as the terminal notification without delivering it.</summary>
    /// <returns><see langword="true"/> when this is the first terminal notification.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool PostCompleted() => _pending.TryRequestTerminal(null);

    /// <summary>Delivers queued notifications on the calling thread, or hands them to the thread already delivering.</summary>
    /// <typeparam name="TDrain">The drain type.</typeparam>
    /// <param name="drain">Drains queued notifications into the downstream observer.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Flush<TDrain>(TDrain drain)
        where TDrain : IDrainTarget =>
        DeliveryGate.Signal(ref _gate, drain);

    /// <summary>Stops delivery; queued notifications are dropped and nothing further is queued or delivered.</summary>
    /// <remarks>It never waits, so a notification already taken for delivery on another thread can still arrive after it returns.</remarks>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Stop() => _pending.Stop();

    /// <summary>Delivers queued values in order, then the terminal notification once nothing is left.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <returns><see langword="true"/> when this call delivered the terminal notification.</returns>
    public bool DrainTo(IObserver<T> observer)
    {
        while (true)
        {
            switch (_pending.TakeNext(out var value, out var error))
            {
                case PendingDelivery.Value:
                {
                    observer.OnNext(value);
                    break;
                }

                case PendingDelivery.Terminal when error is null:
                {
                    observer.OnCompleted();
                    return true;
                }

                case PendingDelivery.Terminal:
                {
                    observer.OnError(error);
                    return true;
                }

                default:
                {
                    return false;
                }
            }
        }
    }

    /// <summary>Claims the delivery when nothing is queued or delivering, so a caller holding its own lock can deliver after releasing it.</summary>
    /// <returns><see langword="true"/> when the caller owns the delivery and must call <see cref="DeliverClaimed{TDrain}(IObserver{T}, T, TDrain)"/>.</returns>
    /// <remarks>
    /// Only a gate compare-and-swap runs, so it is safe to call under the caller's lock. A successful claim holds the
    /// delivery until a <c>DeliverClaimed</c> call releases it; nothing else is delivered meanwhile.
    /// </remarks>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryClaim() => !_pending.HasItems && DeliveryGate.TryEnter(ref _gate);

    /// <summary>Delivers a value on a delivery claimed by <see cref="TryClaim"/>, then drains anything queued meanwhile.</summary>
    /// <typeparam name="TDrain">The drain type.</typeparam>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="value">The value.</param>
    /// <param name="drain">Drains notifications queued while the value was delivered.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void DeliverClaimed<TDrain>(IObserver<T> observer, T value, TDrain drain)
        where TDrain : IDrainTarget =>
        DeliverEntered(observer, value, drain);

    /// <summary>Delivers values in order on a delivery claimed by <see cref="TryClaim"/>, then drains anything queued meanwhile.</summary>
    /// <typeparam name="TDrain">The drain type.</typeparam>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="values">The values, delivered first to last.</param>
    /// <param name="drain">Drains notifications queued while the values were delivered.</param>
    public void DeliverClaimed<TDrain>(IObserver<T> observer, T[] values, TDrain drain)
        where TDrain : IDrainTarget
    {
        try
        {
            for (var i = 0; i < values.Length && !_pending.IsTerminated; i++)
            {
                observer.OnNext(values[i]);
            }
        }
        catch
        {
            _ = DeliveryGate.Reset(ref _gate);
            throw;
        }

        DeliveryGate.Exit(ref _gate, drain);
    }

    /// <summary>Reads the initial value with nested delivery suspended, so a value raised by the read is queued behind it.</summary>
    /// <typeparam name="TReader">The reader type.</typeparam>
    /// <param name="reader">Reads the initial value.</param>
    /// <returns>The initial value.</returns>
    private T ReadInitial<TReader>(TReader reader)
        where TReader : ICurrentValueReader<T>
    {
        _readingInitial = true;
        try
        {
            return reader.Read();
        }
        finally
        {
            _readingInitial = false;
        }
    }

    /// <summary>Delivers a value on a gate this thread entered while nothing was queued.</summary>
    /// <typeparam name="TDrain">The drain type.</typeparam>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="value">The value.</param>
    /// <param name="drain">Drains notifications queued meanwhile.</param>
    private void DeliverEntered<TDrain>(IObserver<T> observer, T value, TDrain drain)
        where TDrain : IDrainTarget
    {
        try
        {
            if (!_pending.IsTerminated)
            {
                observer.OnNext(value);
            }
        }
        catch
        {
            _ = DeliveryGate.Reset(ref _gate);
            throw;
        }

        DeliveryGate.Exit(ref _gate, drain);
    }
}
