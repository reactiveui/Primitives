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
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Coordinates latest values, completion, and errors for a multi-source combine-latest subscription.</summary>
/// <typeparam name="TResult">The projected result type.</typeparam>
[System.Diagnostics.DebuggerDisplay("CombineLatestCoordinator: Sources = {_slots.Count}, MissingValues = {_missingValues}")]
public sealed class CombineLatestCoordinator<TResult> : IDisposable, IDrainTarget
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
