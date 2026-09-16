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

/// <summary>Mediates pair-by-index combination for <see cref="PairSignal{TLeft, TRight, TResult}"/>.</summary>
/// <typeparam name="TLeft">The left value type.</typeparam>
/// <typeparam name="TRight">The right value type.</typeparam>
/// <typeparam name="TResult">The result value type.</typeparam>
/// <remarks>
/// Each side's values and completion are applied inside a serialized delivery, so the projection and the downstream
/// observer run with no lock held. Notifications that arrive while another thread is delivering are queued and applied in
/// arrival order; the first error is delivered after them.
/// </remarks>
[System.Diagnostics.DebuggerDisplay("PairWitness: IsCompleted = {IsCompleted}, LeftQueue = {LeftQueue.Count}, RightQueue = {RightQueue.Count}")]
public sealed class PairWitness<TLeft, TRight, TResult>
{
    /// <summary>Serializes applying updates and downstream deliveries.</summary>
    private DeliveryGateState _delivery;

    /// <summary>Updates and the terminal notification queued while another thread delivers.</summary>
    private PendingNotifications<Update> _pending = new();

    /// <summary>Initializes a new instance of the <see cref="PairWitness{TLeft, TRight, TResult}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="selector">The result projection.</param>
    public PairWitness(IObserver<TResult> observer, Func<TLeft, TRight, TResult> selector)
    {
        Observer = observer;
        Selector = selector;
    }

    /// <summary>Gets the downstream observer.</summary>
    private IObserver<TResult> Observer { get; }

    /// <summary>Gets the result projection.</summary>
    private Func<TLeft, TRight, TResult> Selector { get; }

    /// <summary>Gets left values waiting for a partner; touched only inside a delivery.</summary>
    private Queue<TLeft> LeftQueue { get; } = new();

    /// <summary>Gets right values waiting for a partner; touched only inside a delivery.</summary>
    private Queue<TRight> RightQueue { get; } = new();

    /// <summary>Gets or sets a value indicating whether the left source completed; touched only inside a delivery.</summary>
    private bool IsLeftCompleted { get; set; }

    /// <summary>Gets or sets a value indicating whether the right source completed; touched only inside a delivery.</summary>
    private bool IsRightCompleted { get; set; }

    /// <summary>Gets or sets a value indicating whether completion has been requested; touched only inside a delivery.</summary>
    private bool IsCompleted { get; set; }

    /// <summary>Subscribes to both sources.</summary>
    /// <param name="left">The left source.</param>
    /// <param name="right">The right source.</param>
    /// <returns>The subscriptions.</returns>
    public MultipleDisposable Run(IObservable<TLeft> left, IObservable<TRight> right) =>
        new(
            left.Subscribe(OnLeftNext, OnError, OnLeftCompleted),
            right.Subscribe(OnRightNext, OnError, OnRightCompleted));

    /// <summary>Applies a left value.</summary>
    /// <param name="value">The left value.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void OnLeftNext(TLeft value) => Process(new(IsLeft: true, IsCompletion: false, value, default!));

    /// <summary>Applies a right value.</summary>
    /// <param name="value">The right value.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void OnRightNext(TRight value) => Process(new(IsLeft: false, IsCompletion: false, default!, value));

    /// <summary>Applies the left source's completion.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void OnLeftCompleted() => Process(new(IsLeft: true, IsCompletion: true, default!, default!));

    /// <summary>Applies the right source's completion.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void OnRightCompleted() => Process(new(IsLeft: false, IsCompletion: true, default!, default!));

    /// <summary>Applies an update directly when nothing else is delivering, otherwise queues it for the delivering thread.</summary>
    /// <param name="update">The update.</param>
    private void Process(Update update)
    {
        if (_pending.HasItems || !DeliveryGate.TryEnter(ref _delivery))
        {
            if (_pending.TryEnqueue(update))
            {
                DeliveryGate.Signal(ref _delivery, new PendingDrain(this));
            }

            return;
        }

        try
        {
            if (!_pending.IsTerminated)
            {
                Apply(update);
            }
        }
        catch
        {
            _ = DeliveryGate.Reset(ref _delivery);
            throw;
        }

        DeliveryGate.Exit(ref _delivery, new PendingDrain(this));
    }

    /// <summary>Requests the first error as the terminal notification.</summary>
    /// <param name="error">The error.</param>
    private void OnError(Exception error)
    {
        if (!_pending.TryRequestTerminal(error))
        {
            return;
        }

        DeliveryGate.Signal(ref _delivery, new PendingDrain(this));
    }

    /// <summary>Applies queued updates in order, then delivers the terminal notification.</summary>
    private void DrainPending()
    {
        while (true)
        {
            switch (_pending.TakeNext(out var update, out var error))
            {
                case PendingDelivery.Value:
                {
                    Apply(update);
                    break;
                }

                case PendingDelivery.Terminal when error is null:
                {
                    Observer.OnCompleted();
                    return;
                }

                case PendingDelivery.Terminal:
                {
                    Observer.OnError(error);
                    return;
                }

                default:
                {
                    return;
                }
            }
        }
    }

    /// <summary>Records an update, emits the pair it completes, and requests completion once no more pairs can form.</summary>
    /// <param name="update">The update.</param>
    private void Apply(in Update update)
    {
        if (IsCompleted)
        {
            return;
        }

        Record(update);
        if (LeftQueue.Count != 0 && RightQueue.Count != 0)
        {
            Observer.OnNext(Selector(LeftQueue.Dequeue(), RightQueue.Dequeue()));
        }

        TryComplete();
    }

    /// <summary>Queues a value for its side, or marks the side complete.</summary>
    /// <param name="update">The update.</param>
    private void Record(in Update update)
    {
        if (update.IsCompletion)
        {
            if (update.IsLeft)
            {
                IsLeftCompleted = true;
            }
            else
            {
                IsRightCompleted = true;
            }
        }
        else if (update.IsLeft)
        {
            LeftQueue.Enqueue(update.Left);
        }
        else
        {
            RightQueue.Enqueue(update.Right);
        }
    }

    /// <summary>Requests completion once a completed side has no value left to pair.</summary>
    private void TryComplete()
    {
        if ((!IsLeftCompleted || LeftQueue.Count != 0) && (!IsRightCompleted || RightQueue.Count != 0))
        {
            return;
        }

        IsCompleted = true;
        if (!_pending.TryRequestTerminal(null))
        {
            return;
        }

        DeliveryGate.Signal(ref _delivery, new PendingDrain(this));
    }

    /// <summary>A value or completion from one side.</summary>
    /// <param name="IsLeft">Whether the notification came from the left source.</param>
    /// <param name="IsCompletion">Whether the notification is the side's completion.</param>
    /// <param name="Left">The left value, for a left value.</param>
    /// <param name="Right">The right value, for a right value.</param>
    private readonly record struct Update(bool IsLeft, bool IsCompletion, TLeft Left, TRight Right);

    /// <summary>Drains this witness's queued updates for the delivery gate.</summary>
    /// <param name="Owner">The witness.</param>
    private readonly record struct PendingDrain(PairWitness<TLeft, TRight, TResult> Owner) : IDrainTarget
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Drain() => Owner.DrainPending();
    }
}
