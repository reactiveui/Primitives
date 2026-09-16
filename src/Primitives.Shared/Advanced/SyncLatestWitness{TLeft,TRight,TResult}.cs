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

/// <summary>Mediates latest-value combination for <see cref="SyncLatestSignal{TLeft, TRight, TResult}"/>.</summary>
/// <typeparam name="TLeft">The left value type.</typeparam>
/// <typeparam name="TRight">The right value type.</typeparam>
/// <typeparam name="TResult">The result value type.</typeparam>
/// <remarks>
/// Each side's values are recorded inside a serialized delivery, so the projection and the downstream observer run with no
/// lock held. Values that arrive while another thread is delivering are queued and combined in arrival order; the first
/// error, or completion once both sources complete, is delivered after them.
/// </remarks>
[System.Diagnostics.DebuggerDisplay("SyncLatestWitness: HasLeft = {HasLeft}, HasRight = {HasRight}, IsCompleted = {IsCompleted}")]
public sealed class SyncLatestWitness<TLeft, TRight, TResult>
{
    /// <summary>Serializes recording values and downstream deliveries.</summary>
    private DeliveryGateState _delivery;

    /// <summary>Values and the terminal notification queued while another thread delivers.</summary>
    private PendingNotifications<Update> _pending = new();

    /// <summary>Whether the left source completed, as 0 or 1.</summary>
    private int _leftDone;

    /// <summary>Whether the right source completed, as 0 or 1.</summary>
    private int _rightDone;

    /// <summary>Initializes a new instance of the <see cref="SyncLatestWitness{TLeft, TRight, TResult}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="selector">The result projection.</param>
    public SyncLatestWitness(IObserver<TResult> observer, Func<TLeft, TRight, TResult> selector)
    {
        Observer = observer;
        Selector = selector;
    }

    /// <summary>Gets the downstream observer.</summary>
    private IObserver<TResult> Observer { get; }

    /// <summary>Gets the result projection.</summary>
    private Func<TLeft, TRight, TResult> Selector { get; }

    /// <summary>Gets or sets a value indicating whether the left source has produced a value; touched only inside a delivery.</summary>
    private bool HasLeft { get; set; }

    /// <summary>Gets or sets a value indicating whether the right source has produced a value; touched only inside a delivery.</summary>
    private bool HasRight { get; set; }

    /// <summary>Gets a value indicating whether the terminal notification has been taken for delivery.</summary>
    private bool IsCompleted => _pending.IsTerminated;

    /// <summary>Gets or sets the latest left value; touched only inside a delivery.</summary>
    private TLeft? LatestLeft { get; set; }

    /// <summary>Gets or sets the latest right value; touched only inside a delivery.</summary>
    private TRight? LatestRight { get; set; }

    /// <summary>Subscribes to both sources.</summary>
    /// <param name="left">The left source.</param>
    /// <param name="right">The right source.</param>
    /// <returns>The subscriptions.</returns>
    public MultipleDisposable Run(IObservable<TLeft> left, IObservable<TRight> right) =>
        new(
            left.Subscribe(OnLeftNext, OnError, OnLeftCompleted),
            right.Subscribe(OnRightNext, OnError, OnRightCompleted));

    /// <summary>Records a left value.</summary>
    /// <param name="value">The left value.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void OnLeftNext(TLeft value) => Process(new(IsLeft: true, value, default!));

    /// <summary>Records a right value.</summary>
    /// <param name="value">The right value.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void OnRightNext(TRight value) => Process(new(IsLeft: false, default!, value));

    /// <summary>Records a value directly when nothing else is delivering, otherwise queues it for the delivering thread.</summary>
    /// <param name="update">The value and its side.</param>
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
            if (!IsCompleted)
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

    /// <summary>Marks the left source complete.</summary>
    private void OnLeftCompleted()
    {
        Volatile.Write(ref _leftDone, 1);
        TryComplete();
    }

    /// <summary>Marks the right source complete.</summary>
    private void OnRightCompleted()
    {
        Volatile.Write(ref _rightDone, 1);
        TryComplete();
    }

    /// <summary>Requests completion once both sources have completed.</summary>
    private void TryComplete()
    {
        if (Volatile.Read(ref _leftDone) == 0 || Volatile.Read(ref _rightDone) == 0 || !_pending.TryRequestTerminal(null))
        {
            return;
        }

        DeliveryGate.Signal(ref _delivery, new PendingDrain(this));
    }

    /// <summary>Combines queued values in order, then delivers the terminal notification.</summary>
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

    /// <summary>Records a value and emits the projection once both sources have produced one.</summary>
    /// <param name="update">The value and its side.</param>
    private void Apply(in Update update)
    {
        if (update.IsLeft)
        {
            LatestLeft = update.Left;
            HasLeft = true;
        }
        else
        {
            LatestRight = update.Right;
            HasRight = true;
        }

        if (!HasLeft || !HasRight)
        {
            return;
        }

        Observer.OnNext(Selector(LatestLeft!, LatestRight!));
    }

    /// <summary>A value from one side.</summary>
    /// <param name="IsLeft">Whether the value came from the left source.</param>
    /// <param name="Left">The left value, when <paramref name="IsLeft"/> is set.</param>
    /// <param name="Right">The right value, when <paramref name="IsLeft"/> is clear.</param>
    private readonly record struct Update(bool IsLeft, TLeft Left, TRight Right);

    /// <summary>Drains this witness's queued values for the delivery gate.</summary>
    /// <param name="Owner">The witness.</param>
    private readonly record struct PendingDrain(SyncLatestWitness<TLeft, TRight, TResult> Owner) : IDrainTarget
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Drain() => Owner.DrainPending();
    }
}
