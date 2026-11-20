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

/// <summary>Coordinates a two-source combine-latest operation.</summary>
/// <typeparam name="TLeft">The left value type.</typeparam>
/// <typeparam name="TRight">The right value type.</typeparam>
/// <typeparam name="TResult">The result value type.</typeparam>
/// <param name="observer">The downstream observer.</param>
/// <param name="selector">The projection function.</param>
[System.Diagnostics.DebuggerDisplay("CombineLatestCoordinator<{typeof(TLeft).Name,nq},{typeof(TRight).Name,nq}>")]
public sealed class CombineLatestCoordinator<TLeft, TRight, TResult>(IObserver<TResult> observer, Func<TLeft, TRight, TResult> selector) : IDrainTarget
{
    /// <summary>The downstream observer.</summary>
    private readonly IObserver<TResult> _observer = observer;

    /// <summary>The projection function.</summary>
    private readonly Func<TLeft, TRight, TResult> _selector = selector;

    /// <summary>Subscriptions to both arms, released on terminal delivery.</summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Usage", "CA2213:Disposable fields should be disposed", Justification = "Owned by the caller via Run and released early on terminal delivery.")]
    private MultipleDisposable? _subscriptions;

    /// <summary>Serializes downstream deliveries, so no lock is held while the projection or the observer runs.</summary>
    private DeliveryGateState _delivery;

    /// <summary>Updates and the terminal notification queued while another thread delivers.</summary>
    private PendingNotifications<Update> _pending = new();

    /// <summary>Whether the left source completed, as 0 or 1.</summary>
    private int _leftDone;

    /// <summary>Whether the right source completed, as 0 or 1.</summary>
    private int _rightDone;

    /// <summary>Number of sides that have completed (0, 1, or 2).</summary>
    private int _completedSides;

    /// <summary>A value indicating whether the left source has produced a value.</summary>
    private bool _hasLeft;

    /// <summary>A value indicating whether the right source has produced a value.</summary>
    private bool _hasRight;

    /// <summary>The latest left value.</summary>
    private TLeft? _latestLeft;

    /// <summary>The latest right value.</summary>
    private TRight? _latestRight;

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Drain() =>
        BinaryCoordinatorHelper.DrainPending(
            ref _pending,
            this,
            static (self, update) => self.Apply(in update),
            _observer,
            _subscriptions);

    /// <summary>Subscribes to both combine-latest sources.</summary>
    /// <param name="left">The left source.</param>
    /// <param name="right">The right source.</param>
    /// <returns>The subscription cleanup.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultipleDisposable Run(IObservable<TLeft> left, IObservable<TRight> right) =>
        BinaryCoordinatorHelper.SubscribeArms(
            this,
            out _subscriptions,
            left,
            new(static (self, v) => self.OnLeftNext(v), static (self, e) => self.OnError(e), static self => self.OnLeftCompleted()),
            right,
            new(static (self, v) => self.OnRightNext(v), static (self, e) => self.OnError(e), static self => self.OnRightCompleted()),
            static self => self._pending.IsTerminated);

    /// <summary>Records a left value directly when nothing else is delivering, otherwise queues it for the delivering thread.</summary>
    /// <param name="value">The left value.</param>
    private void OnLeftNext(TLeft value)
    {
        if (_pending.HasItems || !DeliveryGate.TryEnter(ref _delivery))
        {
            Queue(new(IsLeft: true, value, default!));
            return;
        }

        try
        {
            if (!_pending.IsTerminated)
            {
                _latestLeft = value;
                _hasLeft = true;
                EmitLatest();
            }
        }
        catch
        {
            _ = DeliveryGate.Reset(ref _delivery);
            throw;
        }

        DeliveryGate.Exit(ref _delivery, this);
    }

    /// <summary>Records a right value directly when nothing else is delivering, otherwise queues it for the delivering thread.</summary>
    /// <param name="value">The right value.</param>
    private void OnRightNext(TRight value)
    {
        if (_pending.HasItems || !DeliveryGate.TryEnter(ref _delivery))
        {
            Queue(new(IsLeft: false, default!, value));
            return;
        }

        try
        {
            if (!_pending.IsTerminated)
            {
                _latestRight = value;
                _hasRight = true;
                EmitLatest();
            }
        }
        catch
        {
            _ = DeliveryGate.Reset(ref _delivery);
            throw;
        }

        DeliveryGate.Exit(ref _delivery, this);
    }

    /// <summary>Queues an update for the delivering thread and signals it.</summary>
    /// <param name="update">The update.</param>
    private void Queue(Update update)
    {
        if (!_pending.TryEnqueue(update))
        {
            return;
        }

        DeliveryGate.Signal(ref _delivery, this);
    }

    /// <summary>Records an update and emits the projection once both sources have a value.</summary>
    /// <param name="update">The update.</param>
    private void Apply(in Update update)
    {
        if (update.IsLeft)
        {
            _latestLeft = update.Left;
            _hasLeft = true;
        }
        else
        {
            _latestRight = update.Right;
            _hasRight = true;
        }

        EmitLatest();
    }

    /// <summary>Emits the projection of the latest values once both sources have produced one.</summary>
    private void EmitLatest()
    {
        if (!_hasLeft || !_hasRight)
        {
            return;
        }

        TResult projected;
        try
        {
            projected = _selector(_latestLeft!, _latestRight!);
        }
        catch (Exception selectorError)
        {
            OnError(selectorError);
            return;
        }

        _observer.OnNext(projected);
    }

    /// <summary>Requests the first error as the terminal notification.</summary>
    /// <param name="error">The error.</param>
    private void OnError(Exception error)
    {
        if (!_pending.TryRequestTerminal(error))
        {
            return;
        }

        DeliveryGate.Signal(ref _delivery, this);
    }

    /// <summary>Marks the left source as complete.</summary>
    private void OnLeftCompleted()
    {
        if (BinaryCoordinatorHelper.TryCompleteBothSides(ref _leftDone, ref _completedSides)
            && _pending.TryRequestTerminal(null))
        {
            DeliveryGate.Signal(ref _delivery, this);
        }
    }

    /// <summary>Marks the right source as complete.</summary>
    private void OnRightCompleted()
    {
        if (BinaryCoordinatorHelper.TryCompleteBothSides(ref _rightDone, ref _completedSides)
            && _pending.TryRequestTerminal(null))
        {
            DeliveryGate.Signal(ref _delivery, this);
        }
    }

    /// <summary>A value from one side, queued while another thread delivers.</summary>
    /// <param name="IsLeft">Whether the value came from the left source.</param>
    /// <param name="Left">The left value, when <paramref name="IsLeft"/> is set.</param>
    /// <param name="Right">The right value, when <paramref name="IsLeft"/> is clear.</param>
    private readonly record struct Update(bool IsLeft, TLeft Left, TRight Right);
}
