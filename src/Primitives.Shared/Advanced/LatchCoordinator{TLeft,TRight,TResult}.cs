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

/// <summary>Coordinates the general WithLatest projection for <c>Latch</c>.</summary>
/// <typeparam name="TLeft">The left value type.</typeparam>
/// <typeparam name="TRight">The right value type.</typeparam>
/// <typeparam name="TResult">The result value type.</typeparam>
/// <param name="observer">The downstream observer.</param>
/// <param name="selector">The projection function.</param>
[System.Diagnostics.DebuggerDisplay("LatchCoordinator<{typeof(TLeft).Name,nq},{typeof(TRight).Name,nq}>")]
public sealed class LatchCoordinator<TLeft, TRight, TResult>(IObserver<TResult> observer, Func<TLeft, TRight, TResult> selector)
{
    /// <summary>Guards the latest-right state.</summary>
    private readonly Lock _gate = new();

    /// <summary>The downstream observer.</summary>
    private readonly IObserver<TResult> _observer = observer;

    /// <summary>The projection function.</summary>
    private readonly Func<TLeft, TRight, TResult> _selector = selector;

    /// <summary>Subscriptions to both arms, released on terminal delivery.</summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Usage", "CA2213:Disposable fields should be disposed", Justification = "Owned by the caller via Run and released early on terminal delivery.")]
    private MultipleDisposable? _subscriptions;

    /// <summary>Serializes downstream deliveries across left emissions and right errors.</summary>
    private SerializedDelivery<TResult> _delivery = new();

    /// <summary>A value indicating whether the right source has produced a value.</summary>
    private bool _hasRight;

    /// <summary>The latest right value.</summary>
    private TRight? _latestRight;

    /// <summary>Subscribes to both sources.</summary>
    /// <param name="left">The left source.</param>
    /// <param name="right">The right source.</param>
    /// <returns>The subscription cleanup.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultipleDisposable Run(IObservable<TLeft> left, IObservable<TRight> right) =>
        BinaryCoordinatorHelper.SubscribeArms(
            this,
            out _subscriptions,
            right,
            new(static (self, v) => self.OnRightNext(v), static (self, e) => self.OnAnyError(e), static _ => { }, DisposeArmOnCompleted: true),
            left,
            new(static (self, v) => self.OnLeftNext(v), static (self, e) => self.OnAnyError(e), static self => self.OnLeftCompleted()),
            static self => self._delivery.IsTerminated);

    /// <summary>Stores the latest right value.</summary>
    /// <param name="value">The right value.</param>
    private void OnRightNext(TRight value)
    {
        lock (_gate)
        {
            _hasRight = true;
            _latestRight = value;
        }
    }

    /// <summary>Projects a left value with the latest right value when available.</summary>
    /// <param name="value">The left value.</param>
    private void OnLeftNext(TLeft value)
    {
        if (_delivery.IsTerminated)
        {
            return;
        }

        TRight rightValue;
        lock (_gate)
        {
            if (!_hasRight)
            {
                return;
            }

            rightValue = _latestRight!;
        }

        TResult projected;
        try
        {
            projected = _selector(value, rightValue);
        }
        catch (Exception error)
        {
            OnAnyError(error);
            return;
        }

        _delivery.OnNext(_observer, projected, new PendingDrain(this));
    }

    /// <summary>Forwards the first error from either arm and disposes both subscriptions.</summary>
    /// <param name="error">The error.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void OnAnyError(Exception error) => _delivery.OnError(error, new PendingDrain(this));

    /// <summary>Completes when the left source completes and disposes both subscriptions.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void OnLeftCompleted() => _delivery.OnCompleted(new PendingDrain(this));

    /// <summary>Drains queued notifications and releases both arms on terminal delivery.</summary>
    /// <param name="Owner">The coordinator.</param>
    private readonly record struct PendingDrain(LatchCoordinator<TLeft, TRight, TResult> Owner) : IDrainTarget
    {
        /// <inheritdoc/>
        public void Drain()
        {
            if (Owner._delivery.DrainTo(Owner._observer))
            {
                // Run stores the slot before subscribing either arm, and terminals only arrive through an arm.
                Owner._subscriptions!.Dispose();
            }
        }
    }
}
