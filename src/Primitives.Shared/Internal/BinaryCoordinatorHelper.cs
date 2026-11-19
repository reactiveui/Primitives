// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Extensions;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Internal;
#else
namespace ReactiveUI.Primitives.Internal;
#endif

/// <summary>Shared validation, range-specialization, arm subscription, and two-side completion helpers for binary coordinators.</summary>
internal static class BinaryCoordinatorHelper
{
    /// <summary>Total number of arms in a binary coordinator.</summary>
    private const int BothSidesCount = 2;

    /// <summary>Validates binary inputs (<paramref name="left"/>, <paramref name="right"/>, <paramref name="selector"/>) and checks whether both sources are <see cref="RangeSignal"/>.</summary>
    /// <typeparam name="TLeft">The left element type.</typeparam>
    /// <typeparam name="TRight">The right element type.</typeparam>
    /// <typeparam name="TResult">The result element type.</typeparam>
    /// <param name="left">The left source.</param>
    /// <param name="right">The right source.</param>
    /// <param name="selector">The binary projection.</param>
    /// <param name="ranges">Receives the matched integer ranges and cast selector when applicable.</param>
    /// <returns><see langword="true"/> when both sources are <see cref="RangeSignal"/>; otherwise, <see langword="false"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="left"/>, <paramref name="right"/>, or <paramref name="selector"/> is <see langword="null"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static bool ValidateAndTryMatchRanges<TLeft, TRight, TResult>(
        IObservable<TLeft> left,
        IObservable<TRight> right,
        Func<TLeft, TRight, TResult> selector,
        out BinaryIntRanges<TResult> ranges)
    {
        ArgumentExceptionHelper.ThrowIfNull(left);
        ArgumentExceptionHelper.ThrowIfNull(right);
        ArgumentExceptionHelper.ThrowIfNull(selector);

        if (typeof(TLeft) == typeof(int)
            && typeof(TRight) == typeof(int)
            && left is RangeSignal leftRange
            && right is RangeSignal rightRange)
        {
            ranges = new(leftRange, rightRange, Unsafe.As<Func<int, int, TResult>>(selector));
            return true;
        }

        ranges = default;
        return false;
    }

    /// <summary>
    /// Subscribes <paramref name="first"/> and, when <paramref name="isStopped"/> remains <see langword="false"/>,
    /// subscribes <paramref name="second"/>, collecting both arms into <paramref name="subscriptionsSlot"/>.
    /// </summary>
    /// <typeparam name="TOwner">The owning coordinator or witness type.</typeparam>
    /// <typeparam name="TFirst">The first subscribed source element type.</typeparam>
    /// <typeparam name="TSecond">The second subscribed source element type.</typeparam>
    /// <param name="owner">The owning coordinator or witness.</param>
    /// <param name="subscriptionsSlot">Receives the created <see cref="MultipleDisposable"/> before subscribing.</param>
    /// <param name="first">The first source to subscribe.</param>
    /// <param name="firstCallbacks">Callbacks for the first source arm.</param>
    /// <param name="second">The second source to subscribe.</param>
    /// <param name="secondCallbacks">Callbacks for the second source arm.</param>
    /// <param name="isStopped">Checks whether subscribing <paramref name="first"/> already terminated the coordinator.</param>
    /// <returns>The composite subscription owning both arms.</returns>
    internal static MultipleDisposable SubscribeArms<TOwner, TFirst, TSecond>(
        TOwner owner,
        out MultipleDisposable subscriptionsSlot,
        IObservable<TFirst> first,
        in BinaryArmCallbacks<TOwner, TFirst> firstCallbacks,
        IObservable<TSecond> second,
        in BinaryArmCallbacks<TOwner, TSecond> secondCallbacks,
        Func<TOwner, bool> isStopped)
    {
        MultipleDisposable subscriptions = [];
        subscriptionsSlot = subscriptions;
        ForwardingObserverArm<TOwner, TFirst> firstArm = new(owner, firstCallbacks);
        subscriptions.Add(firstArm);
        firstArm.Attach(first.Subscribe(firstArm));
        if (!isStopped(owner))
        {
            ForwardingObserverArm<TOwner, TSecond> secondArm = new(owner, secondCallbacks);
            subscriptions.Add(secondArm);
            secondArm.Attach(second.Subscribe(secondArm));
        }

        return subscriptions;
    }

    /// <summary>Marks one side of a two-source coordinator complete once and reports whether both sides have completed.</summary>
    /// <param name="sideDone">The 0/1 completion flag for this side.</param>
    /// <param name="completedSides">The shared counter of completed sides.</param>
    /// <returns><see langword="true"/> when this call transitioned the second side to completed; otherwise, <see langword="false"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static bool TryCompleteBothSides(ref int sideDone, ref int completedSides) =>
        Interlocked.Exchange(ref sideDone, 1) == 0 && Interlocked.Increment(ref completedSides) == BothSidesCount;

    /// <summary>Creates an initialized <see cref="PendingNotifications{TItem}"/> queue.</summary>
    /// <typeparam name="TItem">The queued update type.</typeparam>
    /// <returns>An empty pending notification queue.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static PendingNotifications<TItem> CreatePending<TItem>() => new();

    /// <summary>
    /// Drains queued updates through <paramref name="apply"/> and forwards any terminal notification
    /// to <paramref name="observer"/> while disposing <paramref name="subscriptions"/>.
    /// </summary>
    /// <typeparam name="TOwner">The owning coordinator or witness type.</typeparam>
    /// <typeparam name="TItem">The queued update type.</typeparam>
    /// <typeparam name="TOutput">The downstream element type.</typeparam>
    /// <param name="pending">The pending notification queue.</param>
    /// <param name="owner">The owning coordinator or witness.</param>
    /// <param name="apply">Applies a single dequeued item.</param>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="subscriptions">The upstream subscriptions to dispose on terminal delivery.</param>
    internal static void DrainPending<TOwner, TItem, TOutput>(
        ref PendingNotifications<TItem> pending,
        TOwner owner,
        Action<TOwner, TItem> apply,
        IObserver<TOutput> observer,
        IDisposable? subscriptions)
    {
        while (true)
        {
            switch (pending.TakeNext(out var update, out var error))
            {
                case PendingDelivery.Value:
                {
                    apply(owner, update);
                    break;
                }

                case PendingDelivery.Terminal when error is null:
                {
                    try
                    {
                        observer.OnCompleted();
                    }
                    finally
                    {
                        subscriptions?.Dispose();
                    }

                    return;
                }

                case PendingDelivery.Terminal:
                {
                    try
                    {
                        observer.OnError(error);
                    }
                    finally
                    {
                        subscriptions?.Dispose();
                    }

                    return;
                }

                default:
                {
                    return;
                }
            }
        }
    }
}
