// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Signals;
#else
namespace ReactiveUI.Primitives.Signals;
#endif

/// <summary>Factory surface for the multicast subject signals, for callers who prefer a factory over the concrete constructors.</summary>
public static partial class Signal
{
    /// <summary>Creates a signal that dispatches its notifications on <paramref name="scheduler"/>.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="scheduler">The scheduler to dispatch notifications on.</param>
    /// <returns>A scheduled signal.</returns>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Major Code Smell",
        "S4018:Generic methods should provide type parameters",
        Justification =
            "The type parameter defines the element type for this Rx-style factory and cannot be inferred from the arguments.")]
    public static ScheduledSignal<T> Scheduled<T>(ISequencer scheduler) =>
        new(scheduler);

    /// <summary>Creates a signal that dispatches its notifications on <paramref name="scheduler"/>, with a default observer active while no other subscribers are present.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="scheduler">The scheduler to dispatch notifications on.</param>
    /// <param name="defaultObserver">A default observer which receives values when no other subscribers are active.</param>
    /// <returns>A scheduled signal.</returns>
    public static ScheduledSignal<T> Scheduled<T>(ISequencer scheduler, IObserver<T>? defaultObserver) =>
        new(scheduler, defaultObserver);

    /// <summary>Creates a signal that buffers notifications while delayed and emits a de-duplicated batch when <see cref="DelayableNotificationSignal{T}.Flush"/> is called.</summary>
    /// <typeparam name="T">The notification type.</typeparam>
    /// <param name="isDelayed">Returns whether notifications are currently delayed.</param>
    /// <param name="flushDistinct">De-duplicates a buffered batch before it is emitted on flush.</param>
    /// <returns>A delayable notification signal.</returns>
    public static DelayableNotificationSignal<T> Delayable<T>(
        Func<bool> isDelayed,
        Func<IList<T>, IEnumerable<T>> flushDistinct) =>
        new(isDelayed, flushDistinct);
}
