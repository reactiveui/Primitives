// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Extensions;

namespace ReactiveUI.Primitives.Advanced;

/// <summary>
/// Delivers the notifications of a sliced sequence to its outer observer and its windows in the order they were posted,
/// without holding the caller's lock while any observer runs.
/// </summary>
/// <typeparam name="TOuter">The type the outer observer receives for each window or group.</typeparam>
/// <typeparam name="T">The value type of each window.</typeparam>
/// <remarks>
/// An operator posts notifications while it holds its own lock, which fixes their order, and calls
/// <see cref="Flush"/> after releasing it. Notifications posted while another thread is delivering are delivered by that
/// thread in posting order, and a notification posted by the delivering thread itself is delivered after the observer
/// returns. Outer notifications stop once the owning <see cref="SharedSubscription"/> has been disposed, while window
/// notifications continue. <see cref="Finish"/> posts the terminal notification; nothing posted after it is delivered.
/// </remarks>
[System.Diagnostics.DebuggerDisplay("SliceRouter: {_delivery}")]
public sealed class SliceRouter<TOuter, T>
{
    /// <summary>Delivers a window or group to the outer observer.</summary>
    private static readonly Action<Applier, Notification> OpenedStep = static (applier, notification) => applier.DeliverOpened(notification.Outer);

    /// <summary>Publishes a value to a window.</summary>
    private static readonly Action<Applier, Notification> ValueStep = static (_, notification) => notification.Window!.Publish(notification.Value);

    /// <summary>Completes a window.</summary>
    private static readonly Action<Applier, Notification> CompletedStep = static (_, notification) => notification.Window!.Complete();

    /// <summary>Faults a window.</summary>
    private static readonly Action<Applier, Notification> FaultedStep = static (_, notification) => notification.Window!.Fault(notification.Error!);

    /// <summary>Completes the outer observer.</summary>
    private static readonly Action<Applier, Notification> OuterCompletedStep = static (applier, _) => applier.OnCompleted();

    /// <summary>The downstream observer of the outer sequence.</summary>
    private readonly IObserver<TOuter> _outer;

    /// <summary>The subscription whose primary handle detaches the outer observer.</summary>
    private readonly SharedSubscription _owner;

    /// <summary>Serializes delivery and holds the notifications that are waiting.</summary>
    private SerializedDelivery<Notification> _delivery = new();

    /// <summary>Applies each delivered notification to its target, created by the first delivery.</summary>
    private Applier? _applier;

    /// <summary>Initializes a new instance of the <see cref="SliceRouter{TOuter, T}"/> class.</summary>
    /// <param name="outer">The downstream observer of the outer sequence.</param>
    /// <param name="owner">The subscription whose primary handle detaches the outer observer when disposed.</param>
    /// <exception cref="ArgumentNullException"><paramref name="outer"/> or <paramref name="owner"/> is <see langword="null"/>.</exception>
    public SliceRouter(IObserver<TOuter> outer, SharedSubscription owner)
    {
        _outer = outer ?? throw new ArgumentNullException(nameof(outer));
        _owner = owner ?? throw new ArgumentNullException(nameof(owner));
    }

    /// <summary>Posts a window or group for delivery to the outer observer.</summary>
    /// <param name="opened">The window or group.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Open(TOuter opened) => Post(new(OpenedStep, null, default!, null, opened));

    /// <summary>Posts a value for delivery to a window.</summary>
    /// <param name="window">The target window.</param>
    /// <param name="value">The value.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Publish(SliceWindow<T> window, T value) => Post(new(ValueStep, window, value, null, default!));

    /// <summary>Posts the completion of a window.</summary>
    /// <param name="window">The window to complete.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Complete(SliceWindow<T> window) => Post(new(CompletedStep, window, default!, null, default!));

    /// <summary>Posts the failure of a window.</summary>
    /// <param name="window">The window to fault.</param>
    /// <param name="error">The error.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Fault(SliceWindow<T> window, Exception error) => Post(new(FaultedStep, window, default!, error, default!));

    /// <summary>Posts the completion of the outer sequence while later notifications for windows are still to come.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void CompleteOuter() => Post(new(OuterCompletedStep, null, default!, null, default!));

    /// <summary>Posts the terminal notification of each window in order, without ending the outer sequence.</summary>
    /// <param name="windows">The windows to terminate.</param>
    /// <param name="error">The error, or <see langword="null"/> to complete each window.</param>
    public void FinishWindows(IEnumerable<SliceWindow<T>> windows, Exception? error)
    {
        foreach (var window in windows)
        {
            if (error is null)
            {
                Complete(window);
                continue;
            }

            Fault(window, error);
        }
    }

    /// <summary>Posts the terminal notification of each window in order, then the terminal notification of the outer sequence.</summary>
    /// <param name="windows">The windows to terminate.</param>
    /// <param name="error">The error, or <see langword="null"/> to complete.</param>
    public void Finish(IEnumerable<SliceWindow<T>> windows, Exception? error)
    {
        FinishWindows(windows, error);
        _ = error is null ? _delivery.PostCompleted() : _delivery.PostError(error);
    }

    /// <summary>Delivers the posted notifications on the calling thread, or hands them to the thread already delivering.</summary>
    /// <remarks>Call it after releasing the lock that guarded the posts.</remarks>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Flush() => _delivery.Flush(new PendingDrain(this));

    /// <summary>Queues a notification for delivery.</summary>
    /// <param name="notification">The notification.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void Post(Notification notification) => _ = _delivery.Post(notification);

    /// <summary>One queued notification.</summary>
    /// <param name="Step">What the notification does when it is delivered.</param>
    /// <param name="Window">The target window, for window notifications.</param>
    /// <param name="Value">The value, for value notifications.</param>
    /// <param name="Error">The error, for fault notifications.</param>
    /// <param name="Outer">The window or group, for open notifications.</param>
    private readonly record struct Notification(Action<Applier, Notification> Step, SliceWindow<T>? Window, T Value, Exception? Error, TOuter Outer);

    /// <summary>Drains the queued notifications for the delivery gate.</summary>
    /// <param name="Owner">The router.</param>
    private readonly record struct PendingDrain(SliceRouter<TOuter, T> Owner) : IDrainTarget
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Drain() => _ = Owner._delivery.DrainTo(Owner._applier ??= new(Owner._outer, Owner._owner));
    }

    /// <summary>Applies delivered notifications to the outer observer and the windows.</summary>
    /// <param name="outer">The downstream observer of the outer sequence.</param>
    /// <param name="owner">The subscription whose primary handle detaches the outer observer.</param>
    private sealed class Applier(IObserver<TOuter> outer, SharedSubscription owner) : IObserver<Notification>
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(Notification value) => value.Step(this, value);

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
            if (owner.IsDisposed)
            {
                return;
            }

            outer.OnError(error);
        }

        /// <inheritdoc/>
        public void OnCompleted()
        {
            if (owner.IsDisposed)
            {
                return;
            }

            outer.OnCompleted();
        }

        /// <summary>Delivers a window or group to the outer observer unless the owner has been disposed.</summary>
        /// <param name="opened">The window or group.</param>
        internal void DeliverOpened(TOuter opened)
        {
            if (owner.IsDisposed)
            {
                return;
            }

            outer.OnNext(opened);
        }
    }
}
