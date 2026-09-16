// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Extensions;

/// <summary>Stores timer ownership, terminal state and serialized delivery for a timer-driven operator sink.</summary>
/// <typeparam name="T">The element type the downstream observer receives.</typeparam>
/// <param name="downstream">The downstream observer notifications are delivered to.</param>
/// <remarks>
/// The owning sink queues notifications under its gate with the <c>Locked</c> members and calls <see cref="Flush"/> after
/// releasing the gate, so the downstream observer never runs while the gate is held and notifications are delivered in the
/// order they were queued. A terminal notification queued before disposal is still delivered.
/// </remarks>
[System.Diagnostics.CodeAnalysis.SuppressMessage(
    "Design",
    "SST2315:A type that owns a disposable should be disposable",
    Justification =
        "The parent operator sink owns the timer's lifetime and releases it under its own gate, so an independent "
        + "disposal path on this state object would race that gate.")]
[System.Diagnostics.DebuggerDisplay("TimerSinkState: Done = {Done}, Timer = {Timer}")]
public sealed class TimerSinkState<T>(IObserver<T> downstream)
{
    /// <summary>The downstream observer notifications are delivered to.</summary>
    private readonly IObserver<T> _downstream = downstream;

    /// <summary>Serializes downstream deliveries.</summary>
    private SerializedDelivery<T> _delivery = new();

    /// <summary>Whether the sink has terminated through error, completion or disposal.</summary>
    private bool _done;

    /// <summary>Gets the timer slot used by the operator's OnNext logic to schedule deferred emissions.</summary>
    public SwapDisposable Timer { get; } = new();

    /// <summary>Gets a value indicating whether the sink has terminated through error, completion or disposal.</summary>
    public bool Done => Volatile.Read(ref _done);

    /// <summary>Queues a value for delivery while the caller holds its gate.</summary>
    /// <param name="value">The value to queue.</param>
    /// <returns><see langword="true"/> when the value was queued; <see langword="false"/> once the sink has terminated.</returns>
    public bool QueueLocked(T value) => !Done && _delivery.Post(value);

    /// <summary>Queues an error as the terminal notification and releases the timer while the caller holds its gate.</summary>
    /// <param name="error">The error to deliver.</param>
    /// <returns><see langword="true"/> when this is the first terminal notification.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="error"/> is <see langword="null"/>.</exception>
    public bool QueueErrorLocked(Exception error)
    {
        ArgumentExceptionHelper.ThrowIfNull(error);

        if (Done)
        {
            return false;
        }

        Volatile.Write(ref _done, true);
        Timer.Dispose();
        return _delivery.PostError(error);
    }

    /// <summary>Queues completion as the terminal notification and releases the timer while the caller holds its gate.</summary>
    /// <returns><see langword="true"/> when this is the first terminal notification.</returns>
    public bool QueueCompletedLocked()
    {
        if (Done)
        {
            return false;
        }

        Volatile.Write(ref _done, true);
        Timer.Dispose();
        return _delivery.PostCompleted();
    }

    /// <summary>Marks the sink terminal and disposes its timer without notification, while the caller holds its gate.</summary>
    public void HandleDisposeLocked()
    {
        Volatile.Write(ref _done, true);
        Timer.Dispose();
    }

    /// <summary>Delivers the queued notifications on the calling thread, or hands them to the thread already delivering.</summary>
    /// <remarks>Call it after releasing the gate the notifications were queued under.</remarks>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Flush() => _delivery.Flush(new PendingDrain(this));

    /// <summary>Drains this state's queued notifications for the delivery gate.</summary>
    /// <param name="Owner">The state.</param>
    private readonly record struct PendingDrain(TimerSinkState<T> Owner) : IDrainTarget
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Drain() => _ = Owner._delivery.DrainTo(Owner._downstream);
    }
}
