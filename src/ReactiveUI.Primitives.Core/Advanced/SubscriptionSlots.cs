// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Advanced;

/// <summary>
/// Helpers for the interlocked single-assignment subscription slots shared by the catch-style sinks
/// (<c>RecoverSignal</c>, <c>ResumeSignal</c>): a slot holds at most one live
/// subscription and, once the sink is disposed, swaps to a sentinel so a late assignment is disposed instead of
/// stored.
/// </summary>
public static class SubscriptionSlots
{
    /// <summary>The sentinel stored in a slot once it has been released.</summary>
    public static readonly IDisposable Disposed = new DisposedMarker();

    /// <summary>Exchanges a slot for the disposed sentinel and disposes any live subscription it held.</summary>
    /// <param name="slot">The slot to release.</param>
    public static void Release(ref IDisposable? slot)
    {
        var current = Interlocked.Exchange(ref slot, Disposed);
        if (current is null || ReferenceEquals(current, Disposed))
        {
            return;
        }

        current.Dispose();
    }

    /// <summary>Stores a subscription into an empty slot, disposing it instead if the slot is already released.</summary>
    /// <param name="slot">The target slot.</param>
    /// <param name="subscription">The subscription to store.</param>
    public static void Assign(ref IDisposable? slot, IDisposable subscription)
    {
        if (Interlocked.CompareExchange(ref slot, subscription, null) is null)
        {
            return;
        }

        subscription.Dispose();
    }
}
